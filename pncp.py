import os
import re
import time
import math
import random
import threading
from datetime import datetime, timedelta
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================== orjson (fallback) ==============================
try:
    import orjson as _orjson
    def jloads(b: bytes | str):  # mais rápido
        if isinstance(b, str):
            b = b.encode("utf-8", "ignore")
        return _orjson.loads(b)
    def jdumps(obj) -> bytes:
        return _orjson.dumps(obj, option=_orjson.OPT_INDENT_2 | _orjson.OPT_SORT_KEYS)
except Exception:
    import json as _json
    def jloads(b: bytes | str):
        if isinstance(b, bytes):
            b = b.decode("utf-8", "ignore")
        return _json.loads(b)
    def jdumps(obj) -> bytes:
        return _json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")

# ============================== Configs ==============================
BASE_CONSULTA = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
BASE_ITENS = "https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"

HEADERS = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "User-Agent": "pncp-teste/mt/2.6"
}

# Concorrência (ajustáveis por env)
MAX_WORKERS_PAGES = int(os.getenv("PNCP_MAX_WORKERS_PAGES", "20"))
MAX_WORKERS_ITENS = int(os.getenv("PNCP_MAX_WORKERS_ITENS", "32"))

# Rate limit (token bucket) + jitter
RPS = float(os.getenv("PNCP_RPS", "15"))           # tokens/s
RPS_BURST = float(os.getenv("PNCP_RPS_BURST", "30"))  # capacidade do bucket
JITTER_MAX = 0.2  # segundos

# Timeouts específicos
CONNECT_TIMEOUT = float(os.getenv("PNCP_CONNECT_TIMEOUT", "5"))
READ_TIMEOUT = float(os.getenv("PNCP_READ_TIMEOUT", "25"))
DEFAULT_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

# Retries HTTP pelo adapter (para 50x transitórios)
ADAPTER_RETRY = Retry(
    total=2,
    backoff_factor=0.3,
    status_forcelist=[502, 503, 504],
    allowed_methods=["GET"],
    raise_on_status=False,
)

# Página: tentativa de tamanhos aceitos pelo servidor
PAGE_SIZE_CANDIDATES = [500, 200, 100, 50, None]
PAGE_SIZE_CACHE: dict[tuple[int, int | None], int | None] = {}

# Modalidades (apenas referência)
MODALIDADES = {
    1: "Leilão – Eletrônico",
    2: "Diálogo Competitivo",
    3: "Concurso",
    4: "Concorrência – Eletrônica",
    5: "Concorrência – Presencial",
    6: "Pregão – Eletrônico",
    7: "Pregão – Presencial",
    8: "Dispensa de Licitação",
    9: "Inexigibilidade",
    10: "Manifestação de Interesse",
    11: "Pré-qualificação",
    12: "Credenciamento",
    13: "Leilão – Presencial",
}

# Segmentação de datas
SPLIT_MODE = os.getenv("PNCP_SPLIT_MODE", "monthly").lower()  # monthly | semi | weekly

# Cache
CACHE_DIR = os.getenv("PNCP_CACHE_DIR", "./cache")
CACHE_ONLY = os.getenv("PNCP_CACHE_ONLY", "0") == "1"
os.makedirs(CACHE_DIR, exist_ok=True)

# Auto-tuning (limiares)
P95_LIMIT = float(os.getenv("PNCP_P95_LIMIT", "8.0"))   # seg
ERR_LIMIT = float(os.getenv("PNCP_ERR_LIMIT", "0.08"))  # 8%
METRICS_WINDOW = 200   # últimas N requisições
ADJUST_EVERY = 5.0     # s entre avaliações

# ============================== Utilidades ==============================
def ask_date(prompt: str) -> str:
    s = input(prompt).strip()
    datetime.strptime(s, "%Y%m%d")  # valida AAAAMMDD
    return s

def parse_modalidades(raw: str) -> list[int]:
    mods, seen = [], set()
    for part in raw.split(";"):
        p = part.strip()
        if p:
            v = int(p)
            if v not in seen:
                seen.add(v)
                mods.append(v)
    return mods or [6]

def compile_or_regex(terms_raw: str):
    parts = [t.strip() for t in terms_raw.split(";") if t.strip()]
    if not parts:
        return None
    pat = "|".join(re.escape(t) for t in parts)
    return re.compile(pat, flags=re.IGNORECASE)

def parse_id_pncp(id_pncp: str) -> tuple[str, int, int]:
    partes = [p.strip() for p in id_pncp.split("/") if p.strip()]
    if len(partes) != 3:
        raise ValueError("id_pncp inválido (use CNPJ/ANO/SEQ)")
    return partes[0], int(partes[1]), int(partes[2])

def format_id_pncp_from_numero_controle(raw: str) -> tuple[str | None, str | None]:
    try:
        left, ano = raw.split("/")
        cnpj = left.split("-")[0]
        seq_raw = left.split("-")[-1]
        seq = seq_raw.lstrip("0") or "0"
        id_pncp = f"{cnpj}/{ano}/{seq}"
        link = f"https://pncp.gov.br/app/editais/{id_pncp}"
        return id_pncp, link
    except Exception:
        return None, None

# ============================== Data windows ==============================
def split_range(data_ini: str, data_fim: str) -> list[tuple[str, str]]:
    """Divide [data_ini, data_fim] em janelas conforme SPLIT_MODE."""
    dt_i = datetime.strptime(data_ini, "%Y%m%d")
    dt_f = datetime.strptime(data_fim, "%Y%m%d")
    if dt_f < dt_i:
        raise ValueError("Data Inicial deve ser anterior ou igual à Data Final")

    out = []
    cur = dt_i
    while cur <= dt_f:
        if SPLIT_MODE == "weekly":
            nxt = cur + timedelta(days=6)
        elif SPLIT_MODE in ("semi", "semimonthly", "quinzenal"):
            # 1–15, 16–fim
            if cur.day <= 15:
                nxt = cur.replace(day=15)
            else:
                # fim do mês
                nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        else:  # monthly (default)
            # fim do mês
            nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

        if nxt > dt_f:
            nxt = dt_f
        out.append((cur.strftime("%Y%m%d"), nxt.strftime("%Y%m%d")))
        cur = nxt + timedelta(days=1)
    return out

# ============================== Rate limit ==============================
class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: float):
        self.rate = rate_per_sec
        self.capacity = capacity
        self.tokens = capacity
        self.ts = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.ts
                if elapsed > 0:
                    self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                    self.ts = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    break
            time.sleep(0.005)

RATE_LIMITER = TokenBucket(RPS, RPS_BURST)

# ============================== Métricas + Concorrência adaptativa ==============================
class Metrics:
    def __init__(self, maxlen=METRICS_WINDOW):
        self.lock = threading.Lock()
        self.samples = deque(maxlen=maxlen)  # (latency, ok_bool, status)
        self.req_count = 0

    def record(self, latency: float, status: int):
        ok = 200 <= status < 300
        with self.lock:
            self.samples.append((latency, ok, status))
            self.req_count += 1

    def snapshot(self):
        with self.lock:
            data = list(self.samples)
        if not data:
            return {"p95": 0.0, "err_rate": 0.0, "n": 0, "recent_429_5xx": 0}
        lat = sorted(x[0] for x in data)
        idx = int(0.95 * (len(lat) - 1)) if lat else 0
        p95 = lat[idx] if lat else 0.0
        err_rate = 1.0 - (sum(1 for _, ok, _ in data if ok) / len(data))
        recent_429_5xx = sum(1 for _, ok, s in data if not ok and (s == 429 or 500 <= s < 600))
        return {"p95": p95, "err_rate": err_rate, "n": len(data), "recent_429_5xx": recent_429_5xx}

METRICS = Metrics()

class AdaptiveConcurrency:
    def __init__(self, initial: int, min_limit: int, max_limit: int, name: str):
        self.name = name
        self.current_limit = max(min_limit, min(initial, max_limit))
        self.min_limit = min_limit
        self.max_limit = max_limit
        self.used = 0
        self.cv = threading.Condition()
        self._stop = False
        self._thread = threading.Thread(target=self._adjust_loop, daemon=True)
        self._thread.start()

    def acquire(self):
        with self.cv:
            while self.used >= self.current_limit:
                self.cv.wait()
            self.used += 1

    def release(self):
        with self.cv:
            self.used -= 1
            if self.used < 0:
                self.used = 0
            self.cv.notify_all()

    def _adjust_loop(self):
        """Ajuste gradual com base em p95 e taxa de erro recentes."""
        while not self._stop:
            time.sleep(ADJUST_EVERY)
            snap = METRICS.snapshot()
            # Regras simples: reduzir agressivo ao degradar; aumentar devagar quando estável
            new_limit = self.current_limit
            if snap["n"] >= max(20, int(METRICS_WINDOW * 0.4)):
                if snap["p95"] > P95_LIMIT or snap["err_rate"] > ERR_LIMIT or snap["recent_429_5xx"] >= 5:
                    new_limit = max(self.min_limit, max(int(self.current_limit * 0.6), self.current_limit - 2))
                else:
                    new_limit = min(self.max_limit, self.current_limit + 1)
            if new_limit != self.current_limit:
                self.current_limit = new_limit
                print(f"[ADAPT] {self.name}: limit -> {self.current_limit} "
                      f"(p95={snap['p95']:.2f}s, err={snap['err_rate']:.1%})")

    def stop(self):
        self._stop = True

CONC_PAGES = AdaptiveConcurrency(initial=min(8, MAX_WORKERS_PAGES),
                                 min_limit=2, max_limit=MAX_WORKERS_PAGES, name="pages")
CONC_ITENS = AdaptiveConcurrency(initial=min(8, MAX_WORKERS_ITENS),
                                 min_limit=2, max_limit=MAX_WORKERS_ITENS, name="itens")

# ============================== Sessão HTTP ==============================
def build_session() -> requests.Session:
    sess = requests.Session()
    # pool dimensionado com folga >= soma dos workers
    pool_size = MAX_WORKERS_PAGES + MAX_WORKERS_ITENS + 12
    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=ADAPTER_RETRY,
    )
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS)
    return sess

# ============================== HTTP com backoff, rate e métricas ==============================
def get_with_backoff(session: requests.Session, url: str, *, params=None,
                     timeout=DEFAULT_TIMEOUT, max_retries=4) -> requests.Response:
    attempt = 0
    last_err_txt = ""
    while True:
        RATE_LIMITER.acquire()
        if JITTER_MAX > 0:
            time.sleep(random.random() * JITTER_MAX)  # jitter
        t0 = time.monotonic()
        try:
            r = session.get(url, params=params, timeout=timeout)
            latency = time.monotonic() - t0
            METRICS.record(latency, r.status_code)
        except requests.RequestException as ex:
            latency = time.monotonic() - t0
            METRICS.record(latency, 599)
            last_err_txt = str(ex)
            # tratar como 5xx para retry
            r = None

        if r is not None and 200 <= r.status_code < 300:
            return r

        status = r.status_code if r is not None else 599
        body = ""
        try:
            body = (r.text if r is not None else last_err_txt)[:400]
        except Exception:
            body = "<sem corpo>"

        if status == 429 or 500 <= status < 600 or status == 599:
            attempt += 1
            if attempt > max_retries:
                raise requests.HTTPError(f"HTTP {status} em {url}\n{body}")
            wait = (1.4 ** attempt) + attempt * 0.25
            print(f"[WARN] {status} em {url} — retry {attempt}/{max_retries} em {wait:.1f}s")
            time.sleep(wait)
            continue

        # 4xx != 429: falha imediata
        raise requests.HTTPError(f"HTTP {status} em {url}\n{body}")

# ============================== Cache helpers ==============================
def _cache_path_contratacoes(modalidade: int, modo: int | None, di: str, df: str) -> str:
    tag_modo = "x" if modo is None else str(modo)
    return os.path.join(CACHE_DIR, f"contratacoes_m{modalidade}_md{tag_modo}_{di}_{df}.json")

def _cache_path_itens(cnpj: str, ano: int, seq: int) -> str:
    return os.path.join(CACHE_DIR, f"itens_{cnpj}_{ano}_{seq}.json")

def _atomic_write(path: str, data: bytes):
    tmp = f"{path}.tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    # Windows friendly swap
    if os.path.exists(path):
        os.remove(path)
    os.replace(tmp, path)

# ============================== Consultas (com fallback de página) ==============================
def fetch_page_with_pagesize(session: requests.Session, pagina: int, data_ini: str, data_fim: str,
                             modalidade: int, modo: int | None):
    key = (modalidade, modo)
    base_params = {
        "dataInicial": data_ini,
        "dataFinal":   data_fim,
        "codigoModalidadeContratacao": int(modalidade),
        "pagina": pagina,
    }
    if modo is not None:
        base_params["codigoModoDisputa"] = int(modo)

    # Usa tamanho já validado se houver
    if key in PAGE_SIZE_CACHE:
        ps = PAGE_SIZE_CACHE[key]
        params = dict(base_params)
        if ps is not None:
            params["tamanhoPagina"] = ps
        r = get_with_backoff(session, BASE_CONSULTA, params=params)
        return jloads(r.content)

    last_err = None
    for ps in PAGE_SIZE_CANDIDATES:
        params = dict(base_params)
        if ps is not None:
            params["tamanhoPagina"] = ps
        try:
            r = get_with_backoff(session, BASE_CONSULTA, params=params)
            payload = jloads(r.content)
            PAGE_SIZE_CACHE[key] = ps
            info_ps = "padrão" if ps is None else str(ps)
            print(f"[INFO] tamanhoPagina={info_ps} aceito para modalidade={modalidade}")
            return payload
        except requests.HTTPError as e:
            msg = str(e)
            # se for 400 de tamanho inválido, tenta próximo
            if " 400 " in f" {msg} " or "tamanho" in msg.lower():
                last_err = e
                continue
            raise
    raise last_err or RuntimeError("Nenhum tamanhoPagina aceito.")

def discover_total_pages_for_modalidade(session: requests.Session, data_ini: str, data_fim: str,
                                        modalidade: int, modo: int | None):
    payload = fetch_page_with_pagesize(session, 1, data_ini, data_fim, modalidade, modo)
    total = payload.get("totalPaginas") or payload.get("totalPaginasConsulta") or 1
    dados = payload.get("data", [])
    print(f"[DESC] mod={modalidade:>2} ({MODALIDADES.get(modalidade,'?')}) "
          f"j={data_ini[:6]} p1={len(dados)} total={int(total)}")
    return int(total), dados

def fetch_all_pages_for_modalidade(session: requests.Session, total_pages: int, data_ini: str, data_fim: str,
                                   modalidade: int, modo: int | None) -> list[dict]:
    results: list[dict] = []

    def _one(p: int):
        CONC_PAGES.acquire()
        try:
            payload = fetch_page_with_pagesize(session, p, data_ini, data_fim, modalidade, modo)
            lote = payload.get("data", [])
            print(f"[PÁG] mod={modalidade:>2} j={data_ini[:6]} p={p}/{total_pages} reg={len(lote)}")
            return lote
        finally:
            CONC_PAGES.release()

    if total_pages <= 1:
        return results

    # Executor apenas como "pool", limite real é o CONC_PAGES (adaptativo)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PAGES) as ex:
        futures = {ex.submit(_one, p): p for p in range(2, total_pages + 1)}
        for fut in as_completed(futures):
            try:
                results.extend(fut.result())
            except Exception as e:
                print(f"[ERRO] Página {futures[fut]} (mod {modalidade}) falhou: {e}")
    return results

def fetch_contratacoes_for_window(session: requests.Session, data_ini: str, data_fim: str,
                                  modalidades: list[int], modo: int | None, use_cache=True) -> list[dict]:
    all_contratacoes: list[dict] = []
    for m in modalidades:
        cache_path = _cache_path_contratacoes(m, modo, data_ini, data_fim)
        if use_cache and os.path.exists(cache_path):
            try:
                with open(cache_path, "rb") as f:
                    dados = jloads(f.read())
                print(f"[CACHE] mod={m:>2} j={data_ini[:6]} reg={len(dados)} (hit)")
                all_contratacoes.extend(dados)
                continue
            except Exception as e:
                print(f"[CACHE] erro ao ler {cache_path}: {e} — vai buscar na API")

        try:
            total, page1 = discover_total_pages_for_modalidade(session, data_ini, data_fim, m, modo)
        except Exception as e:
            print(f"[ERRO] Descoberta de páginas falhou (mod={m}): {e}")
            continue

        outras = fetch_all_pages_for_modalidade(session, total, data_ini, data_fim, m, modo)
        dados_mod = page1 + outras
        all_contratacoes.extend(dados_mod)

        # salva cache desta janela+modalidade
        try:
            _atomic_write(cache_path, jdumps(dados_mod))
            print(f"[CACHE] salvo {cache_path} reg={len(dados_mod)}")
        except Exception as e:
            print(f"[CACHE] falha ao salvar {cache_path}: {e}")

    return all_contratacoes

# ============================== Itens ==============================
def itens_pncp_por_id(session: requests.Session, id_pncp: str) -> list[dict]:
    cnpj, ano, seq = parse_id_pncp(id_pncp)
    cache_path = _cache_path_itens(cnpj, ano, seq)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "rb") as f:
                data = jloads(f.read())
            return data
        except Exception:
            pass

    url = BASE_ITENS.format(cnpj=cnpj, ano=ano, seq=seq)
    r = get_with_backoff(session, url)
    data = jloads(r.content)
    itens = data.get("data", data) if isinstance(data, dict) else data

    edital_link = f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
    out = []
    for it in itens:
        numero = it.get("numeroItem")
        desc = it.get("descricao") or it.get("descricaoItem") or ""
        qtd = it.get("quantidade")
        vu = it.get("valorUnitarioEstimado", it.get("valorEstimado"))
        vt = it.get("valorTotalEstimado")
        if vt is None and isinstance(qtd, (int, float)) and isinstance(vu, (int, float)):
            vt = qtd * vu
        detalhar = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens/{numero}/resultados"
        out.append({
            "id_pncp": f"{cnpj}/{ano}/{seq}",
            "Numero": numero,
            "Descricao": desc,
            "Quantidade": qtd,
            "Valor unitario estimado": vu,
            "Valor total estimado": vt,
            "Detalhar": detalhar,
            "Edital": edital_link
        })

    # cacheia
    try:
        _atomic_write(cache_path, jdumps(out))
    except Exception:
        pass
    return out

def fetch_itens_para_ids(session: requests.Session, ids_uniq: list[tuple[str, str]]) -> list[dict]:
    encontrados = []
    lock = threading.Lock()

    def _worker(id_pncp: str):
        CONC_ITENS.acquire()
        try:
            return itens_pncp_por_id(session, id_pncp)
        finally:
            CONC_ITENS.release()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_ITENS) as ex:
        futs = {ex.submit(_worker, i[0]): i[0] for i in ids_uniq}
        for fut in as_completed(futs):
            idp = futs[fut]
            try:
                itens = fut.result()
                with lock:
                    encontrados.extend(itens)
                print(f"[ITENS] {idp} -> {len(itens)}")
            except Exception as e:
                print(f"[WARN] itens falharam {idp}: {e}")

    return encontrados

# ============================== MAIN ==============================
if __name__ == "__main__":
    try:
        print("Modalidades disponíveis (código: nome):")
        print(", ".join(f"{k}: {v}" for k, v in MODALIDADES.items()))

        palavra_contratacao = input("Palavra no OBJETO da contratação: ").strip().lower()
        modalidades_raw = input("Códigos de modalidade (ex.: 6;8;9): ").strip() or "6"
        modalidades = parse_modalidades(modalidades_raw)
        modo_raw = input("Código do modo de disputa (opcional, Enter p/ pular): ").strip()
        modo = int(modo_raw) if modo_raw else None
        data_inicial = ask_date("Data inicial (AAAAMMDD): ")
        data_final   = ask_date("Data final   (AAAAMMDD): ")

        s_itens_raw = input("Termos na DESCRIÇÃO do item (separe por ';'): ").strip()
        termos_re = compile_or_regex(s_itens_raw)

        session = build_session()

        # Segmenta por janelas
        janelas = split_range(data_inicial, data_final)
        print(f"[INFO] {len(janelas)} janela(s) ({SPLIT_MODE})")

        contratacoes = []
        for di, df in janelas:
            if CACHE_ONLY:
                dados = fetch_contratacoes_for_window(session, di, df, modalidades, modo, use_cache=True)
            else:
                dados = fetch_contratacoes_for_window(session, di, df, modalidades, modo, use_cache=True)
                # use_cache=True já tenta cache antes da API — se CACHE_ONLY=1, nunca bate na API
            contratacoes.extend(dados)
            print(f"[WIN ] {di}..{df} total_acum={len(contratacoes)}")

        print(f"\n[INFO] Contratações coletadas (todas janelas/modalidades): {len(contratacoes)}")

        # Filtro no OBJETO
        filtradas = [
            c for c in contratacoes
            if palavra_contratacao in (c.get("objetoCompra") or "").lower()
        ]
        print(f"[INFO] Após filtro no objeto: {len(filtradas)}")

        # IDs PNCP únicos
        ids = []
        for c in filtradas:
            raw = c.get("numeroControlePNCP")
            if not raw:
                continue
            id_pncp, link = format_id_pncp_from_numero_controle(raw)
            if id_pncp:
                ids.append((id_pncp, link))

        seen = set()
        ids_uniq = []
        for i in ids:
            if i[0] not in seen:
                seen.add(i[0])
                ids_uniq.append(i)

        print(f"[INFO] IDs PNCP (únicos): {len(ids_uniq)}")
        for id_pncp, link in ids_uniq[:20]:
            # limita o spam no console
            print(f"  - {id_pncp} | {link}")
        if len(ids_uniq) > 20:
            print(f"  ... (+{len(ids_uniq)-20} ids)")

        # Itens (paralelo com rate limit)
        itens_all = fetch_itens_para_ids(session, ids_uniq)

        # Filtra itens pelos termos (se houver)
        encontrados = []
        for row in itens_all:
            desc = row.get("Descricao") or ""
            if not termos_re or termos_re.search(desc):
                encontrados.append(row)

        # Saída final resumida
        print("\n===== ITENS ENCONTRADOS =====")
        for r in encontrados:
            print(f"[{r['id_pncp']}] Item {r['Numero']}: {r['Descricao']}")
            print(f"  Quantidade: {r['Quantidade']}")
            print(f"  VU estimado: {r['Valor unitario estimado']}")
            print(f"  VT estimado: {r['Valor total estimado']}")
            print(f"  Detalhar: {r['Detalhar']}")
            print(f"  Edital: {r['Edital']}\n")

        print(f"[OK] Total de itens encontrados: {len(encontrados)}")

    except Exception as e:
        print(f"[FATAL] {e}")
    finally:
        # encerra threads do auto-tuning
        try:
            CONC_PAGES.stop()
            CONC_ITENS.stop()
        except Exception:
            pass
