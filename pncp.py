import os
import re
import time
import random
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================== orjson (fallback) ==============================
try:
    import orjson as _orjson
    def jloads(b):
        if isinstance(b, str):
            b = b.encode("utf-8", "ignore")
        return _orjson.loads(b)
    def jdumps(obj) -> bytes:
        return _orjson.dumps(obj, option=_orjson.OPT_INDENT_2 | _orjson.OPT_SORT_KEYS)
except Exception:
    import json as _json
    def jloads(b):
        if isinstance(b, bytes):
            b = b.decode("utf-8", "ignore")
        return _json.loads(b)
    def jdumps(obj) -> bytes:
        return _json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")

# ============================== Configs ==============================
BASE_CONSULTA = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
BASE_ITENS    = "https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
HEADERS = {
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "User-Agent": "pncp-teste/mt/3.1-nopacer"
}

# Concorrência (estática)
MAX_WORKERS_PAGES = int(os.getenv("PNCP_MAX_WORKERS_PAGES", "32"))
MAX_WORKERS_ITENS = int(os.getenv("PNCP_MAX_WORKERS_ITENS", "48"))

# Rate limit FIXO (opcional, desligado por padrão)
USE_RATELIMIT = os.getenv("PNCP_USE_RATELIMIT", "0") == "1"
RPS           = float(os.getenv("PNCP_RPS", "25"))          # tokens/s (se habilitado)
RPS_BURST     = float(os.getenv("PNCP_RPS_BURST", "50"))    # capacidade do bucket
JITTER_MAX    = float(os.getenv("PNCP_JITTER_MAX", "0.10")) # 100 ms

# Timeouts
CONNECT_TIMEOUT = float(os.getenv("PNCP_CONNECT_TIMEOUT", "5"))
READ_TIMEOUT    = float(os.getenv("PNCP_READ_TIMEOUT", "25"))
DEFAULT_TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

# Retries do adapter (50x transitórios)
ADAPTER_RETRY = Retry(
    total=2, backoff_factor=0.3, status_forcelist=[502, 503, 504], allowed_methods=["GET"], raise_on_status=False
)

# Página: tentativa de tamanhos aceitos
PAGE_SIZE_CANDIDATES = [500, 200, 100, 50, None]
PAGE_SIZE_CACHE: dict[tuple[int, int | None], int | None] = {}

# Modalidades (referência)
MODALIDADES = {
    1:"Leilão – Eletrônico", 2:"Diálogo Competitivo", 3:"Concurso",
    4:"Concorrência – Eletrônica", 5:"Concorrência – Presencial",
    6:"Pregão – Eletrônico", 7:"Pregão – Presencial", 8:"Dispensa de Licitação",
    9:"Inexigibilidade", 10:"Manifestação de Interesse", 11:"Pré-qualificação",
    12:"Credenciamento", 13:"Leilão – Presencial",
}

# Segmentação de datas
SPLIT_MODE = os.getenv("PNCP_SPLIT_MODE", "monthly").lower()  # monthly|semi|weekly

# Cache
CACHE_DIR = os.getenv("PNCP_CACHE_DIR", "./cache")
CACHE_ONLY = os.getenv("PNCP_CACHE_ONLY", "0") == "1"
os.makedirs(CACHE_DIR, exist_ok=True)

# ============================== Utilidades ==============================
def ask_date(prompt: str) -> str:
    s = input(prompt).strip()
    datetime.strptime(s, "%Y%m%d")
    return s

def parse_modalidades(raw: str) -> list[int]:
    mods, seen = [], set()
    for part in raw.split(";"):
        p = part.strip()
        if p:
            v = int(p)
            if v not in seen:
                seen.add(v); mods.append(v)
    return mods or [6]

def compile_or_regex(terms_raw: str):
    parts = [t.strip() for t in terms_raw.split(";") if t.strip()]
    if not parts: return None
    return re.compile("|".join(re.escape(t) for t in parts), flags=re.IGNORECASE)

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
        return f"{cnpj}/{ano}/{seq}", f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
    except Exception:
        return None, None

# ============================== Janela de datas ==============================
def split_range(data_ini: str, data_fim: str) -> list[tuple[str, str]]:
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
            if cur.day <= 15:
                nxt = cur.replace(day=15)
            else:
                nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
        else:  # monthly
            nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

        if nxt > dt_f: nxt = dt_f
        out.append((cur.strftime("%Y%m%d"), nxt.strftime("%Y%m%d")))
        cur = nxt + timedelta(days=1)
    return out

# ============================== Métricas (somente para logs) ==============================
class Metrics:
    def __init__(self, maxlen=300):
        self.lock = threading.Lock()
        self.samples = deque(maxlen=maxlen)  # (latency, ok, status)
    def record(self, latency: float, status: int):
        ok = 200 <= status < 300
        with self.lock:
            self.samples.append((latency, ok, status))
    def snapshot(self):
        with self.lock:
            data = list(self.samples)
        if not data:
            return {"p95":0.0, "err_rate":0.0, "n":0}
        lat = sorted(x[0] for x in data)
        idx = max(0, int(0.95*(len(lat)-1)))
        p95 = lat[idx]
        err_rate = 1.0 - (sum(1 for _, ok, _ in data if ok) / len(data))
        return {"p95":p95, "err_rate":err_rate, "n":len(data)}

METRICS = Metrics()

# ============================== Rate limit FIXO (opcional) ==============================
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
            time.sleep(0.003)

RATE_LIMITER = TokenBucket(RPS, RPS_BURST) if USE_RATELIMIT else None

# ============================== Sessão HTTP ==============================
def build_session() -> requests.Session:
    sess = requests.Session()
    pool_size = MAX_WORKERS_PAGES + MAX_WORKERS_ITENS + 16  # folga
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size, max_retries=ADAPTER_RETRY)
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS)
    return sess

# ============================== HTTP (backoff; 429 respeita Retry-After) ==============================
def _parse_retry_after(resp: requests.Response) -> float | None:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)
    except Exception:
        return None  # podia ser http-date

def get_with_backoff(session: requests.Session, url: str, *, params=None,
                     timeout=DEFAULT_TIMEOUT, max_retries=4) -> requests.Response:
    attempt = 0
    last_err = ""
    while True:
        if RATE_LIMITER: RATE_LIMITER.acquire()
        if RATE_LIMITER and JITTER_MAX > 0:
            time.sleep(random.random() * JITTER_MAX)

        t0 = time.monotonic()
        try:
            r = session.get(url, params=params, timeout=timeout)
            latency = time.monotonic() - t0
            METRICS.record(latency, r.status_code)
        except requests.RequestException as ex:
            latency = time.monotonic() - t0
            METRICS.record(latency, 599)
            last_err = str(ex)
            r = None

        if r is not None and 200 <= r.status_code < 300:
            return r

        status = r.status_code if r is not None else 599
        body = ""
        try:
            body = (r.text if r is not None else last_err)[:300]
        except Exception:
            body = "<sem corpo>"

        # 429 e 5xx => backoff simples + Retry-After
        if status == 429:
            ra = _parse_retry_after(r) if r is not None else None
            if ra and ra > 0:
                print(f"[429] Retry-After: {ra:.1f}s")
                time.sleep(min(ra, 15.0))
            attempt += 1
            if attempt > max_retries:
                raise requests.HTTPError(f"HTTP 429 em {url}\n{body}")
            wait = (1.4 ** attempt) + random.random() * 0.3
            time.sleep(wait)
            continue
        if status == 599 or 500 <= status < 600:
            attempt += 1
            if attempt > max_retries:
                raise requests.HTTPError(f"HTTP {status} em {url}\n{body}")
            wait = (1.4 ** attempt) + attempt * 0.2
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
    if os.path.exists(path):
        os.remove(path)
    os.replace(tmp, path)

# ============================== Consulta com fallback de página ==============================
def fetch_page_with_pagesize(session: requests.Session, pagina: int, di: str, df: str,
                             modalidade: int, modo: int | None):
    key = (modalidade, modo)
    base_params = {
        "dataInicial": di,
        "dataFinal":   df,
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
            if " 400 " in f" {msg} " or "tamanho" in msg.lower():
                last_err = e
                continue
            raise
    raise last_err or RuntimeError("Nenhum tamanhoPagina aceito.")

def discover_total_pages_for_modalidade(session: requests.Session, di: str, df: str, modalidade: int, modo: int | None):
    payload = fetch_page_with_pagesize(session, 1, di, df, modalidade, modo)
    total = payload.get("totalPaginas") or payload.get("totalPaginasConsulta") or 1
    dados = payload.get("data", [])
    print(f"[DESC] mod={modalidade:>2} ({MODALIDADES.get(modalidade,'?')}) j={di[:6]} p1={len(dados)} total={int(total)}")
    return int(total), dados

def fetch_all_pages_for_modalidade(session: requests.Session, total_pages: int, di: str, df: str,
                                   modalidade: int, modo: int | None) -> list[dict]:
    results: list[dict] = []

    def _one(p: int):
        payload = fetch_page_with_pagesize(session, p, di, df, modalidade, modo)
        lote = payload.get("data", [])
        print(f"[PÁG] mod={modalidade:>2} j={di[:6]} p={p}/{total_pages} reg={len(lote)}")
        return lote

    if total_pages <= 1:
        return results

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PAGES) as ex:
        futures = {ex.submit(_one, p): p for p in range(2, total_pages + 1)}
        for fut in as_completed(futures):
            try:
                results.extend(fut.result())
            except Exception as e:
                print(f"[ERRO] Página {futures[fut]} (mod {modalidade}) falhou: {e}")
    return results

def fetch_contratacoes_for_window(session: requests.Session, di: str, df: str,
                                  modalidades: list[int], modo: int | None, use_cache=True) -> list[dict]:
    all_contratacoes: list[dict] = []
    for m in modalidades:
        cache_path = _cache_path_contratacoes(m, modo, di, df)
        if use_cache and os.path.exists(cache_path):
            try:
                with open(cache_path, "rb") as f:
                    dados = jloads(f.read())
                print(f"[CACHE] mod={m:>2} j={di[:6]} reg={len(dados)} (hit)")
                all_contratacoes.extend(dados); continue
            except Exception as e:
                print(f"[CACHE] erro ao ler {cache_path}: {e} — vai buscar na API")

        try:
            total, page1 = discover_total_pages_for_modalidade(session, di, df, m, modo)
        except Exception as e:
            print(f"[ERRO] Descoberta de páginas falhou (mod={m}): {e}")
            continue

        outras = fetch_all_pages_for_modalidade(session, total, di, df, m, modo)
        dados_mod = page1 + outras
        all_contratacoes.extend(dados_mod)

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
                return jloads(f.read())
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
        vu  = it.get("valorUnitarioEstimado", it.get("valorEstimado"))
        vt  = it.get("valorTotalEstimado")
        if vt is None and isinstance(qtd, (int, float)) and isinstance(vu, (int, float)):
            vt = qtd * vu
        detalhar = f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/{ano}/{seq}/itens/{numero}/resultados"
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

    try:
        _atomic_write(cache_path, jdumps(out))
    except Exception:
        pass
    return out

def fetch_itens_para_ids(session: requests.Session, ids_uniq: list[tuple[str, str]]) -> list[dict]:
    encontrados = []
    lock = threading.Lock()
    def _worker(id_pncp: str):
        return itens_pncp_por_id(session, id_pncp)
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

# ============================== Relatório TXT ==============================
def generate_report_txt(encontrados: list[dict], meta: dict) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(os.getcwd(), f"{os.getenv('PNCP_REPORT_BASENAME','pncp_itens')}_{ts}.txt")
    linhas = [
        "RELATÓRIO PNCP — Itens encontrados",
        f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        "",
        "Parâmetros da pesquisa:",
        f"  • Objeto contém: {meta.get('palavra_contratacao') or '(vazio)'}",
        f"  • Termos em descrição do item: {meta.get('termos_itens') or '(nenhum)'}",
        f"  • Período: {meta.get('data_inicial')} .. {meta.get('data_final')} (janela: {meta.get('split_mode')})",
        f"  • Modalidades: {meta.get('modalidades')}",
        f"  • Modo de disputa: {meta.get('modo_disputa')}",
        "",
        f"TOTAL de itens encontrados: {len(encontrados)}",
        "="*70, ""
    ]
    for r in encontrados:
        linhas += [
            f"ID PNCP: {r.get('id_pncp','')}",
            f"Item: {r.get('Numero','')}",
            f"Descrição: {r.get('Descricao','')}",
            f"Quantidade: {r.get('Quantidade','')}",
            f"Valor unitário estimado: {r.get('Valor unitario estimado','')}",
            f"Valor total estimado: {r.get('Valor total estimado','')}",
            f"Detalhar (resultados): {r.get('Detalhar','')}",
            f"Edital: {r.get('Edital','')}",
            "-"*70, ""
        ]
    _atomic_write(path, "\n".join(linhas).encode("utf-8"))
    return path

# ============================== MAIN ==============================
def main():
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

    janelas = split_range(data_inicial, data_final)
    print(f"[INFO] {len(janelas)} janela(s) ({SPLIT_MODE})")

    contratacoes = []
    for di, df in janelas:
        dados = fetch_contratacoes_for_window(session, di, df, modalidades, modo, use_cache=True)
        contratacoes.extend(dados)
        print(f"[WIN ] {di}..{df} total_acum={len(contratacoes)}")

    print(f"\n[INFO] Contratações coletadas: {len(contratacoes)}")

    filtradas = [c for c in contratacoes if palavra_contratacao in (c.get("objetoCompra") or "").lower()]
    print(f"[INFO] Após filtro no objeto: {len(filtradas)}")

    ids = []
    for c in filtradas:
        raw = c.get("numeroControlePNCP")
        if not raw: continue
        id_pncp, link = format_id_pncp_from_numero_controle(raw)
        if id_pncp:
            ids.append((id_pncp, link))

    seen = set(); ids_uniq = []
    for i in ids:
        if i[0] not in seen:
            seen.add(i[0]); ids_uniq.append(i)

    print(f"[INFO] IDs PNCP (únicos): {len(ids_uniq)}")
    for id_pncp, link in ids_uniq[:20]:
        print(f"  - {id_pncp} | {link}")
    if len(ids_uniq) > 20:
        print(f"  ... (+{len(ids_uniq)-20} ids)")

    itens_all = fetch_itens_para_ids(session, ids_uniq)

    encontrados = []
    for row in itens_all:
        desc = row.get("Descricao") or ""
        if not termos_re or termos_re.search(desc):
            encontrados.append(row)

    print("\n===== ITENS ENCONTRADOS =====")
    for r in encontrados:
        print(f"[{r['id_pncp']}] Item {r['Numero']}: {r['Descricao']}")
        print(f"  Quantidade: {r['Quantidade']}")
        print(f"  VU estimado: {r['Valor unitario estimado']}")
        print(f"  VT estimado: {r['Valor total estimado']}")
        print(f"  Detalhar: {r['Detalhar']}")
        print(f"  Edital: {r['Edital']}\n")
    print(f"[OK] Total de itens encontrados: {len(encontrados)}")

    meta = {
        "palavra_contratacao": palavra_contratacao,
        "termos_itens": s_itens_raw,
        "data_inicial": data_inicial,
        "data_final": data_final,
        "split_mode": SPLIT_MODE,
        "modalidades": ";".join(str(m) for m in modalidades),
        "modo_disputa": modo if modo is not None else "(não informado)",
    }
    report_path = generate_report_txt(encontrados, meta)
    print(f"[OK] Relatório salvo: {report_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] {e}")
