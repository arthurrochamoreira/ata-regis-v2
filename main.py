import os
import re
import time
import math
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================== Configs ==============================
BASE_CONSULTA = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
BASE_ITENS = "https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
HEADERS = {"Accept": "*/*", "User-Agent": "pncp-teste/mt/1.0"}

# Limites de concorrência
MAX_WORKERS_PAGES = 20     # threads para páginas
MAX_WORKERS_ITENS = 32     # threads para itens

# Tentativas de tamanho de página (ordem de teste) e cache por (modalidade, modo)
PAGE_SIZE_CANDIDATES = [500, 200, 100, 50, None]
PAGE_SIZE_CACHE: dict[tuple[int, int | None], int | None] = {}

# Timeouts e retries (mantidos da sua versão)
DEFAULT_TIMEOUT = 60
ADAPTER_RETRY = Retry(
    total=2, backoff_factor=0.3,
    status_forcelist=[502, 503, 504],
    allowed_methods=["GET"]
)

# Modalidades (apenas referência impressa)
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

# ============================== Utilidades ==============================
def ask_date(prompt):
    s = input(prompt).strip()
    datetime.strptime(s, "%Y%m%d")  # valida AAAAMMDD
    return s

def parse_terms(raw: str) -> list[str]:
    return [t.strip() for t in raw.split(";") if t.strip()]

def compile_or_regex(terms: list[str]) -> re.Pattern | None:
    if not terms:
        return None
    pat = "|".join(re.escape(t) for t in terms)
    return re.compile(pat, flags=re.IGNORECASE)

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

def build_session():
    sess = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=MAX_WORKERS_PAGES + MAX_WORKERS_ITENS + 4,
        pool_maxsize=MAX_WORKERS_PAGES + MAX_WORKERS_ITENS + 8,
        max_retries=ADAPTER_RETRY,
    )
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS)
    return sess

def get_with_backoff(session: requests.Session, url: str, *, params=None,
                     timeout=DEFAULT_TIMEOUT, max_retries=4):
    """
    Retry com backoff para 429 e 5xx (mantido da sua versão).
    Em 4xx != 429, falha imediata.
    """
    attempt = 0
    while True:
        r = session.get(url, params=params, timeout=timeout)
        if 200 <= r.status_code < 300:
            return r
        if r.status_code == 429 or 500 <= r.status_code < 600:
            attempt += 1
            if attempt > max_retries:
                raise requests.HTTPError(f"HTTP {r.status_code} em {r.url}\n{r.text[:1000]}")
            wait = (1.4 ** attempt) + attempt * 0.25
            print(f"[WARN] {r.status_code} em {r.url} — retry {attempt}/{max_retries} em {wait:.1f}s")
            time.sleep(wait)
            continue
        # outros 4xx => falha imediata com corpo do erro
        body = "<sem corpo>"
        try:
            body = r.text[:1000]
        except Exception:
            pass
        raise requests.HTTPError(f"HTTP {r.status_code} em {r.url}\n{body}")

# ============================== Consultas (com fallback de página) ==============================
def fetch_page_with_pagesize(session: requests.Session, pagina: int, data_ini: str, data_fim: str,
                             modalidade: int, modo: int | None):
    """
    Busca UMA página tentando tamanhos de página conforme PAGE_SIZE_CANDIDATES.
    Cacheia o tamanho aceito por (modalidade, modo).
    """
    key = (modalidade, modo)
    base_params = {
        "dataInicial": data_ini,
        "dataFinal":   data_fim,
        "codigoModalidadeContratacao": int(modalidade),
        "pagina": pagina,
    }
    if modo is not None:
        base_params["codigoModoDisputa"] = int(modo)

    # se já descobrimos um tamanho válido, usa direto
    if key in PAGE_SIZE_CACHE:
        ps = PAGE_SIZE_CACHE[key]
        params = dict(base_params)
        if ps is not None:
            params["tamanhoPagina"] = ps
        r = get_with_backoff(session, BASE_CONSULTA, params=params)
        return r.json()

    # senão: tenta em ordem
    last_err = None
    for ps in PAGE_SIZE_CANDIDATES:
        params = dict(base_params)
        if ps is not None:
            params["tamanhoPagina"] = ps
        try:
            r = get_with_backoff(session, BASE_CONSULTA, params=params)
            payload = r.json()
            PAGE_SIZE_CACHE[key] = ps
            info_ps = "padrão do servidor" if ps is None else str(ps)
            print(f"[INFO] usando tamanhoPagina={info_ps} para modalidade={modalidade}")
            return payload
        except requests.HTTPError as e:
            # se for erro de tamanho inválido (400 do PNCP), tenta próximo
            msg = str(e)
            if "Tamanho de página inválido" in msg or "400" in msg:
                last_err = e
                continue
            # outros erros: repassa
            raise
    raise last_err or RuntimeError("Nenhum tamanhoPagina aceito pelo servidor.")

def discover_total_pages_for_modalidade(session: requests.Session, data_ini: str, data_fim: str,
                                        modalidade: int, modo: int | None):
    """Baixa a página 1, retorna (totalPaginas, data_pag_1)."""
    payload = fetch_page_with_pagesize(session, 1, data_ini, data_fim, modalidade, modo)
    total = payload.get("totalPaginas") or payload.get("totalPaginasConsulta") or 1
    dados = payload.get("data", [])
    print(f"[INFO] modalidade={modalidade:>2} ({MODALIDADES.get(modalidade,'?')}) "
          f"totalPaginas={total} | pág1={len(dados)} registros")
    return int(total), dados

def fetch_all_pages_for_modalidade(session: requests.Session, total_pages: int, data_ini: str, data_fim: str,
                                   modalidade: int, modo: int | None) -> list[dict]:
    """Busca páginas 2..N em paralelo para UMA modalidade."""
    results = []

    def _one(p):
        payload = fetch_page_with_pagesize(session, p, data_ini, data_fim, modalidade, modo)
        lote = payload.get("data", [])
        print(f"[MOD {modalidade:>2}] [PÁG {p}] {len(lote)} registros")
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

def fetch_contratacoes_multi_modalidade(session: requests.Session, data_ini: str, data_fim: str,
                                        modalidades: list[int], modo: int | None) -> list[dict]:
    """Orquestra a coleta para várias modalidades."""
    all_contratacoes = []
    # 1) descobrir páginas para cada modalidade
    discovered = []
    for m in modalidades:
        try:
            discovered.append((m, discover_total_pages_for_modalidade(session, data_ini, data_fim, m, modo)))
        except Exception as e:
            print(f"[ERRO] Descobrir páginas da modalidade {m} falhou: {e}")

    # 2) coletar páginas 2..N para cada modalidade
    for m, (total, page1) in discovered:
        all_contratacoes.extend(page1)
        outras = fetch_all_pages_for_modalidade(session, total, data_ini, data_fim, m, modo)
        all_contratacoes.extend(outras)

    return all_contratacoes

# ============================== Itens (paralelo) ==============================
def fetch_itens_for_compra(session: requests.Session, cnpj, ano, seq):
    url = BASE_ITENS.format(cnpj=cnpj, ano=ano, seq=seq)
    r = get_with_backoff(session, url)
    data = r.json()
    return data.get("data", data) if isinstance(data, dict) else data

def fetch_itens_multithread(session: requests.Session, contratacoes: list[dict]) -> dict[tuple[str, int, int], list[dict]]:
    """Recebe contratações e retorna {(cnpj,ano,seq): [itens]} (dedup interno)."""
    resultados = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_ITENS) as ex:
        futures = {}
        seen = set()
        for c in contratacoes:
            try:
                cnpj = c["orgaoEntidade"]["cnpj"]
                ano  = c["anoCompra"]
                seq  = c["sequencialCompra"]
                key = (cnpj, ano, seq)
            except KeyError:
                continue
            if key in seen:
                continue
            seen.add(key)
            futures[ex.submit(fetch_itens_for_compra, session, cnpj, ano, seq)] = key

        for fut in as_completed(futures):
            key = futures[fut]
            try:
                resultados[key] = fut.result() or []
            except Exception as e:
                print(f"[WARN] Itens {key} falharam: {e}")
                resultados[key] = []
    return resultados

# ============================== MAIN ==============================
if __name__ == "__main__":
    try:
        print("Modalidades disponíveis (código: nome):")
        print(", ".join(f"{k}: {v}" for k, v in MODALIDADES.items()))

        palavra_contratacao = input("Palavra no OBJETO da contratação: ").strip().lower()
        termos_itens_raw = input("Termos na DESCRIÇÃO do item (separe por ';'): ").strip()
        termos_itens = parse_terms(termos_itens_raw)
        termos_re = compile_or_regex(termos_itens)

        modalidades_raw = input("Códigos de modalidade (ex.: 6;8;9): ").strip() or "6"
        modalidades = parse_modalidades(modalidades_raw)

        modo_raw = input("Código do modo de disputa (opcional, Enter p/ pular): ").strip()
        modo = int(modo_raw) if modo_raw else None

        data_inicial = ask_date("Data inicial (AAAAMMDD): ")
        data_final   = ask_date("Data final   (AAAAMMDD): ")

        session = build_session()

        # 1) Coletar contratações de TODAS as modalidades (com descoberta de páginas + fallback de tamanho)
        contratacoes = fetch_contratacoes_multi_modalidade(session, data_inicial, data_final, modalidades, modo)
        print(f"\n[INFO] Contratações coletadas (todas as modalidades): {len(contratacoes)}")

        # 2) Filtrar por palavra no objeto
        filtradas = [
            c for c in contratacoes
            if palavra_contratacao in (c.get("objetoCompra") or "").lower()
        ]
        print(f"[INFO] Contratações após filtro no objeto: {len(filtradas)}")

        # 3) Buscar itens das contratações filtradas (dedup interno)
        itens_map = fetch_itens_multithread(session, filtradas)

        # 4) Filtrar itens por termos (OR via regex)
        encontrados = []
        for c in filtradas:
            key = (c["orgaoEntidade"]["cnpj"], c["anoCompra"], c["sequencialCompra"])
            itens = itens_map.get(key, [])
            obj = (c.get("objetoCompra") or "").strip()
            for it in itens:
                desc = (it.get("descricao") or it.get("descricaoItem") or "")
                if not termos_re or termos_re.search(desc):
                    encontrados.append({
                        "cnpj": key[0], "ano": key[1], "sequencial": key[2],
                        "numeroItem": it.get("numeroItem"),
                        "descricaoItem": (desc or "").strip(),
                        "objeto": obj
                    })

        # 5) Saída
        for r in encontrados:
            print(f"[{r['cnpj']}/{r['ano']}/{r['sequencial']}] "
                  f"Item {r['numeroItem']}: {r['descricaoItem']} "
                  f"(Objeto: {r['objeto']})")

        print(f"\n[OK] Total de itens encontrados: {len(encontrados)}")

    except Exception as e:
        print(f"[FATAL] {e}")
