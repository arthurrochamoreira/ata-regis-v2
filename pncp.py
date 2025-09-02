import re
import time
import math
import requests
import json
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================== Configs ==============================
BASE_CONSULTA = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
BASE_ITENS = "https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
HEADERS = {"Accept": "*/*", "User-Agent": "pncp-script-py/1.3"}

# Limites de concorrência
MAX_WORKERS_PAGES = 20      # threads para páginas
MAX_WORKERS_ITENS = 32      # threads para itens
CONC_ITENS = threading.Semaphore(MAX_WORKERS_ITENS) # Semáforo para controlar concorrência de itens

# Tentativas de tamanho de página (ordem de teste) e cache por (modalidade, modo)
PAGE_SIZE_CANDIDATES = [500, 200, 100, 50, None]
PAGE_SIZE_CACHE: dict[tuple[int, int | None], int | None] = {}

# Timeouts e retries
DEFAULT_TIMEOUT = 60
ADAPTER_RETRY = Retry(
    total=2, backoff_factor=0.3,
    status_forcelist=[502, 503, 504],
    allowed_methods=["GET"]
)

# Modalidades (apenas referência impressa)
MODALIDADES = {
    1: "Leilão – Eletrônico", 2: "Diálogo Competitivo", 3: "Concurso",
    4: "Concorrência – Eletrônica", 5: "Concorrência – Presencial",
    6: "Pregão – Eletrônico", 7: "Pregão – Presencial", 8: "Dispensa de Licitação",
    9: "Inexigibilidade", 10: "Manifestação de Interesse", 11: "Pré-qualificação",
    12: "Credenciamento", 13: "Leilão – Presencial",
}

# ============================== Utilidades ==============================
def jloads(b): return json.loads(b.decode('utf-8'))
def jdumps(d): return json.dumps(d, ensure_ascii=False, indent=2).encode('utf-8')

def parse_id_pncp(id_pncp: str) -> tuple[str, str, str]:
    parts = id_pncp.split("/")
    if len(parts) != 3:
        raise ValueError(f"ID PNCP inválido: {id_pncp}")
    return parts[0], parts[1], parts[2]

def format_id_pncp_from_numero_controle(raw: str) -> tuple[str | None, str | None]:
    """
    Tenta formatar um ID PNCP e link a partir do campo 'numeroControlePncp'.
    Exemplo de entrada: '01234567000189-1-00001/2024'
    Saída esperada: ('01234567000189/2024/1', 'https://pncp.gov.br/app/editais/01234567000189/2024/1')
    """
    try:
        left, ano = raw.split("/")
        cnpj = left.split("-")[0]
        seq_raw = left.split("-")[-1]
        seq = seq_raw.lstrip("0") or "0" # Remove zeros à esquerda, mas mantém "0" se for o caso
        id_pncp = f"{cnpj}/{ano}/{seq}"
        link = f"https://pncp.gov.br/app/editais/{id_pncp}"
        return id_pncp, link
    except Exception:
        return None, None

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
        body = "<sem corpo>"
        try:
            body = r.text[:1000]
        except Exception:
            pass
        raise requests.HTTPError(f"HTTP {r.status_code} em {r.url}\n{body}")

# ============================== Consultas de Contratações ==============================
def fetch_page_with_pagesize(session: requests.Session, pagina: int, data_ini: str, data_fim: str,
                             modalidade: int, modo: int | None):
    key = (modalidade, modo)
    base_params = {
        "dataInicial": data_ini, "dataFinal": data_fim,
        "codigoModalidadeContratacao": int(modalidade), "pagina": pagina,
    }
    if modo is not None:
        base_params["codigoModoDisputa"] = int(modo)

    if key in PAGE_SIZE_CACHE:
        ps = PAGE_SIZE_CACHE[key]
        params = dict(base_params)
        if ps is not None:
            params["tamanhoPagina"] = ps
        return get_with_backoff(session, BASE_CONSULTA, params=params).json()

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
            msg = str(e)
            if "Tamanho de página inválido" in msg or "400" in msg:
                last_err = e
                continue
            raise
    raise last_err or RuntimeError("Nenhum tamanhoPagina aceito pelo servidor.")

def discover_total_pages_for_modalidade(session: requests.Session, data_ini: str, data_fim: str,
                                        modalidade: int, modo: int | None):
    payload = fetch_page_with_pagesize(session, 1, data_ini, data_fim, modalidade, modo)
    total = payload.get("totalPaginas") or payload.get("totalPaginasConsulta") or 1
    dados = payload.get("data", [])
    print(f"[INFO] modalidade={modalidade:>2} ({MODALIDADES.get(modalidade,'?')}) "
          f"totalPaginas={total} | pág1={len(dados)} registros")
    return int(total), dados

def fetch_all_pages_for_modalidade(session: requests.Session, total_pages: int, data_ini: str, data_fim: str,
                                     modalidade: int, modo: int | None) -> list[dict]:
    if total_pages <= 1:
        return []
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_PAGES) as ex:
        futures = {ex.submit(fetch_page_with_pagesize, session, p, data_ini, data_fim, modalidade, modo): p for p in range(2, total_pages + 1)}
        for fut in as_completed(futures):
            try:
                payload = fut.result()
                lote = payload.get("data", [])
                print(f"[MOD {modalidade:>2}] [PÁG {futures[fut]}] {len(lote)} registros")
                results.extend(lote)
            except Exception as e:
                print(f"[ERRO] Página {futures[fut]} (mod {modalidade}) falhou: {e}")
    return results

def fetch_contratacoes_multi_modalidade(session: requests.Session, data_ini: str, data_fim: str,
                                        modalidades: list[int], modo: int | None) -> list[dict]:
    all_contratacoes = []
    discovered = []
    for m in modalidades:
        try:
            discovered.append((m, discover_total_pages_for_modalidade(session, data_ini, data_fim, m, modo)))
        except Exception as e:
            print(f"[ERRO] Descobrir páginas da modalidade {m} falhou: {e}")

    for m, (total, page1) in discovered:
        all_contratacoes.extend(page1)
        outras = fetch_all_pages_for_modalidade(session, total, data_ini, data_fim, m, modo)
        all_contratacoes.extend(outras)

    return all_contratacoes

# ============================== Itens (paralelismo, sem cache) ==============================
def itens_pncp_por_id(session: requests.Session, id_pncp: str) -> list[dict]:
    """
    Busca e formata os itens de uma compra diretamente da API, sem usar cache.
    """
    cnpj, ano, seq = parse_id_pncp(id_pncp)
    
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
            "Descricao": desc.strip(),
            "Quantidade": qtd,
            "Valor unitario estimado": vu,
            "Valor total estimado": vt,
            "Detalhar": detalhar,
            "Edital": edital_link
        })

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

        # 1) Coletar contratações de TODAS as modalidades
        contratacoes = fetch_contratacoes_multi_modalidade(session, data_inicial, data_final, modalidades, modo)
        print(f"\n[INFO] Contratações coletadas (todas as modalidades): {len(contratacoes)}")

        # 2) Filtrar por palavra no objeto
        filtradas = [
            c for c in contratacoes
            if palavra_contratacao in (c.get("objetoCompra") or "").lower()
        ]
        print(f"[INFO] Contratações após filtro no objeto: {len(filtradas)}")

        # 3) Coletar IDs únicos e mapear para objeto e link da compra
        ids_uniq = []
        id_to_info = {}
        seen_ids = set()
        for c in filtradas:
            id_pncp, link = None, None
            
            # Tentativa 1: Usar a nova função com numeroControlePncp
            numero_controle = c.get("numeroControlePncp")
            if numero_controle:
                id_pncp, link = format_id_pncp_from_numero_controle(numero_controle)

            # Tentativa 2 (Fallback): Montar manualmente se a primeira falhar
            if not id_pncp:
                try:
                    cnpj = c["orgaoEntidade"]["cnpj"]
                    ano = c["anoCompra"]
                    seq = c["sequencialCompra"]
                    id_pncp = f"{cnpj}/{ano}/{seq}"
                    link = f"https://pncp.gov.br/app/editais/{id_pncp}"
                except (KeyError, TypeError):
                    continue # Pula esta contratação se não tiver os dados mínimos

            # Se conseguimos um ID, adicionamos para processamento
            if id_pncp and id_pncp not in seen_ids:
                ids_uniq.append((id_pncp, id_pncp))
                seen_ids.add(id_pncp)
                id_to_info[id_pncp] = {
                    "objeto": (c.get("objetoCompra") or "").strip(),
                    "link": link
                }
        
        print(f"[INFO] Buscando itens para {len(ids_uniq)} contratações únicas.")

        # 4) Buscar itens das contratações (com paralelismo)
        itens_brutos = fetch_itens_para_ids(session, ids_uniq)

        # 5) Filtrar itens por termos (OR via regex)
        encontrados_finais = []
        for item in itens_brutos:
            desc = item.get("Descricao", "")
            if not termos_re or termos_re.search(desc):
                encontrados_finais.append(item)

        # 6) Saída
        print("\n--- RESULTADOS ENCONTRADOS ---")
        for r in encontrados_finais:
            id_pncp = r['id_pncp']
            info = id_to_info.get(id_pncp, {"objeto": "?", "link": "Link não encontrado"})
            objeto = info["objeto"]
            link = info["link"]
            print(f"[{id_pncp}] "
                  f"Item {r['Numero']}: {r['Descricao']} "
                  f"(Objeto: {objeto})\n"
                  f" -> Link: {link}\n")

        print(f"\n[OK] Total de itens encontrados: {len(encontrados_finais)}")

    except Exception as e:
        print(f"[FATAL] {e}")