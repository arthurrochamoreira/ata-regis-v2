import os
import re
import time
import math
import requests
import json
import threading
import calendar
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================== Configs ==============================
BASE_CONSULTA = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
BASE_ITENS = "https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
HEADERS = {"Accept": "*/*", "User-Agent": "pncp-script-py/1.5"}
PAUSA_ENTRE_MESES_SEGUNDOS = 30  # Pausa entre meses

# Limites de concorrência
MAX_WORKERS_PAGES = 20      # threads para páginas
MAX_WORKERS_ITENS = 32      # threads para itens
CONC_ITENS = threading.Semaphore(MAX_WORKERS_ITENS)  # controle de paralelismo para itens

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

# Mapas amigáveis (não obrigatórios; se não houver mapeamento, mostra o código cru)
PODER_MAP = {
    "E": "Executivo",
    "L": "Legislativo",
    "J": "Judiciário",
    "M": "Ministério Público",
    "D": "Defensoria Pública",
    "T": "Tribunais de Contas",
    "N": "Município",  # costuma aparecer para Prefeitura
}
ESFERA_MAP = {
    "U": "União",
    "E": "Estadual",
    "M": "Municipal",
    "D": "Distrito Federal",
}

# ============================== Utilidades ==============================
def jloads(b):
    return json.loads(b.decode('utf-8'))

def jdumps(d):
    return json.dumps(d, ensure_ascii=False, indent=2).encode('utf-8')

def _fmt_date(v):
    """Formata datas conhecidas; se não houver valor, retorna None (para não imprimir)."""
    if not v:
        return None
    s = str(v)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:len(fmt)], fmt).strftime("%d/%m/%Y")
        except Exception:
            pass
    if re.fullmatch(r"\d{8}", s):
        try:
            return datetime.strptime(s, "%Y%m%d").strftime("%d/%m/%Y")
        except Exception:
            pass
    return s  # retorna cru se não reconhecido

def _get(d, *paths, default=None):
    for p in paths:
        cur = d
        ok = True
        for k in p:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and cur not in (None, ""):
            return cur
    return default

def gerar_intervalos_mensais(data_inicial_str: str, data_final_str: str) -> list[tuple[str, str]]:
    fmt = "%Y%m%d"
    start_date = datetime.strptime(data_inicial_str, fmt)
    end_date = datetime.strptime(data_final_str, fmt)
    intervalos = []
    current_date = start_date
    while current_date <= end_date:
        _, last_day_of_month = calendar.monthrange(current_date.year, current_date.month)
        month_start = current_date.replace(day=1)
        month_end = current_date.replace(day=last_day_of_month)
        intervalo_start = max(start_date, month_start)
        intervalo_end = min(end_date, month_end)
        intervalos.append((intervalo_start.strftime(fmt), intervalo_end.strftime(fmt)))
        current_date = month_end + timedelta(days=1)
    return intervalos

def parse_id_pncp(id_pncp: str) -> tuple[str, str, str]:
    parts = id_pncp.split("/")
    if len(parts) != 3:
        raise ValueError(f"ID PNCP inválido: {id_pncp}")
    return parts[0], parts[1], parts[2]

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

def ask_date(prompt):
    s = input(prompt).strip()
    datetime.strptime(s, "%Y%m%d")
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

# ------------------------- Metadados da contratação (apenas o que a API entrega) -------------------------
def extract_contratacao_meta(c: dict) -> dict:
    """
    Extrai SOMENTE campos que existem no endpoint /consulta/v1/contratacoes/publicacao,
    sem inventar chaves e sem colocar 'N/D'. Campos ausentes simplesmente não entram.
    Agora inclui: razaoSocial, cnpj, poderId, esferaId (com rótulos).
    """
    meta: dict = {}

    # --- Órgão / Entidade (orgaoEntidade) ---
    org = c.get("orgaoEntidade") or {}
    if isinstance(org, dict):
        razao = org.get("razaosocial") or org.get("razaoSocial")
        if razao:
            meta["Órgão"] = razao

        cnpj_org = org.get("cnpj")
        if cnpj_org:
            meta["CNPJ do órgão"] = cnpj_org

        poder = org.get("poderId")
        if poder:
            rotulo_poder = PODER_MAP.get(poder)
            meta["Poder"] = f"{poder} - {rotulo_poder}" if rotulo_poder else str(poder)

        esfera = org.get("esferaId")
        if esfera:
            rotulo_esfera = ESFERA_MAP.get(esfera)
            meta["Esfera"] = f"{esfera} - {rotulo_esfera}" if rotulo_esfera else str(esfera)

    # --- Modalidade ---
    cod_mod = c.get("codigoModalidadeContratacao")
    if cod_mod is not None:
        try:
            cod_i = int(cod_mod)
            desc = MODALIDADES.get(cod_i)
            meta["Modalidade da contratação"] = f"{cod_i} - {desc}" if desc else str(cod_i)
        except Exception:
            meta["Modalidade da contratação"] = str(cod_mod)

    # --- Amparo legal ---
    amparo = c.get("amparoLegal")
    if isinstance(amparo, dict):
        amparo_str = amparo.get("nome") or amparo.get("descricao")
    else:
        amparo_str = str(amparo) if amparo else None
    if amparo_str:
        meta["Amparo legal"] = amparo_str

    # --- Modo de disputa (na consulta costuma vir como código) ---
    cod_modo = c.get("codigoModoDisputa")
    if cod_modo is not None:
        meta["Modo de disputa"] = str(cod_modo)

    # --- Registro de preço ---
    reg_preco = c.get("registroPreco")
    if isinstance(reg_preco, bool):
        meta["Registro de preço"] = "Sim" if reg_preco else "Não"

    # --- Datas ---
    data_divulgacao = _fmt_date(c.get("dataDivulgacaoPncp"))
    if data_divulgacao:
        meta["Data de divulgação no PNCP"] = data_divulgacao

    data_ini_prop = _fmt_date(c.get("dataInicioRecebimentoProposta"))
    if data_ini_prop:
        meta["Data de início de recebimento de propostas"] = data_ini_prop

    data_fim_prop = _fmt_date(c.get("dataFimRecebimentoProposta"))
    if data_fim_prop:
        meta["Data fim de recebimento de propostas"] = data_fim_prop

    # --- Situação ---
    situacao = c.get("situacao")
    if situacao:
        meta["Situação"] = situacao

    # --- Id PNCP + links úteis ---
    numero_controle = c.get("numeroControlePNCP") or c.get("numeroControlePncp")
    id_pncp, link_edital = (None, None)
    if numero_controle:
        id_pncp, link_edital = format_id_pncp_from_numero_controle(numero_controle)
    if id_pncp:
        meta["Id contratação PNCP"] = id_pncp
        meta["Fonte"] = {
            "edital": link_edital,
            "api_itens": f"https://pncp.gov.br/api/pncp/v1/orgaos/{id_pncp.replace('/', '/compras/', 1)}/itens"
        }
    else:
        # fallback: monta por cnpj/ano/seq se vierem no payload
        cnpj = _get(c, ("orgaoEntidade", "cnpj"))
        ano = c.get("anoCompra")
        seq = c.get("sequencialCompra")
        if cnpj and ano and seq:
            meta["Id contratação PNCP"] = f"{cnpj}/{ano}/{seq}"
            meta["Fonte"] = {
                "edital": f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}",
                "api_itens": f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/compras/{ano}/{seq}/itens"
            }

    return meta

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

# ============================== Itens (paralelismo) ==============================
def itens_pncp_por_id(session: requests.Session, id_pncp: str) -> list[dict]:
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
    if not ids_uniq:
        return []
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

# ============================== Saída / Relatórios ==============================
def ensure_dirs(root: str, year: str, month: str):
    """
    Garante a estrutura:
      root/
        json/YYYY/MM/
        txt/YYYY/MM/
    """
    json_dir = os.path.join(root, "json", year, month)
    txt_dir  = os.path.join(root, "txt",  year, month)
    os.makedirs(json_dir, exist_ok=True)
    os.makedirs(txt_dir, exist_ok=True)
    return json_dir, txt_dir

def _append_if_present(lines: list[str], label: str, value):
    if value not in (None, "", [], {}):
        lines.append(f"{label}: {value}")

def salvar_relatorios(json_path: str, txt_path: str, dados_json: dict, dados_txt: dict):
    """
    Salva JSON (todos os itens) e TXT (itens filtrados).
    Retorna a string do conteúdo TXT gerado (para compor o unificado) e a contagem de itens filtrados.
    """
    # JSON
    if dados_json:
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(dados_json, f, ensure_ascii=False, indent=4)
            print(f"[OK] JSON salvo: {json_path}")
        except Exception as e:
            print(f"[ERRO] Falha ao salvar JSON: {e}")
    else:
        print("[INFO] Nenhum item bruto para salvar no JSON.")

    # TXT
    txt_content_lines = []
    total_itens_filtrados = 0
    if dados_txt:
        header = [
            "RELATÓRIO DE ITENS FILTRADOS NO PNCP",
            f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
        ]
        txt_content_lines.extend([header[0], header[1]])

        for id_pncp, data in dados_txt.items():
            meta = data.get('metadados', {}) or {}

            txt_content_lines.append("\n============================================================")
            _append_if_present(txt_content_lines, "CONTRATAÇÃO ID", meta.get('Id contratação PNCP') or id_pncp)
            _append_if_present(txt_content_lines, "Objeto", data.get('objeto'))
            fonte = (meta.get('Fonte') or {})
            _append_if_present(txt_content_lines, "Link do Edital", fonte.get('edital'))

            txt_content_lines.append("------------------------------------------------------------")

            # Imprime SOMENTE os campos existentes no meta (sem N/D)
            order = [
                "Órgão",
                "CNPJ do órgão",
                "Poder",
                "Esfera",
                "Modalidade da contratação",
                "Amparo legal",
                "Modo de disputa",
                "Registro de preço",
                "Data de divulgação no PNCP",
                "Situação",
                "Data de início de recebimento de propostas",
                "Data fim de recebimento de propostas",
            ]
            for k in order:
                if k in meta and meta[k] not in (None, "", [], {}):
                    txt_content_lines.append(f"{k}: {meta[k]}")

            _append_if_present(txt_content_lines, "Fonte (API Itens)", fonte.get('api_itens'))

            txt_content_lines.append("------------------------------------------------------------")
            txt_content_lines.append("Itens Encontrados (filtrados por descrição):\n")

            for item in data.get('itens_filtrados', []):
                txt_content_lines.append(f"  - Item {item.get('Numero', '?')}:")
                _append_if_present(txt_content_lines, "    Descrição", item.get('Descricao'))
                _append_if_present(txt_content_lines, "    Quantidade", item.get('Quantidade'))
                _append_if_present(txt_content_lines, "    Valor Unitário Estimado", item.get('Valor unitario estimado'))
                _append_if_present(txt_content_lines, "    Valor Total Estimado", item.get('Valor total estimado'))
                txt_content_lines.append("")  # linha em branco entre itens
                total_itens_filtrados += 1

        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write("\n".join(txt_content_lines).rstrip() + "\n")
            print(f"[OK] TXT salvo: {txt_path}")
        except Exception as e:
            print(f"[ERRO] Falha ao salvar TXT: {e}")
    else:
        print("[INFO] Nenhum item correspondeu à descrição para gerar TXT.")
    return ("\n".join(txt_content_lines).rstrip() + "\n") if txt_content_lines else "", total_itens_filtrados

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

        data_inicial_geral = ask_date("Data inicial GERAL (AAAAMMDD): ")
        data_final_geral   = ask_date("Data final GERAL   (AAAAMMDD): ")

        session = build_session()

        # Diretórios raiz dos relatórios
        ROOT_REL = os.path.join(os.getcwd(), "Relatórios")
        os.makedirs(ROOT_REL, exist_ok=True)

        intervalos_mensais = gerar_intervalos_mensais(data_inicial_geral, data_final_geral)
        total_meses = len(intervalos_mensais)
        print(f"\n[INFO] O período foi dividido em {total_meses} busca(s) mensal(is).")

        # Para compor o TXT UNIFICADO ao final:
        unificado_sections = []
        total_itens_filtrados_geral = 0

        for i, (data_inicial_mes, data_final_mes) in enumerate(intervalos_mensais):
            print(f"\n{'='*20} BUSCANDO MÊS {i+1}/{total_meses}: {data_inicial_mes} a {data_final_mes} {'='*20}")

            # 1) Coletar contratações do mês
            contratacoes_mes = fetch_contratacoes_multi_modalidade(session, data_inicial_mes, data_final_mes, modalidades, modo)
            print(f"\n[INFO] Mês {i+1}: {len(contratacoes_mes)} contratações coletadas.")

            # 2) Filtrar por palavra no objeto
            filtradas_mes = [c for c in contratacoes_mes if palavra_contratacao in (c.get("objetoCompra") or "").lower()]
            print(f"[INFO] Mês {i+1}: {len(filtradas_mes)} contratações após filtro no objeto.")

            # 3) Coletar IDs únicos + metadados
            id_to_info_mes = {}
            ids_do_mes = []
            for c in filtradas_mes:
                id_pncp, link = None, None
                numero_controle = c.get("numeroControlePncp") or c.get("numeroControlePNCP")
                if numero_controle:
                    id_pncp, link = format_id_pncp_from_numero_controle(numero_controle)
                if not id_pncp:
                    try:
                        cnpj, ano, seq = c["orgaoEntidade"]["cnpj"], c["anoCompra"], c["sequencialCompra"]
                        id_pncp, link = f"{cnpj}/{ano}/{seq}", f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
                    except (KeyError, TypeError):
                        continue

                if id_pncp and id_pncp not in id_to_info_mes:
                    meta = extract_contratacao_meta(c)
                    # garante ID/link dentro dos metadados, sem "N/D"
                    if not meta.get("Id contratação PNCP"):
                        meta["Id contratação PNCP"] = id_pncp
                        meta["Fonte"] = {
                            "edital": link,
                            "api_itens": f"https://pncp.gov.br/api/pncp/v1/orgaos/{id_pncp.replace('/', '/compras/', 1)}/itens"
                        }
                    else:
                        # completa links se faltarem
                        fonte = meta.setdefault("Fonte", {})
                        fonte.setdefault("edital", link)
                        fonte.setdefault("api_itens", f"https://pncp.gov.br/api/pncp/v1/orgaos/{id_pncp.replace('/', '/compras/', 1)}/itens")

                    id_to_info_mes[id_pncp] = {
                        "objeto": (c.get("objetoCompra") or "").strip(),
                        "link": link,
                        "metadados": meta,
                    }
                    ids_do_mes.append(id_pncp)

            print(f"[INFO] Mês {i+1}: {len(ids_do_mes)} contratações únicas para buscar itens.")

            # 4) Buscar itens das contratações
            itens_brutos_mes = fetch_itens_para_ids(session, [(i, i) for i in ids_do_mes])

            # 5a) JSON: todos os itens + metadados
            dados_para_json_mes = {}
            for item in itens_brutos_mes:
                idp = item['id_pncp']
                if idp not in dados_para_json_mes:
                    base = id_to_info_mes.get(idp, {})
                    dados_para_json_mes[idp] = {
                        "objeto": base.get("objeto"),
                        "link": base.get("link"),
                        "metadados": base.get("metadados", {}),
                        "todos_os_itens": []
                    }
                dados_para_json_mes[idp]['todos_os_itens'].append(item)

            # 5b) TXT: somente itens filtrados + metadados
            itens_filtrados_mes = [item for item in itens_brutos_mes if not termos_re or termos_re.search(item.get("Descricao", ""))]
            dados_para_txt_mes = {}
            for item in itens_filtrados_mes:
                idp = item['id_pncp']
                if idp not in dados_para_txt_mes:
                    base = id_to_info_mes.get(idp, {})
                    dados_para_txt_mes[idp] = {
                        "objeto": base.get("objeto"),
                        "link": base.get("link"),
                        "metadados": base.get("metadados", {}),
                        "itens_filtrados": []
                    }
                dados_para_txt_mes[idp]['itens_filtrados'].append(item)

            # 6) Saída por mês (com estrutura de diretórios /Relatórios/json/YYYY/MM/ e /Relatórios/txt/YYYY/MM/)
            dt_start = datetime.strptime(data_inicial_mes, "%Y%m%d")
            year = f"{dt_start.year:04d}"
            month = f"{dt_start.month:02d}"
            json_dir, txt_dir = ensure_dirs(ROOT_REL, year, month)

            json_path = os.path.join(json_dir, f"relatorio_pncp_{year}-{month}.json")
            txt_path  = os.path.join(txt_dir,  f"relatorio_pncp_{year}-{month}.txt")

            txt_block, qtd_itens_filtrados_mes = salvar_relatorios(json_path, txt_path, dados_para_json_mes, dados_para_txt_mes)

            # Para o UNIFICADO (rótulo de mês):
            if txt_block:
                titulo_mes = f"\n########################  {year}-{month}  ########################\n"
                unificado_sections.append(titulo_mes + txt_block)
                total_itens_filtrados_geral += qtd_itens_filtrados_mes

            # 7) Resumo
            print(f"\n[RESUMO {year}-{month}] Contratações com itens filtrados: {len(dados_para_txt_mes)}")
            print(f"[RESUMO {year}-{month}] Itens individuais filtrados: {qtd_itens_filtrados_mes}")

            # 8) Pausa entre meses
            if i < total_meses - 1:
                print(f"\n[PAUSA] Fim do mês {year}-{month}. Pausando por {PAUSA_ENTRE_MESES_SEGUNDOS}s...")
                time.sleep(PAUSA_ENTRE_MESES_SEGUNDOS)

        # ================= UNIFICADO AO FINAL ==================
        # arquivo: Relatórios/txt/_UNIFICADO_{AAAAMMDD}_{AAAAMMDD}.txt
        unificado_dir = os.path.join(ROOT_REL, "txt")
        os.makedirs(unificado_dir, exist_ok=True)
        unificado_name = f"_UNIFICADO_{data_inicial_geral}_{data_final_geral}.txt"
        unificado_path = os.path.join(unificado_dir, unificado_name)

        header_unificado = [
            "RELATÓRIO PNCP — UNIFICADO (TODOS OS MESES)\n",
            f"Período coberto: {datetime.strptime(data_inicial_geral, '%Y%m%d').strftime('%d/%m/%Y')} a {datetime.strptime(data_final_geral, '%Y%m%d').strftime('%d/%m/%Y')}",
            f"Palavra no OBJETO: {palavra_contratacao or '(vazio)'}",
            f"Termos na DESCRIÇÃO do item: {termos_itens_raw or '(vazio)'}",
            f"Quantidade total de itens que bateram com a descrição: {total_itens_filtrados_geral}",
            "\n====================================================================\n"
        ]

        try:
            with open(unificado_path, "w", encoding="utf-8") as f:
                f.write("\n".join(header_unificado))
                f.write("\n".join(unificado_sections).rstrip() + "\n")
            print(f"\n[OK] UNIFICADO salvo em: {unificado_path}")
        except Exception as e:
            print(f"\n[ERRO] Falha ao salvar UNIFICADO: {e}")

        print("\n[FINAL] Processo concluído para todos os meses.")

    except Exception as e:
        print(f"\n[FATAL] Ocorreu um erro inesperado: {e}")