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
HEADERS = {"Accept": "*/*", "User-Agent": "pncp-script-py/1.3"}
PAUSA_ENTRE_MESES_SEGUNDOS = 30 # Pausa de 2 minutos

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

def gerar_intervalos_mensais(data_inicial_str: str, data_final_str: str) -> list[tuple[str, str]]:
    """Gera uma lista de tuplas (data_inicio, data_fim) para cada mês no intervalo."""
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
        
        # Avança para o próximo mês
        next_month_start = (month_end + timedelta(days=1))
        current_date = next_month_start
        
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

# ============================== Geração de Relatórios ==============================
def salvar_relatorios(filename_base: str, dados_json: dict, dados_txt: dict):
    """Salva os resultados em arquivos .json (todos os itens) e .txt (itens filtrados)."""
    
    # --- Salvar em JSON (TODOS os itens das contratações) ---
    if not dados_json:
        print("\n[INFO] Nenhum item bruto encontrado para gerar o arquivo JSON.")
    else:
        json_filename = f"{filename_base}.json"
        try:
            with open(json_filename, 'w', encoding='utf-8') as f:
                json.dump(dados_json, f, ensure_ascii=False, indent=4)
            print(f"\n[OK] Relatório JSON com TODOS os itens salvo em: {json_filename}")
        except Exception as e:
            print(f"\n[ERRO] Falha ao salvar relatório JSON: {e}")

    # --- Salvar em TXT (APENAS itens que bateram com a descrição) ---
    if not dados_txt:
        print("[INFO] Nenhum item correspondeu à descrição para gerar o relatório TXT.")
    else:
        txt_filename = f"{filename_base}.txt"
        try:
            with open(txt_filename, 'w', encoding='utf-8') as f:
                f.write("RELATÓRIO DE ITENS FILTRADOS NO PNCP\n")
                f.write(f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")
                
                for id_pncp, data in dados_txt.items():
                    f.write("\n============================================================\n")
                    f.write(f"CONTRATAÇÃO ID: {id_pncp}\n")
                    f.write(f"Objeto: {data.get('objeto', 'N/A')}\n")
                    f.write(f"Link: {data.get('link', 'N/A')}\n")
                    f.write("------------------------------------------------------------\n")
                    f.write("Itens Encontrados (filtrados por descrição):\n\n")

                    for item in data.get('itens_filtrados', []):
                        f.write(f"  - Item {item.get('Numero', '?')}:\n")
                        f.write(f"    Descrição: {item.get('Descricao', 'N/A')}\n")
                        f.write(f"    Quantidade: {item.get('Quantidade', 'N/A')}\n")
                        f.write(f"    Valor Unitário Estimado: {item.get('Valor unitario estimado', 'N/A')}\n")
                        f.write(f"    Valor Total Estimado: {item.get('Valor total estimado', 'N/A')}\n\n")
            
            print(f"[OK] Relatório TXT com itens FILTRADOS salvo em: {txt_filename}")
        except Exception as e:
            print(f"[ERRO] Falha ao salvar relatório TXT: {e}")

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

        intervalos_mensais = gerar_intervalos_mensais(data_inicial_geral, data_final_geral)
        total_meses = len(intervalos_mensais)
        print(f"\n[INFO] O período foi dividido em {total_meses} busca(s) mensal(is).")
        
        # Alteração: Laço principal agora executa o processo completo por mês.
        for i, (data_inicial_mes, data_final_mes) in enumerate(intervalos_mensais):
            print(f"\n{'='*20} BUSCANDO MÊS {i+1}/{total_meses}: {data_inicial_mes} a {data_final_mes} {'='*20}")

            # 1) Coletar contratações do MÊS ATUAL
            contratacoes_mes = fetch_contratacoes_multi_modalidade(session, data_inicial_mes, data_final_mes, modalidades, modo)
            print(f"\n[INFO] Mês {i+1}: {len(contratacoes_mes)} contratações coletadas.")

            # 2) Filtrar por palavra no objeto
            filtradas_mes = [c for c in contratacoes_mes if palavra_contratacao in (c.get("objetoCompra") or "").lower()]
            print(f"[INFO] Mês {i+1}: {len(filtradas_mes)} contratações após filtro no objeto.")

            # 3) Coletar IDs únicos DO MÊS e mapear para objeto e link
            id_to_info_mes = {}
            ids_do_mes = []
            for c in filtradas_mes:
                id_pncp, link = None, None
                numero_controle = c.get("numeroControlePncp")
                if numero_controle: id_pncp, link = format_id_pncp_from_numero_controle(numero_controle)
                if not id_pncp:
                    try:
                        cnpj, ano, seq = c["orgaoEntidade"]["cnpj"], c["anoCompra"], c["sequencialCompra"]
                        id_pncp, link = f"{cnpj}/{ano}/{seq}", f"https://pncp.gov.br/app/editais/{cnpj}/{ano}/{seq}"
                    except (KeyError, TypeError): continue
                
                if id_pncp and id_pncp not in id_to_info_mes:
                    id_to_info_mes[id_pncp] = {"objeto": (c.get("objetoCompra") or "").strip(), "link": link}
                    ids_do_mes.append(id_pncp)
            
            print(f"[INFO] Mês {i+1}: {len(ids_do_mes)} contratações únicas encontradas para buscar itens.")

            # 4) Buscar itens das contratações DO MÊS
            itens_brutos_mes = fetch_itens_para_ids(session, [(i, i) for i in ids_do_mes])

            # 5a) Preparar dados para o JSON DO MÊS
            dados_para_json_mes = {}
            for item in itens_brutos_mes:
                id_pncp = item['id_pncp']
                if id_pncp not in dados_para_json_mes:
                    dados_para_json_mes[id_pncp] = id_to_info_mes.get(id_pncp, {})
                    dados_para_json_mes[id_pncp]['todos_os_itens'] = []
                dados_para_json_mes[id_pncp]['todos_os_itens'].append(item)

            # 5b) Preparar dados para o TXT DO MÊS
            itens_filtrados_mes = [item for item in itens_brutos_mes if not termos_re or termos_re.search(item.get("Descricao", ""))]
            dados_para_txt_mes = {}
            for item in itens_filtrados_mes:
                id_pncp = item['id_pncp']
                if id_pncp not in dados_para_txt_mes:
                    dados_para_txt_mes[id_pncp] = id_to_info_mes.get(id_pncp, {})
                    dados_para_txt_mes[id_pncp]['itens_filtrados'] = []
                dados_para_txt_mes[id_pncp]['itens_filtrados'].append(item)

            # 6) Saída: Gerar arquivos de relatório PARA O MÊS ATUAL
            if dados_para_json_mes or dados_para_txt_mes:
                mes_ano_str = datetime.strptime(data_inicial_mes, "%Y%m%d").strftime("%Y-%m")
                nome_base_relatorio = f"relatorio_pncp_{mes_ano_str}"
                salvar_relatorios(nome_base_relatorio, dados_para_json_mes, dados_para_txt_mes)
            else:
                print(f"\n[INFO] Nenhum item encontrado no mês {i+1} para gerar relatórios.")

            # 7) Imprimir resumo DO MÊS
            total_itens_filtrados_mes = len(itens_filtrados_mes)
            print(f"\n[RESUMO DO MÊS {i+1}] Contratações com itens filtrados: {len(dados_para_txt_mes)}")
            print(f"[RESUMO DO MÊS {i+1}] Itens individuais filtrados: {total_itens_filtrados_mes}")

            # 8) Pausa antes do próximo mês
            if i < total_meses - 1:
                print(f"\n[PAUSA] Fim do processamento do mês {i+1}. Pausando por {PAUSA_ENTRE_MESES_SEGUNDOS} segundos...")
                time.sleep(PAUSA_ENTRE_MESES_SEGUNDOS)

        print("\n[FINAL] Processo concluído para todos os meses.")

    except Exception as e:
        print(f"\n[FATAL] Ocorreu um erro inesperado: {e}")