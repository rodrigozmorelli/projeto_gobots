import os
import requests
import time
import pandas as pd
from math import ceil
from datetime import datetime, timedelta

# ======================================================
# 1) AUTENTICAÇÃO & CONFIG
# ======================================================
def carregar_access_token(caminho_arquivo="token.txt"):
    """
    Lê o token de um arquivo externo e retorna como string.
    """
    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        token = f.read().strip()
    return token

def extrair_user_id_de_token(access_token):
    """
    Extrai o user_id do token, assumindo que o formato do token seja:
    'APP_USR-<...>-<...>-<...>-<USER_ID>'.
    """
    return access_token.split("-")[-1]

ACCESS_TOKEN = carregar_access_token("token.txt")
USER_ID = extrair_user_id_de_token(ACCESS_TOKEN)

HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}"
}
BASE_URL = "https://api.mercadolibre.com"

# Intervalo de datas (últimos 30 dias)
DATE_FORMAT = "%Y-%m-%d"
DATE_TO = datetime.now().strftime(DATE_FORMAT)
DATE_FROM = (datetime.now() - timedelta(days=30)).strftime(DATE_FORMAT)

# Valor "padrão" de ACOS, caso não haja dados
ACOS_BENCHMARK = 10.0


# ======================================================
# 2) BUSCAR advertiser_id PARA PRODUCT ADS (PADS)
# ======================================================
def obter_advertiser_id_pads():
    """
    Busca todos os advertisers para product_id=PADS e retorna o primeiro, 
    ou None se não existir.
    """
    print("[INFO] Obtendo lista de advertisers para PADS...")
    url = f"{BASE_URL}/advertising/advertisers?product_id=PADS"
    headers_local = dict(HEADERS)
    headers_local["Content-Type"] = "application/json"
    headers_local["Api-Version"] = "1"
    
    resp = requests.get(url, headers=headers_local)
    if not resp.ok:
        print("Status code:", resp.status_code)
        print("Response text:", resp.text)
    resp.raise_for_status()
    
    data = resp.json() or {}
    advertisers = data.get("advertisers", [])
    if not advertisers:
        print("[WARN] Nenhum advertiser retornado para PADS.")
        return None
    
    adv = advertisers[0]
    advertiser_id = adv["advertiser_id"]
    
    print(f"[INFO] Advertiser ID para PADS: {advertiser_id}")
    return advertiser_id


# ======================================================
# 3) LISTAR CAMPANHAS DE PRODUCT ADS + MÉTRICAS
# ======================================================
def listar_campanhas_advertiser(advertiser_id, date_from, date_to, limit=50):
    """
    Lista campanhas (/advertisers/{advertiser_id}/product_ads/campaigns),
    incluindo métricas agregadas no período [date_from, date_to].
    """
    print("[INFO] Listando campanhas de Product Ads com métricas...")
    campaigns = []
    offset = 0
    
    metrics_fields = (
        "clicks,prints,ctr,cost,cpc,acos,"
        "units_quantity,direct_units_quantity,indirect_units_quantity,"
        "direct_items_quantity,indirect_items_quantity,"
        "organic_units_quantity,organic_items_quantity,advertising_items_quantity,"
        "cvr,roas,sov,direct_amount,indirect_amount,total_amount"
    )
    
    headers_local = dict(HEADERS)
    headers_local["Api-Version"] = "2"
    
    while True:
        url = (
            f"{BASE_URL}/advertising/advertisers/{advertiser_id}/product_ads/campaigns"
            f"?date_from={date_from}"
            f"&date_to={date_to}"
            f"&metrics={metrics_fields}"
            f"&metrics_summary=false"
            f"&limit={limit}"
            f"&offset={offset}"
        )
        resp = requests.get(url, headers=headers_local)
        
        if not resp.ok:
            print("Status code:", resp.status_code)
            print("Response text:", resp.text)
        resp.raise_for_status()
        
        data = resp.json() or {}
        results = data.get("results", [])
        if not results:
            break
        
        campaigns.extend(results)
        
        paging = data.get("paging", {})
        total = paging.get("total", 0)
        
        offset_next = offset + limit
        if offset_next >= total:
            break
        offset = offset_next
    
    print(f"[INFO] Total de {len(campaigns)} campanhas retornadas.")
    return campaigns


# ======================================================
# 4) LISTAR ITENS (ANÚNCIOS) DE PRODUCT ADS + MÉTRICAS
# ======================================================
def listar_product_ads_items(advertiser_id, date_from, date_to, limit=50):
    """
    Lista todos os anúncios (itens) de Product Ads para um advertiser,
    com métricas no período [date_from, date_to].
    """
    print("[INFO] Listando anúncios de Product Ads + métricas...")
    
    all_ads_items = []
    offset = 0
    
    metrics_fields = (
        "clicks,prints,ctr,cost,cpc,acos,"
        "units_quantity,direct_units_quantity,indirect_units_quantity,"
        "direct_items_quantity,indirect_items_quantity,"
        "organic_units_quantity,organic_items_quantity,advertising_items_quantity,"
        "cvr,roas,sov,direct_amount,indirect_amount,total_amount"
    )
    
    headers_local = dict(HEADERS)
    headers_local["Api-Version"] = "2"
    
    while True:
        url = (
            f"{BASE_URL}/advertising/advertisers/{advertiser_id}/product_ads/items"
            f"?limit={limit}"
            f"&offset={offset}"
            f"&date_from={date_from}"
            f"&date_to={date_to}"
            f"&metrics={metrics_fields}"
        )
        resp = requests.get(url, headers=headers_local)
        
        if not resp.ok:
            print("Status code:", resp.status_code)
            print("Response text:", resp.text)
        resp.raise_for_status()
        
        data = resp.json() or {}
        results = data.get("results", [])
        if not results:
            break
        
        all_ads_items.extend(results)
        
        paging = data.get("paging", {})
        total = paging.get("total", 0)
        offset_next = offset + limit
        
        if offset_next >= total:
            break
        offset = offset_next
    
    print(f"[INFO] Total de {len(all_ads_items)} anúncios retornados.")
    return all_ads_items


# ======================================================
# 5) LISTAR TODOS OS ITENS DO VENDEDOR (SCROLL PAGINATION)
# ======================================================
def listar_itens_vendedor_sem_limite(user_id, status="active", limit=50):
    """
    Lista TODOS os itens do vendedor via /users/{user_id}/items/search,
    utilizando scroll pagination (search_type=scan), para contornar o limite
    máximo de offset=999.
    """
    print(f"[INFO] Listando itens do vendedor {user_id} (scroll scan) com status={status}...")
    
    all_item_ids = []
    scroll_id = None
    
    while True:
        if scroll_id:
            url = (f"{BASE_URL}/users/{user_id}/items/search"
                   f"?status={status}"
                   f"&search_type=scan"
                   f"&scroll_id={scroll_id}"
                   f"&limit={limit}")
        else:
            # Primeira chamada (sem scroll_id)
            url = (f"{BASE_URL}/users/{user_id}/items/search"
                   f"?status={status}"
                   f"&search_type=scan"
                   f"&limit={limit}")
        
        resp = requests.get(url, headers=HEADERS)
        if not resp.ok:
            print("[WARN] Falha ao buscar itens com scan.")
            print("Status code:", resp.status_code)
            print("Response:", resp.text)
            resp.raise_for_status()
        
        data = resp.json() or {}
        
        results = data.get("results", [])
        if not results:
            # Nada mais encontrado
            print("[INFO] Nenhum resultado adicional encontrado.")
            break
        
        all_item_ids.extend(results)
        
        new_scroll_id = data.get("scroll_id", None)
        if not new_scroll_id or new_scroll_id == scroll_id:
            # Sem novo scroll_id => fim
            print("[INFO] Scroll pagination esgotada (ou sem novo scroll_id).")
            break
        
        scroll_id = new_scroll_id
        print(f"[DEBUG] +{len(results)} itens. Total parcial: {len(all_item_ids)}.")
    
    print(f"[INFO] Total final de {len(all_item_ids)} itens coletados (via scroll scan).")
    return all_item_ids


# ======================================================
# 6) MULTI-GET /items?ids=..., PARA PEGAR DETALHES EXTRAS
# ======================================================
def multiget_items_details(item_ids, chunk_size=20):
    """
    Faz MULTIGET de /items?ids=ID1,ID2,... (até 20 por chamada).
    Retorna um dicionário { item_id: {dados do item} }.
    """
    item_details_map = {}
    unique_ids = list(set(item_ids))
    total_ids = len(unique_ids)
    
    print(f"[INFO] Coletando detalhes de {total_ids} itens via MULTIGET.")
    
    n_chunks = ceil(total_ids / chunk_size)
    
    for i in range(n_chunks):
        chunk = unique_ids[i*chunk_size:(i+1)*chunk_size]
        if not chunk:
            break
        
        ids_str = ",".join(chunk)
        url = f"{BASE_URL}/items?ids={ids_str}"
        resp = requests.get(url, headers=HEADERS)
        
        if not resp.ok:
            print("Status code:", resp.status_code)
            print("Response text:", resp.text)
        resp.raise_for_status()
        
        data_list = resp.json() or []
        for obj in data_list:
            code = obj.get("code")
            body = obj.get("body", {})
            if code == 200:
                _item_id = body.get("id")
                item_details_map[_item_id] = body
            else:
                print(f"[WARN] Erro no multiget para item: {obj}")
    
    print(f"[INFO] Detalhes obtidos para {len(item_details_map)} itens.")
    return item_details_map


# ======================================================
# 7) API DE PERFORMANCE: /item/{ITEM_ID}/performance
# ======================================================
def obter_performance_item(item_id, is_user_product=False):
    """
    Retorna dicionário com score, level, level_wording e pendências,
    ou None se der erro (400, 401, 404...).
    """
    entity = "user-product" if is_user_product else "item"
    url = f"{BASE_URL}/{entity}/{item_id}/performance"
    
    resp = requests.get(url, headers=HEADERS)
    
    # Lida com possíveis erros
    if resp.status_code in [400, 401, 404]:
        print(f"[WARN] Erro ao obter performance do item {item_id}: status {resp.status_code}")
        return None
    elif not resp.ok:
        print(f"[WARN] Erro inesperado ao obter performance do item {item_id}: status {resp.status_code}")
        return None
    
    data = resp.json() or {}
    
    score = data.get("score", 0)
    level = data.get("level", "")
    level_wording = data.get("level_wording", "")
    
    pending_count = 0
    
    buckets = data.get("buckets", [])
    for b in buckets:
        b_status = b.get("status")
        if b_status == "PENDING":
            pending_count += 1
        
        variables = b.get("variables", [])
        for v in variables:
            if v.get("status") == "PENDING":
                pending_count += 1
            
            rules = v.get("rules", [])
            for r_rule in rules:
                st = r_rule.get("status")
                if st == "PENDING":
                    pending_count += 1
    
    return {
        "performance_score": score,
        "performance_level": level,
        "performance_level_wording": level_wording,
        "performance_pending_count": pending_count
    }

def obter_performance_em_lote(item_ids, is_user_product=False, delay_s=0.05):
    """
    Chama obter_performance_item para cada item_id, retornando dict {item_id: ...}.
    """
    results = {}
    for i_id in item_ids:
        perf = obter_performance_item(i_id, is_user_product=is_user_product)
        results[i_id] = perf
        time.sleep(delay_s)
    return results


# ======================================================
# 8) DEFINIÇÃO DE LIMIARES (THRESHOLDS) DINÂMICOS
# ======================================================
def define_limiares_dinamicos(df_campanhas, df_items_ads):
    """
    Calcula medianas (acos, ctr, cvr, roas, cpc) para definir thresholds.
    """
    df_camp = df_campanhas[["acos", "ctr", "cvr", "roas", "cpc"]] if not df_campanhas.empty else pd.DataFrame()
    df_it = df_items_ads[["acos", "ctr", "cvr", "roas", "cpc"]] if not df_items_ads.empty else pd.DataFrame()
    df_geral = pd.concat([df_camp, df_it], ignore_index=True)
    
    if df_geral.empty:
        return {
            "ACOS_BENCHMARK": 10.0,
            "ROI_THRESHOLD": 2.0,
            "CTR_THRESHOLD": 1.0,
            "CVR_THRESHOLD": 1.0,
            "CPC_THRESHOLD": 2.0,
            "BEST_SELLER_THRESHOLD": 80
        }
    
    def safe_median(col):
        return df_geral[col].median() if col in df_geral and not df_geral[col].empty else 0
    
    acos_median = safe_median("acos")
    roas_median = safe_median("roas")
    ctr_median  = safe_median("ctr")
    cvr_median  = safe_median("cvr")
    cpc_median  = safe_median("cpc")
    
    def safe_scale(val, factor):
        return val * factor if val > 0 else factor
    
    limiares = {
        "ACOS_BENCHMARK"       : safe_scale(acos_median, 1.2),
        "ROI_THRESHOLD"        : safe_scale(roas_median, 0.8),
        "CTR_THRESHOLD"        : safe_scale(ctr_median, 0.8),
        "CVR_THRESHOLD"        : safe_scale(cvr_median, 0.8),
        "CPC_THRESHOLD"        : safe_scale(cpc_median, 1.2),
        "BEST_SELLER_THRESHOLD": 80
    }
    return limiares


# ======================================================
# 9) GERAÇÃO DE INSIGHTS (INCLUINDO PERFORMANCE & HEALTH)
# ======================================================
def gerar_insights_campanhas(df_camp, limiares):
    """
    Exemplo de insights para campanhas (não tem 'health').
    """
    registros = []
    for _, row in df_camp.iterrows():
        r = row.to_dict()
        
        acos = row.get("acos", 0.0)
        cpc = row.get("cpc", 0.0)
        ctr = row.get("ctr", 0.0)
        roas = row.get("roas", 0.0)
        cvr = row.get("cvr", 0.0)
        clicks = row.get("clicks", 0)
        
        prioridade = 0
        recs = []
        
        if roas < limiares["ROI_THRESHOLD"]:
            recs.append(f"ROAS {roas:.2f} < {limiares['ROI_THRESHOLD']:.2f}. Ajustar lances/margens.")
            prioridade += 5
        
        if acos > limiares["ACOS_BENCHMARK"]:
            recs.append(f"ACOS {acos:.2f}% > {limiares['ACOS_BENCHMARK']:.2f}%. Reduzir CPC ou otimizar custo.")
            prioridade += 4
        
        if cpc > limiares["CPC_THRESHOLD"]:
            recs.append(f"CPC R${cpc:.2f} > R${limiares['CPC_THRESHOLD']:.2f}. Negativar KW caras.")
            prioridade += 3
        
        if ctr < limiares["CTR_THRESHOLD"]:
            recs.append(f"CTR {ctr:.2f}% < {limiares['CTR_THRESHOLD']:.2f}%. Melhorar anúncios/criativos.")
            prioridade += 2
        
        if cvr < limiares["CVR_THRESHOLD"] and clicks > 30:
            recs.append(f"CVR {cvr:.2f}% < {limiares['CVR_THRESHOLD']:.2f}%. Verificar competitividade/preço.")
            prioridade += 3
        
        camp_name = str(row.get("campaign_name", "")).lower()
        if "geral" in camp_name or "test" in camp_name:
            recs.append("Renomear campanha para algo mais específico (evitar rótulos genéricos).")
            prioridade += 1
        
        if not recs:
            recs.append("Campanha dentro das metas. Monitorar regularmente.")
        
        r["prioridade"] = prioridade
        r["acoes_e_melhorias"] = "; ".join(recs)
        registros.append(r)
    
    df_out = pd.DataFrame(registros)
    df_out.sort_values("prioridade", ascending=False, inplace=True)
    return df_out


def gerar_insights_itens_ads(df_items, limiares):
    """
    Gera insights considerando ACOS, CPC, CTR, etc.
    E agora também se health/performance_level == unhealthy/warning.
    """
    registros = []
    for _, row in df_items.iterrows():
        r = row.to_dict()
        
        acos = row.get("acos", 0.0)
        cpc = row.get("cpc", 0.0)
        ctr = row.get("ctr", 0.0)
        roas = row.get("roas", 0.0)
        cvr = row.get("cvr", 0.0)
        clicks = row.get("clicks", 0)
        units_qty = row.get("units_quantity", 0)
        
        # performance e health
        perf_score = row.get("performance_score", None)
        perf_level_wording = row.get("performance_level_wording", "")
        perf_pending = row.get("performance_pending_count", 0)
        
        # health pode ser None, 'healthy', 'warning', 'unhealthy'
        health = (row.get("health") or "").lower()
        performance_level = (row.get("performance_level") or "").lower()
        
        prioridade = 0
        recs = []
        
        # --------------------------------------------------
        # 1) Health/performance-level
        # --------------------------------------------------
        if health == "unhealthy" or performance_level == "unhealthy":
            recs.append("Anúncio está com perda de exposição (unhealthy). Atuar urgentemente!")
            prioridade += 10
        elif health == "warning" or performance_level == "warning":
            recs.append("Anúncio pode perder exposição (warning). Corrigir pendências!")
            prioridade += 5
        elif health == "healthy" or performance_level == "healthy":
            recs.append("Anúncio está saudável (healthy). Manter boas práticas.")
            prioridade += 1
        
        # --------------------------------------------------
        # 2) Regras de roas, acos, cpc, ctr, cvr...
        # --------------------------------------------------
        if roas < limiares["ROI_THRESHOLD"]:
            recs.append(f"ROAS {roas:.2f} < {limiares['ROI_THRESHOLD']:.2f}. Verificar lances/margens.")
            prioridade += 5
        
        if acos > limiares["ACOS_BENCHMARK"]:
            recs.append(f"ACOS {acos:.2f}% > {limiares['ACOS_BENCHMARK']:.2f}%. Otimizar CPC ou negativar KW.")
            prioridade += 4
        
        if cpc > limiares["CPC_THRESHOLD"]:
            recs.append(f"CPC R${cpc:.2f} > R${limiares['CPC_THRESHOLD']:.2f}. Negativar KW caras.")
            prioridade += 3
        
        if ctr < limiares["CTR_THRESHOLD"]:
            recs.append(f"CTR {ctr:.2f}% < {limiares['CTR_THRESHOLD']:.2f}%. Otimizar imagens/título.")
            prioridade += 2
        
        if cvr < limiares["CVR_THRESHOLD"] and clicks > 30:
            recs.append(f"CVR {cvr:.2f}% < {limiares['CVR_THRESHOLD']:.2f}%. Revisar ficha/preço.")
            prioridade += 3
        
        if units_qty > limiares["BEST_SELLER_THRESHOLD"]:
            recs.append("Produto campeão em Ads. Aumentar investimento/segmentação.")
            prioridade += 6
        
        # --------------------------------------------------
        # 3) Performance Score
        # --------------------------------------------------
        if perf_score is not None:
            lw = perf_level_wording.lower()
            if lw in ["básica", "basica", "basic"]:
                recs.append(f"Qualidade do anúncio '{perf_level_wording}'. Completar pendências (score={perf_score}).")
                prioridade += 5
            elif lw in ["satisfatória", "standard", "estándar"]:
                recs.append(f"Qualidade mediana '{perf_level_wording}'. Melhorar ações (score={perf_score}).")
                prioridade += 3
            elif lw in ["profissional", "profesional"]:
                recs.append(f"Qualidade '{perf_level_wording}'. Pendências: {perf_pending}.")
                prioridade += 1
        
        if not recs:
            recs.append("Item dentro das metas. Acompanhar normalmente.")
        
        r["prioridade"] = prioridade
        r["acoes_e_melhorias"] = "; ".join(recs)
        registros.append(r)
    
    df_out = pd.DataFrame(registros)
    df_out.sort_values("prioridade", ascending=False, inplace=True)
    return df_out


def gerar_insights_potenciais(df_pot, limiares):
    """
    Gera insights para itens potenciais (ainda não estão em Ads).
    Também leva em consideração health/performance_level.
    """
    if df_pot.empty:
        return df_pot
    
    registros = []
    for _, row in df_pot.iterrows():
        r = row.to_dict()
        
        sq = row.get("sold_quantity", 0) or 0
        brand = row.get("brand", "")
        price = row.get("price", 0.0)
        pictures = row.get("pictures", [])
        num_pics = len(pictures)
        
        perf_score = row.get("performance_score", None)
        perf_level_wording = row.get("performance_level_wording", "")
        perf_pending = row.get("performance_pending_count", 0)
        
        shipping_info = row.get("shipping", {})
        free_shipping = shipping_info.get("free_shipping", False)
        
        # health/performance
        health = (row.get("health") or "").lower()
        performance_level = (row.get("performance_level") or "").lower()
        
        prioridade = 0
        recs = []
        
        # --------------------------------------------------
        # 1) Health/performance-level
        # --------------------------------------------------
        if health == "unhealthy" or performance_level == "unhealthy":
            recs.append("Item perdendo exposição (unhealthy). Necessário corrigir!")
            prioridade += 10
        elif health == "warning" or performance_level == "warning":
            recs.append("Item pode perder exposição (warning). Corrigir pendências!")
            prioridade += 5
        elif health == "healthy" or performance_level == "healthy":
            recs.append("Item está saudável (healthy). Potencial positivo.")
            prioridade += 1
        
        # --------------------------------------------------
        # 2) Regras básicas (vendas, frete, fotos, preço...)
        # --------------------------------------------------
        if sq > limiares["BEST_SELLER_THRESHOLD"]:
            recs.append("Vendas orgânicas altas. Grande potencial para Ads.")
            prioridade += 10
        elif sq > limiares["BEST_SELLER_THRESHOLD"]*0.5:
            recs.append("Vendas moderadas. Ads pode escalar.")
            prioridade += 5
        else:
            recs.append("Vendas baixas. Verificar ROI antes de investir em Ads.")
            prioridade += 2
        
        if not free_shipping:
            recs.append("Considere frete grátis para melhorar conversão.")
            prioridade += 2
        
        if num_pics < 5:
            recs.append(f"Poucas fotos ({num_pics}). Adicionar imagens de qualidade.")
            prioridade += 2
        
        if brand:
            recs.append(f"Marca '{brand}'. Destacar na campanha/ficha.")
            prioridade += 1
        else:
            recs.append("Sem marca. Se genérico, avaliar diferencial.")
            prioridade += 1
        
        if price <= 0:
            recs.append("Preço não definido ou zero. Corrigir antes de Ads.")
            prioridade += 5
        elif price > 5000:
            recs.append(f"Preço elevado (R${price:.2f}). Revisar público alvo e CPC.")
            prioridade += 3
        
        # 3) Performance Score
        if perf_score is not None:
            lw = perf_level_wording.lower()
            if lw in ["básica", "basica", "basic"]:
                recs.append(f"Qualidade do anúncio '{perf_level_wording}'. Corrigir pendências (score={perf_score}).")
                prioridade += 5
            elif lw in ["satisfatória", "standard", "estándar"]:
                recs.append(f"Qualidade mediana '{perf_level_wording}'. Melhorar para maior exposição (score={perf_score}).")
                prioridade += 3
            elif lw in ["profissional", "profesional"]:
                recs.append(f"Boa qualidade '{perf_level_wording}'. Pendências: {perf_pending}.")
                prioridade += 1
        
        if not recs:
            recs.append("Item apto a Ads. Monitorar desempenho inicial.")
        
        r["prioridade"] = prioridade
        r["acoes_e_melhorias"] = "; ".join(recs)
        registros.append(r)
    
    df_out = pd.DataFrame(registros)
    df_out.sort_values("prioridade", ascending=False, inplace=True)
    return df_out


# ======================================================
# 10) FUNÇÃO PRINCIPAL PARA GERAR O RELATÓRIO COMPLETO
# ======================================================
def gerar_relatorio_completo():
    print("[INFO] Iniciando relatório completo Product Ads + Itens do vendedor + Performance...")
    
    advertiser_id = obter_advertiser_id_pads()
    if not advertiser_id:
        print("[ERRO] Não foi possível obter advertiser de PADS. Abortando.")
        return
    
    # 1) Listar campanhas
    campaigns = listar_campanhas_advertiser(advertiser_id, DATE_FROM, DATE_TO)
    
    # 2) Listar itens em ads + métricas
    ads_items = listar_product_ads_items(advertiser_id, DATE_FROM, DATE_TO)
    
    # 3) Listar TODOS os itens ativos do vendedor
    vendedor_item_ids = listar_itens_vendedor_sem_limite(USER_ID, "active", limit=50)
    
    # 4) Mapa de anúncios (itens) => ads
    ads_map = {ad["item_id"]: ad for ad in ads_items}
    # 5) Itens que não estão em ads => potenciais
    potenciais_item_ids = [i for i in vendedor_item_ids if i not in ads_map]
    
    # 6) MultiGet para TODOS os itens (ads + potenciais), p/ health etc.
    todos_ids = list(set(ads_map.keys()) | set(potenciais_item_ids))
    detalhes_todos = multiget_items_details(todos_ids)
    
    # 7) Montar DataFrame Campanhas
    campanhas_rows = []
    for c in campaigns:
        c_metrics = c.get("metrics", {})
        campanhas_rows.append({
            "campaign_id" : c.get("id"),
            "campaign_name": c.get("name"),
            "status"      : c.get("status"),
            "budget"      : c.get("budget"),
            "currency_id" : c.get("currency_id"),
            "strategy"    : c.get("strategy"),
            "acos_target" : c.get("acos_target"),
            "channel"     : c.get("channel"),
            "prints"      : c_metrics.get("prints", 0),
            "clicks"      : c_metrics.get("clicks", 0),
            "ctr"         : c_metrics.get("ctr", 0.0),
            "cost"        : c_metrics.get("cost", 0.0),
            "cpc"         : c_metrics.get("cpc", 0.0),
            "acos"        : c_metrics.get("acos", 0.0),
            "units_quantity"       : c_metrics.get("units_quantity", 0),
            "direct_units_quantity": c_metrics.get("direct_units_quantity", 0),
            "indirect_units_quantity": c_metrics.get("indirect_units_quantity", 0),
            "cvr"         : c_metrics.get("cvr", 0.0),
            "roas"        : c_metrics.get("roas", 0.0),
            "sov"         : c_metrics.get("sov", 0.0),
            "direct_amount"   : c_metrics.get("direct_amount", 0.0),
            "indirect_amount" : c_metrics.get("indirect_amount", 0.0),
            "total_amount"    : c_metrics.get("total_amount", 0.0),
            "organic_units_quantity"     : c_metrics.get("organic_units_quantity", 0),
            "organic_items_quantity"     : c_metrics.get("organic_items_quantity", 0),
            "direct_items_quantity"      : c_metrics.get("direct_items_quantity", 0),
            "indirect_items_quantity"    : c_metrics.get("indirect_items_quantity", 0),
            "advertising_items_quantity" : c_metrics.get("advertising_items_quantity", 0),
            "acos_benchmark" : ACOS_BENCHMARK,
        })
    df_campanhas = pd.DataFrame(campanhas_rows)
    
    # 8) Montar DataFrame ItensEmAds
    items_ads_rows = []
    ads_item_ids = []
    for ad in ads_items:
        metrics = ad.get("metrics", {})
        i_id = ad.get("item_id")
        ads_item_ids.append(i_id)
        
        items_ads_rows.append({
            "item_id": i_id,
            "campaign_id": ad.get("campaign_id"),
            "title": ad.get("title"),
            "status_ads": ad.get("status"),
            "channel": ad.get("channel"),
            "date_created": ad.get("date_created"),
            "listing_type_id": ad.get("listing_type_id"),
            "buy_box_winner": ad.get("buy_box_winner"),
            "prints" : metrics.get("prints", 0),
            "clicks" : metrics.get("clicks", 0),
            "ctr"    : metrics.get("ctr", 0.0),
            "cost"   : metrics.get("cost", 0.0),
            "cpc"    : metrics.get("cpc", 0.0),
            "acos"   : metrics.get("acos", 0.0),
            "cvr"    : metrics.get("cvr", 0.0),
            "roas"   : metrics.get("roas", 0.0),
            "sov"    : metrics.get("sov", 0.0),
            "units_quantity"       : metrics.get("units_quantity", 0),
            "direct_units_quantity": metrics.get("direct_units_quantity", 0),
            "indirect_units_quantity": metrics.get("indirect_units_quantity", 0),
            "organic_units_quantity": metrics.get("organic_units_quantity", 0),
            "organic_items_quantity": metrics.get("organic_items_quantity", 0),
            "direct_items_quantity"   : metrics.get("direct_items_quantity", 0),
            "indirect_items_quantity" : metrics.get("indirect_items_quantity", 0),
            "advertising_items_quantity": metrics.get("advertising_items_quantity", 0),
            "direct_amount"   : metrics.get("direct_amount", 0.0),
            "indirect_amount" : metrics.get("indirect_amount", 0.0),
            "total_amount"    : metrics.get("total_amount", 0.0),
            "acos_benchmark"  : ACOS_BENCHMARK,
        })
    df_items_ads = pd.DataFrame(items_ads_rows)
    
    # 9) Montar DataFrame de ItensPotenciais (não estão em Ads)
    potenciais_rows = []
    for item_id in potenciais_item_ids:
        body = detalhes_todos.get(item_id, {})
        shipping_info = body.get("shipping", {})
        
        # Tentar extrair a marca
        item_brand = ""
        attributes_list = body.get("attributes", [])
        for attr in attributes_list:
            if attr.get("id") == "BRAND":
                item_brand = attr.get("value_name", "")
                break
        
        potenciais_rows.append({
            "item_id": item_id,
            "title": body.get("title"),
            "sold_quantity": body.get("sold_quantity"),
            "price": body.get("price"),
            "date_created": body.get("date_created"),
            "brand": item_brand,
            "category_id": body.get("category_id"),
            "domain_id": body.get("domain_id"),
            "shipping": shipping_info,
            "pictures": body.get("pictures", []),
            "listing_type_id": body.get("listing_type_id", ""),
            "health": body.get("health", None),
        })
    df_potenciais = pd.DataFrame(potenciais_rows)
    
    # 10) Obter Performance
    todos_ids_performance = list(set(ads_item_ids + potenciais_item_ids))
    
    print(f"[INFO] Coletando performance de {len(todos_ids_performance)} itens via /item/ID/performance...")
    perf_map = obter_performance_em_lote(todos_ids_performance, is_user_product=False, delay_s=0.05)
    
    # 10.1) Fallback: se performance == None e tivermos 'health', gerar performance sintética
    for item_id in todos_ids_performance:
        if perf_map[item_id] is None:
            body = detalhes_todos.get(item_id, {})
            h = body.get("health")
            if h is not None:
                if h == "healthy":
                    perf_map[item_id] = {
                        "performance_score": 80,
                        "performance_level": "HEALTHY",
                        "performance_level_wording": "Item em bom estado de exposição",
                        "performance_pending_count": 0,
                    }
                elif h == "warning":
                    perf_map[item_id] = {
                        "performance_score": 40,
                        "performance_level": "WARNING",
                        "performance_level_wording": "Item pode perder exposição",
                        "performance_pending_count": 1,
                    }
                elif h == "unhealthy":
                    perf_map[item_id] = {
                        "performance_score": 20,
                        "performance_level": "UNHEALTHY",
                        "performance_level_wording": "Item está perdendo exposição",
                        "performance_pending_count": 2,
                    }
                else:
                    # valor health desconhecido
                    perf_map[item_id] = {
                        "performance_score": 0,
                        "performance_level": f"({h})",
                        "performance_level_wording": "Health não mapeado",
                        "performance_pending_count": 0,
                    }
            else:
                perf_map[item_id] = {
                    "performance_score": None,
                    "performance_level": "",
                    "performance_level_wording": "Sem Performance e Sem Health",
                    "performance_pending_count": 0,
                }
    
    # 11) Enriquecer DF ItensEmAds com performance/health
    if not df_items_ads.empty:
        df_items_ads["performance_score"] = df_items_ads["item_id"].apply(
            lambda i: perf_map[i].get("performance_score", None)
        )
        df_items_ads["performance_level"] = df_items_ads["item_id"].apply(
            lambda i: perf_map[i].get("performance_level", "")
        )
        df_items_ads["performance_level_wording"] = df_items_ads["item_id"].apply(
            lambda i: perf_map[i].get("performance_level_wording", "")
        )
        df_items_ads["performance_pending_count"] = df_items_ads["item_id"].apply(
            lambda i: perf_map[i].get("performance_pending_count", 0)
        )
        df_items_ads["health"] = df_items_ads["item_id"].apply(
            lambda i: detalhes_todos.get(i, {}).get("health", None)
        )
    
    # 12) Enriquecer DF Potenciais com performance
    if not df_potenciais.empty:
        df_potenciais["performance_score"] = df_potenciais["item_id"].apply(
            lambda i: perf_map[i].get("performance_score", None)
        )
        df_potenciais["performance_level"] = df_potenciais["item_id"].apply(
            lambda i: perf_map[i].get("performance_level", "")
        )
        df_potenciais["performance_level_wording"] = df_potenciais["item_id"].apply(
            lambda i: perf_map[i].get("performance_level_wording", "")
        )
        df_potenciais["performance_pending_count"] = df_potenciais["item_id"].apply(
            lambda i: perf_map[i].get("performance_pending_count", 0)
        )
    
    # 13) Definir limites dinamicamente
    limiares = define_limiares_dinamicos(df_campanhas, df_items_ads)
    print("[INFO] Limiar(es) calculado(s) dinamicamente:", limiares)
    
    # 14) Gerar Insights
    df_camp_insights = gerar_insights_campanhas(df_campanhas, limiares)
    df_items_insights = gerar_insights_itens_ads(df_items_ads, limiares)
    df_pot_insights = gerar_insights_potenciais(df_potenciais, limiares)
    
    # 15) Salvar Excel
    data_hoje = datetime.now().strftime("%Y-%m-%d")
    nome_arquivo = f"product_ads_relatorio_{USER_ID}_{data_hoje}.xlsx"
    
    with pd.ExcelWriter(nome_arquivo) as writer:
        df_camp_insights.to_excel(writer, sheet_name="Campanhas", index=False)
        df_items_insights.to_excel(writer, sheet_name="ItensEmAds", index=False)
        df_pot_insights.to_excel(writer, sheet_name="ItensPotenciais", index=False)
    
    print(f"[INFO] Relatório gerado com sucesso: {nome_arquivo}")


# ======================================================
# 11) EXECUÇÃO
# ======================================================
if __name__ == "__main__":
    gerar_relatorio_completo()