import requests
import pandas as pd
from datetime import datetime, timedelta
import json

# ======================================================
# 1) AUTENTICAÇÃO & CONFIG
# ======================================================
def load_access_token(caminho_arquivo="token.txt"):
    """
    Lê o token de um arquivo externo e retorna como string.
    """
    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        token = f.read().strip()
    return token

def extract_user_id_from_token(access_token):
    """
    Extrai o user_id do token, assumindo que o formato do token seja:
    'APP_USR-<...>-<...>-<...>-<USER_ID>'.
    """
    return access_token.split("-")[-1]

def get_go_bots_api_response():
    url = 'https://askhere.gobots.com.br/ml/all'
    access_token = load_access_token('gobots_token.txt')
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data

def get_access_token_from_gobots_api(user_id, data):
    for item in data:
        if item.get('user_id') == user_id:
            return item.get('access_token')
    return None
    

# ======================================================
# 2) OBTER VISITAS, VENDAS E PREÇO POR PRODUTO
# ======================================================

#Função para obter os itens do vendedor
def get_all_active_items():
    """
    Retorna a lista de todos os ITEM_IDs do vendedor,
    lidando com a paginação sem ultrapassar o total de itens
    e evitando erros com offset maior que 1000.
    """
    url_base = f"{BASE_URL}/users/{USER_ID}/items/search"
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    offset = 0
    limit = 50
    todos_os_itens = []

    while True:
        params = {"offset": offset, "limit": limit, "status":"active"}
        resp = requests.get(url_base, headers=headers, params=params)
        resp.raise_for_status()
        
        data = resp.json()
        item_ids = data.get("results", [])
        todos_os_itens.extend(item_ids)

        paging = data.get("paging", {})
        total_itens = paging.get("total", 0)


        # Se veio menos itens que o limite ou já atingimos o total, interrompe.
        if len(item_ids) < limit or (offset + limit) >= total_itens:
            print("[INFO] Fim da paginação ou todos os itens já listados.")
            break

        # Evite offset acima de 1000 (muitas vezes a API não permite)
        if offset + limit >= 1000:
            print("[WARN] Offset atingiu 1000. Interrompendo para evitar erro 400.")
            break

        offset += limit

    return todos_os_itens

# Função para obter as vendas de um item
def get_all_items_with_sales(date_from, date_to):
    url = f'https://api.mercadolibre.com/orders/search?seller={USER_ID}&order.status=paid&order.date_created.from={date_from}&order.date_created.to={date_to}'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    offset = 0
    limit = 50
    all_items = []
    
    while True:
        params = {"offset": offset, "limit": limit}
        response = requests.get(url, headers=headers, params=params)   
        if response.status_code != 200:
            print(f"Erro na requisição: {response.status_code}")
            break
        
        data = response.json()
        results = data['results']
        for order in results:
            item_id = order["order_items"][0]["item"]["id"]
            if item_id not in all_items:
                all_items.append(item_id)
           

        paging = data.get("paging", {})
        print(paging)
        total_items = paging.get("total", 0)

        if total_items < limit or (offset + limit) >= total_items:
            print("[INFO] Fim da paginação ou todos os itens já listados.")
            break

        # Evite offset acima de 1000 (muitas vezes a API não permite)
        if offset + limit >= 1000:
            print("[WARN] Offset atingiu 1000. Interrompendo para evitar erro 400.")
            break

        offset += limit
    
    return all_items

# Função para obter as visitas de um item
def get_item_visits(item_id, date_from, date_to):
    url = f'https://api.mercadolibre.com/items/{item_id}/visits?date_from={date_from}&date_to={date_to}'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()['total_visits']
    else:
        return None

# Função para obter as vendas de um item
def get_item_sales(item_id, date_from, date_to):
    url = f'https://api.mercadolibre.com/orders/search?seller={USER_ID}&item={item_id}&order.date_created.from={date_from}&order.date_created.to={date_to}'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        total_sales = response.json()['paging']['total']
        return total_sales
    else:
        return None

# Função para obter detalhes do item, incluindo o preço
def get_item_details(item_id):
    url = f'https://api.mercadolibre.com/items/{item_id}'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return {
            'title': data['title'],
            'price': data['price'],
            'permalink' : data['permalink'],
            'image_url' : data["pictures"][0]["secure_url"] if data["pictures"] and len(data["pictures"]) > 0 else None
        }
    else:
        return None

# Função para obter score de qualidade do item
def get_item_quality_score(item_id):
    url = f'https://api.mercadolibre.com/item/{item_id}/performance'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        return data.get('score')
    else:
        return None
    
#Função para obter estoque de um item
def get_item_stock(item_id):
    url = f'https://api.mercadolibre.com/items/{item_id}'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}

    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        if "available_quantity" in data:
            stock = data["available_quantity"]
        elif "initial_quantity" in data:
            stock = data["initial_quantity"]
        elif "variations" in data and data["variations"]:
            # Se houver variações, soma o estoque de todas
            stock = sum(var.get("available_quantity", 0) for var in data["variations"])
        return stock
    else:
        return None
    
    
#Obter posicionamento do item
def get_item_position(item_id):
    url = f"https://api.mercadolibre.com/highlights/MLB/item/{item_id}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        return data['position']
    else:
        return None

#Obter informações da loja
def get_store_info(user_id):
    url = f'https://api.mercadolibre.com/users/{user_id}'
    headers = {'Authorization': f'Bearer {ACCESS_TOKEN}'}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return {
            'store_name' : data.get('nickname'),
            'store_permalink' : data.get('permalink')
        }
    else:
        return None


# Função principal
def build_output():
    # Definir período (último mês)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_WINDOW) 
    date_from = start_date.strftime('%Y-%m-%dT%H:%M:%S.000-00:00')
    date_to = end_date.strftime('%Y-%m-%dT%H:%M:%S.000-00:00')

    # Obter lista de todos os itens ativos do vendedor
    items = get_all_items_with_sales(date_from, date_to)
    # items = group_orders(items)
    print(items)
    store_info = get_store_info(USER_ID)
    
    if items:
        results = []  # Lista para armazenar os resultados
        
        for item in items:
            item_id = item
            sales = get_item_sales(item_id, date_from, date_to)
            if sales > 0:                
                visits = get_item_visits(item_id, date_from, date_to)            
                details = get_item_details(item_id)
                quality_score = get_item_quality_score(item_id)
                stock = get_item_stock(item_id)
                position = get_item_position(item_id)
                
                if details:
                    results.append({
                        'store_name': store_info['store_name'],
                        'store_permalink': store_info['store_permalink'],
                        'item_id': item_id,
                        'title': details['title'],
                        'price': details['price'],
                        'permalink': details['permalink'],
                        'visits': visits,
                        'sales': sales,
                        'quality_score': quality_score,
                        'stock': stock,
                        'image_url': details['image_url'],
                        'position': position
                    })
        
        # Criar um DataFrame a partir dos resultados
        df = pd.DataFrame(results)
        return df
    else:
        print('Erro ao obter lista de itens do vendedor')
        return None

# ======================================================
# 3) CALCULAR AS MÉTRICAS E SALVAR O DATAFRAME EM CSV
# ======================================================

#Calcular as métricas por produto

def calculate_metrics(df):
    df['conversion'] = df['sales']/df['visits']
    df['sales_potential'] = df['conversion'] * df['price']
    df = df.sort_values(by='sales_potential', ascending=False)

    return df

# Executar a função principal e imprimir o resultado
with open('user_ids.txt', 'r') as file:
    user_ids = file.read().split(',')


user_ids = [user_id.strip() for user_id in user_ids]
user_ids = [int(x) for x in user_ids]

go_bots_api_response = get_go_bots_api_response()

# with open("go_bots_api_output.txt", "w") as file:
#     file.write(repr(go_bots_api_response))

# Execute the function for each user_id
for user_id in user_ids:
    ACCESS_TOKEN = get_access_token_from_gobots_api(user_id, go_bots_api_response)
    print(ACCESS_TOKEN)
    USER_ID = user_id

    HEADERS = {
        "Authorization": f"Bearer {ACCESS_TOKEN}"
    }
    BASE_URL = "https://api.mercadolibre.com"

    DAYS_WINDOW = 30

    df_dados = build_output()

    if df_dados.shape[0] > 0:
        df_resultados = calculate_metrics(df_dados)
        nome_arquivo = df_resultados['store_name'].iloc[0] + '_' + str(USER_ID) + '.csv'
        df_resultados.to_csv('output_tables\\' + nome_arquivo, encoding='utf-8')
        print('Dados salvos com sucesso.')
    else:
        print("Não foi possível obter os dados.")