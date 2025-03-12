import asyncio
from datetime import datetime, timedelta
import os

import aiohttp
import pandas as pd


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

async def get_go_bots_api_response(session):
    url = 'https://askhere.gobots.com.br/ml/all'
    access_token = load_access_token('gobots_token.txt')
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            return await response.json()
        return None

def get_access_token_from_gobots_api(user_id, data):
    for item in data:
        if item.get('user_id') == user_id:
            return item.get('access_token')
    return None
    

# ======================================================
# 2) OBTER VISITAS, VENDAS E PREÇO POR PRODUTO
# ======================================================

# Função para obter as os itens de um vendedor através da API de orders
async def get_all_items_with_sales(session, date_from, date_to, user_id, access_token):
    url = 'https://api.mercadolibre.com/orders/search'
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {
        'seller': user_id,
        'order.status': 'paid',
        'order.date_created.from': date_from,
        'order.date_created.to': date_to,
        'limit': 50,

    }
    all_items = []        
    offset = 0

    while True:
        params['offset'] = offset
        async with session.get(url, headers=headers, params=params) as response:
            if response.status != 200:
                print(f"Erro na requisição: {response.status}, user id: {user_id}")
                break
            
            data = await response.json()
            results = data.get('results', [])
            for order in results:
                item_id = order["order_items"][0]["item"]["id"]
                if item_id not in all_items:
                    all_items.append(item_id)

            paging = data.get('paging', {})
            # print(paging)
            total_items = paging.get('total', 0)

            # Se veio menos itens que o limite ou já atingimos o total, interrompe.
            if total_items < params['limit'] or (offset + params['limit']) >= total_items:
                # print("[INFO] Fim da paginação ou todos os itens já listados.")
                break

            # Evite offset acima de 1000 (muitas vezes a API não permite)
            # if (offset + params['limit']) >= 1000:
            #     print("[WARN] Offset limit reached 1000, stopping to avoid error.")
            #     break
            offset += params['limit']
    
    return all_items

# Função para obter as visitas de um item
async def get_item_visits(session, item_id, date_from, date_to, access_token):
    url = f'https://api.mercadolibre.com/items/{item_id}/visits'
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'date_from': date_from, 'date_to': date_to}
    async with session.get(url, headers=headers, params=params) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('total_visits')
        return None

# Função para obter as vendas de um item
async def get_item_sales(session, item_id, date_from, date_to, user_id, access_token):
    url = 'https://api.mercadolibre.com/orders/search'
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {
        'seller': user_id,
        'item': item_id,
        'order.status': 'paid',
        'order.date_created.from': date_from,
        'order.date_created.to': date_to
    }
    async with session.get(url, headers=headers, params=params) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('paging', {}).get('total', 0)
        return None

# Função para obter detalhes do item, incluindo o preço
async def get_item_details(session, item_id, access_token):
    url = f'https://api.mercadolibre.com/items/{item_id}'
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            return {
                'title': data.get('title'),
                'price': data.get('price'),
                'permalink': data.get('permalink'),
                'image_url': data["pictures"][0]["secure_url"] if data.get("pictures") else None
            }
        return None

# Função para obter score de qualidade do item
async def get_item_quality_score(session, item_id, access_token):
    url = f'https://api.mercadolibre.com/items/{item_id}/performance'
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('score')
        return None
    
#Função para obter estoque de um item
async def get_item_stock(session, item_id, access_token):
    url = f'https://api.mercadolibre.com/items/{item_id}'
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            if "available_quantity" in data:
                return data["available_quantity"]
            elif "initial_quantity" in data:
                return data["initial_quantity"]
            elif data.get("variations"):
                # Se houver variações, soma o estoque de todas
                return sum(var.get("available_quantity", 0) for var in data["variations"])
        return None
    
    
#Obter posicionamento do item
async def get_item_position(session, item_id):
    url = f"https://api.mercadolibre.com/highlights/MLB/item/{item_id}"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('position')
        return None

#Obter informações da loja
async def get_store_info(session, user_id, access_token):
    url = f'https://api.mercadolibre.com/users/{user_id}'
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            return {
                'store_name': data.get('nickname'),
                'store_permalink': data.get('permalink')
            }
        return None

async def process_item(session, item_id, date_from, date_to, user_id, access_token, store_info):
    sales, visits, details, quality_score, stock, position = await asyncio.gather(
        get_item_sales(session, item_id, date_from, date_to, user_id, access_token),
        get_item_visits(session, item_id, date_from, date_to, access_token),
        get_item_details(session, item_id, access_token),
        get_item_quality_score(session, item_id, access_token),
        get_item_stock(session, item_id, access_token),
        get_item_position(session, item_id),
    )
    
    if sales and sales > 0 and details:
        return {
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
        }
    return None

async def build_output(session, user_id, access_token, days_window):
    # Definir período (último mês)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_window)
    date_from = start_date.strftime('%Y-%m-%dT%H:%M:%S.000-00:00')
    date_to = end_date.strftime('%Y-%m-%dT%H:%M:%S.000-00:00')

    items = await get_all_items_with_sales(session, date_from, date_to, user_id, access_token)
    store_info = await get_store_info(session, user_id, access_token)
    
    if not items or not store_info:
        return pd.DataFrame()

    tasks = [process_item(session, item_id, date_from, date_to, user_id, access_token, store_info) for item_id in items]
    results = await asyncio.gather(*tasks)
    valid_results = [res for res in results if res is not None]
    
    return pd.DataFrame(valid_results)

# ======================================================
# 3) CALCULAR AS MÉTRICAS E SALVAR O DATAFRAME EM CSV
# ======================================================

#Calcular as métricas por produto

def calculate_metrics(df):
    df['conversion'] = df['sales']/df['visits']
    df['sales_potential'] = df['conversion'] * df['price']
    df = df.sort_values(by='sales_potential', ascending=False)

    return df

async def process_user(session, user_id, go_bots_data):
    access_token = get_access_token_from_gobots_api(user_id, go_bots_data)
    if not access_token:
        print(f"No access token for user {user_id}")
        return

    df = await build_output(session, user_id, access_token, 30)
    if df.shape[0] > 0:
        df = calculate_metrics(df)
        store_name = df['store_name'].iloc[0]
        df.to_csv(f'output_tables/{store_name}_{user_id}.csv', index=False)
        print(f"Processed user {user_id}")
    else:
        print(f"No data for user {user_id}")

async def main():
    os.makedirs('output_tables', exist_ok=True)

    with open('user_ids.txt', 'r') as f:
        user_ids = [int(uid.strip()) for uid in f.read().split(',')]

    async with aiohttp.ClientSession() as session:
        go_bots_data = await get_go_bots_api_response(session)
        if not go_bots_data:
            print("Failed to fetch GoBots data")
            return
        
        tasks = [process_user(session, uid, go_bots_data) for uid in user_ids]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
