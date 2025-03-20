import asyncio
from collections import defaultdict
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
    item_sales = defaultdict(int)
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
                item_sales[item_id] += 1

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
    
    items_with_sales = [{"item_id": item_id, "sales": sales} for item_id, sales in item_sales.items()]
    return items_with_sales

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

async def get_batch_item_details(session, item_ids, access_token):
    url = 'https://api.mercadolibre.com/items'
    params = {'ids': ','.join(item_ids)}
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(url, headers=headers, params=params) as response:
        if response.status != 200:
            return {}
        data = await response.json()
        details = {}
        for item in data:
            if item.get('code') == 200:
                item_data = item.get('body', {})
                item_id = item_data.get('id')
                stock = None
                if "available_quantity" in item_data:
                    stock = item_data["available_quantity"]
                elif "initial_quantity" in item_data:
                    stock = item_data["initial_quantity"]
                elif item_data.get("variations"):
                    stock = sum(var.get("available_quantity", 0) for var in item_data["variations"])
                details[item_id] = {
                    'title': item_data.get('title'),
                    'price': item_data.get('price'),
                    'permalink': item_data.get('permalink'),
                    'image_url': item_data["pictures"][0]["secure_url"] if item_data.get("pictures") else None,
                    'stock': stock,
                }
        return details

# Função para obter score de qualidade do item
async def get_item_quality_score(session, item_id, access_token):
    url = f'https://api.mercadolibre.com/item/{item_id}/performance'
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('score')
        return None

#Obter posicionamento do item
async def get_item_position(session, item_id, access_token):
    url = f"https://api.mercadolibre.com/highlights/MLB/item/{item_id}"
    headers = {'Authorization': f'Bearer {access_token}'}
    async with session.get(url, headers=headers) as response:
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

async def process_item(session, item_id, date_from, date_to, access_token, store_info, sales, details):
    visits, quality_score, position = await asyncio.gather(
        get_item_visits(session, item_id, date_from, date_to, access_token),
        get_item_quality_score(session, item_id, access_token),
        get_item_position(session, item_id, access_token),
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
            'stock': details['stock'],
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

    item_ids = [item["item_id"] for item in items]
    details_dict = {}
    max_batch_size = 20
    for i in range(0, len(item_ids), max_batch_size):
        batch_ids = item_ids[i:i+max_batch_size]
        details_dict.update(await get_batch_item_details(session, batch_ids, access_token))

    tasks = [
        process_item(session, item["item_id"], date_from, date_to, 
                     access_token, store_info, item["sales"], 
                     details_dict.get(item["item_id"]))
        for item in items
    ]
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
        df['quality_score'] = df['quality_score'].astype('Int64')
        df['position'] = df['position'].astype('Int64')
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
