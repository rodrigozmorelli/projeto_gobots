import asyncio
import io
import os
import subprocess

import aiofiles
import pandas as pd
from jinja2 import Environment, FileSystemLoader
from playwright.async_api import async_playwright


async def read_input(input_file):
    async with aiofiles.open(input_file, mode='r', encoding="utf-8") as f:
        content = await f.read()
        df = pd.read_csv(io.StringIO(content))

    df = filter_input(df)

    #Select the desired columns and format them
    df['conversion'] = (df['conversion']).astype(float).map('{:.1%}'.format)

    
    # Format the column as Brazilian Reais
    df['price'] = df['price'].apply(lambda x: f"R${x:,.2f}")
    df['sales_potential'] = df['sales_potential'].apply(lambda x: f"R${x:,.2f}")

    #Format stock column
    df['stock'] = df['stock'].fillna(0).astype(int)    

    #Format position and quality_score columns
    df['position'] = df['position'].fillna('-')
    df['quality_score'] = df['quality_score'].fillna('-')

    return df

def filter_input(df):
    # Calculate the cumulative sales
    df['cumulative_sales']  = df['sales']/df['sales'].sum()

    #Split the products into 2 groups
    df['product_group'] = -1
    df.loc[df['sales'] >= 1, 'product_group'] = 1

    #First, try to recommend only products that represent more than 10% of the sales
    if df[df['cumulative_sales'] >= 0.1].shape[0] > 0:
        df.loc[df['cumulative_sales'] >= 0.1, 'product_group'] = 2
    #If not, recommend products that represent more than 5% of the sales
    elif df[df['cumulative_sales'] >= 0.05].shape[0] > 0:
        df.loc[df['cumulative_sales'] >= 0.05, 'product_group'] = 2
    #If not, recommend the top 3 products
    else:
        df = df.sort_values(by='sales', ascending=False)
        df.loc[:3, 'product_group'] = 2

    return df

def select_and_rename(df):

    df = df[['permalink','image_url','title','quality_score','stock','position','price','sales','conversion','sales_potential']]
    df = df.rename(columns={'item_id':'ID do Produto','title':'Descrição','quality_score':'Score de Qualidade','stock':'Estoque','position':'Posição Mais Vendidos','price':'Preço','sales':'Vendas','conversion':'Conversão','sales_potential':'Receita p/ Click'})

    return df

async def convert_html_to_pdf(html_content, pdf_output_path):
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            
            await page.set_content(html_content)
            
            pdf_bytes = await page.pdf(
                width="14.8in",
                height="21in",
                margin={"top": "0", "right": "0", "bottom": "0", "left": "0"},
                print_background=True
            )
            
            await browser.close()

            ghostscript_cmd = [
                'gswin64c',
                '-sDEVICE=pdfwrite',
                '-dPDFSETTINGS=/ebook',
                '-dNOPAUSE',
                '-dQUIET',
                '-dBATCH',
                '-sOutputFile=-',
                '-'
            ]
            
            proc = await asyncio.to_thread(
                subprocess.run,
                ghostscript_cmd,
                input=pdf_bytes,
                capture_output=True,
                check=True
            )
            
            with open(pdf_output_path, 'wb') as f:
                f.write(proc.stdout)
            
            print(f"Successfull PDF conversion: {pdf_output_path}")
            return True

    except Exception as e:
        if os.path.exists(pdf_output_path):
            os.remove(pdf_output_path)
        print(f"Error during PDF conversion: {str(e)}")
        return False
async def process_file(semaphore, file):
    try:

        # Set up Jinja2 environment
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template('table_template.html')

        # Get the input dataframe
        df = await read_input('output_tables\\' + file)
        store_name = df['store_name'].iloc[0]
        store_permalink = df['store_permalink'].iloc[0]
        
        df_rec = df[df['product_group'] == 2]
        df_others = df[df['product_group'] == 1]
        df_others.sort_values(by=['sales', 'sales_potential'], ascending=False)

        df_rec = select_and_rename(df_rec)
        df_others = select_and_rename(df_others)

        html_output = template.render(
            df_rec=df_rec,
            df_others=df_others,
            store_name=store_name,
            store_permalink=store_permalink,
            page_title_text='Recomendação de Produtos'
        )
        
        pdf_path = f'output_pdf/{file.removesuffix(".csv")}.pdf'
        async with semaphore:
            success = await convert_html_to_pdf(html_output, pdf_path)
        
        return f"Processed {file} - {'Success' if success else 'Failed'}"
    
    except Exception as e:
        return f"Error processing {file}: {str(e)}"


async def main():
    MAX_WORKERS = 5
    os.makedirs('output_pdf', exist_ok=True)

    # Listar todos os arquivos na pasta
    files = [f for f in os.listdir('output_tables') 
             if f.endswith('.csv') and os.path.isfile(os.path.join('output_tables', f))]
    
    semaphore = asyncio.Semaphore(MAX_WORKERS)

    tasks = [process_file(semaphore, file) for file in files]
    results = await asyncio.gather(*tasks)
    
    for result in results:
        print(result)

if __name__ == '__main__':
    asyncio.run(main())
