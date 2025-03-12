import os
import pandas as pd
from jinja2 import Environment, FileSystemLoader

def read_input(input_file):
    df =  pd.read_csv(input_file)

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
    if df[df['cumulative_sales'] >= 0.1].shape[0] > 0:
        df.loc[df['cumulative_sales'] >= 0.1, 'product_group'] = 2
    elif df[df['cumulative_sales'] >= 0.05].shape[0] > 0:
        df.loc[df['cumulative_sales'] >= 0.05, 'product_group'] = 2
    else:
        df = df.sort_values(by='sales', ascending=False)
        df.loc[:3, 'product_group'] = 2

    return df

def select_and_rename(df):

    df = df[['permalink','image_url','title','quality_score','stock','position','price','sales','conversion','sales_potential']]
    df = df.rename(columns={'item_id':'ID do Produto','title':'Descrição','quality_score':'Score de Qualidade','stock':'Estoque','position':'Posição Mais Vendidos','price':'Preço','sales':'Vendas','conversion':'Conversão','sales_potential':'Receita p/ Click'})

    return df

# Listar todos os arquivos na pasta
path = 'output_tables'
files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]


for file in files:
    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('table_template.html')

    # Get the input dataframe
    df = read_input('output_tables\\' + file)
    store_name = df['store_name'].iloc[0]
    store_permalink = df['store_permalink'].iloc[0]

    df_rec = df[df['product_group'] == 2]
    df_others = df[df['product_group'] == 1]
    df_others.sort_values(by=['sales','sales_potential'], ascending=False, inplace=True)

    df_rec = select_and_rename(df_rec)
    df_others = select_and_rename(df_others)

    # Render the template with the DataFrame
    html_output = template.render(df_rec=df_rec,
                                df_others=df_others,
                                store_name=store_name,
                                store_permalink=store_permalink,
                                page_title_text='Recomendação de Produtos')
    
    output_path = 'output_html\\'+ file.removesuffix(".csv") + '.html'

    # Write the output to a file
    with open(output_path, 'w') as f:
        f.write(html_output)