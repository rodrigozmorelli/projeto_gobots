import pandas as pd
from jinja2 import Environment, FileSystemLoader

# Create a sample DataFrame
def read_input():
    df =  pd.read_csv('ranking.csv')

    #Select the desired columns and format them
    df = df[['permalink','image_url','title','quality_score','price','sales','conversion','sales_potential']]
    df['conversion'] = (df['conversion']).astype(float).map('{:.1%}'.format)

    
    # Format the column as Brazilian Reais
    df['price'] = df['price'].apply(lambda x: f"R${x:,.2f}")
    df['sales_potential'] = df['sales_potential'].apply(lambda x: f"R${x:,.2f}")


    df = df.rename(columns={'item_id':'ID do Produto','title':'Descrição','quality_score':'Score de Qualidade','price':'Preço','sales':'Vendas','conversion':'Conversão','sales_potential':'Receita p/ Click'})
    df = df.head(3)

    return df

# Set up Jinja2 environment
env = Environment(loader=FileSystemLoader('.'))
template = env.get_template('table_template.html')

# Render the template with the DataFrame
html_output = template.render(df=read_input(),
                              page_title_text='Recomendação de Produtos')

# Write the output to a file
with open('output.html', 'w') as f:
    f.write(html_output)