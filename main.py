#pip install requests pandas google-cloud
########################################################################################################
# SCRIPT QUE FAZ O REQUEST DAS VAGAS NO SITE DA CATHO // METODO CRAWLER
# NAO ESQUECER DE ALTERAR O NOME DO ARQUIVO JSON DA CHAVE GCP NA LINHA 87 DESTE CODIGO
########################################################################################################

import requests 
import pandas as pd
from google.oauth2 import service_account
import io
import csv
import base64

def do_req(pg, q, slug, lista_df):
    req = requests.get(f"https://www.catho.com.br/vagas/_next/data/kV_SWimkUFCXPK-QrRFx5/{slug}.json?q={q}&slug={slug}&page={pg}")

    reqjson = req.json()
    df = pd.DataFrame(reqjson['pageProps']['jobSearch']['jobSearchResult']['data']['jobs'])

    df = pd.json_normalize(df['job_customized_data']) #normalizado o json/dicionario que começa com {

    #cria novo dataframe
    df_vagas = pd.json_normalize(df['vagas'].explode().tolist()) #normalizando o dicionario que começa com [

    #concatenar
    df = pd.concat([df.drop(columns=['vagas']), df_vagas], axis=1)

    lista_df.append(df)

    # retorno
    if int(reqjson['pageProps']['pageState']['props']['page']) <= int(reqjson['pageProps']['pageState']['props']['totalPages']):
        return True
    else:
        return False
        
def start_main(request):
#if __name__ == '__main__':
    #q = "engenheiro de dados pleno"
    #metodo = "bigquery"
    q = request.args.get("q")
    metodo = request.args.get("metodo", default="json")

    if q == None or q == "": return {'data': 'Sem retorno'}

    lst_metodos_permitidos = ['json', 'csv', 'bigquery']
    if not metodo in lst_metodos_permitidos: metodo = "json"

    slug = q.replace(" ", "-")

    status = True
    pg = 1
    lista_df = []

    while status:
        status = do_req(pg, q, slug, lista_df)
        if status: pg += 1

    if len(lista_df) == 0: return {"data": "Sem resposta"}

    #empilhar a lista em unico dataframe
    if len(lista_df) > 1:
        df_final = pd.concat(lista_df)
    else:
        df_final = lista_df[0]

    df_final = df_final.reset_index(drop=True)

    #exemplos, drop de linha nula
    df_final.dropna(subset=['id'], inplace=True)

    #exemplos renomeando as colunas
    df_final = df_final.rename(columns=lambda x: x.replace('.', '_'))

    #exemplos de int e float
    df_final['id'] = df_final['id'].astype(int)
    df_final['salario'] = df_final['salario'].astype(float)

    #exemplos remocao de colunas
    df_final.drop(columns=['grupoMidia', 'benef', 'habilidades', 'ppdFiltro', 'salarioACombinar', 'hrenova', 'pja', 'origemAgregador', 'ppdInfo_instAdapt', 'anunciante_confidencial', 'contratante_confidencial'], inplace=True)

    #verificacao do metodo
    if metodo == "json":
        json_data = df_final.to_dict(orient='records')
        return {"data": json_data}
    
    elif metodo == "bigquery":
        chave = service_account.Credentials.from_service_account_file('arquivochave-gcp.json')
        df_final.to_gbq("raspagem.crawler", project_id='aula-gcp-arruda', if_exists="replace", credentials=chave) 

        return {"data": "Carga feita no bigquery"}

    elif metodo == 'csv':
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=";", quoting=csv.QUOTE_NONNUMERIC, lineterminator="\n")
        writer.writerow(df_final.columns)

        for row in df_final.itertuples(index=False):
            writer.writerow(row)

        buffer.seek(0)
        csv_bytes = buffer.read().encode('utf-8-sig')
        b64 = base64.b64encode(csv_bytes).decode()

        return f'''
        <html>
        <head>
            <style>
            body {{
                margin: 0;
                padding: 0;
                background-image: url('https://python.arrudaconsulting.com.br/wp-content/uploads/2023/04/BG-Banner-Home.jpg');
                background-repeat: no-repeat;
                background-size: cover;
            }}
            .container {{
                display: flex;
                justify-content: center;
                align-items: flex-start; /* ajustado para alinhar a imagem ainda mais no topo */
                height: 15vh; /* ajustado para diminuir a altura do contêiner */
            }}
            img {{
                max-width: 100%;
                max-height: 100%;
            }}
            .download {{
                text-align: center;
                margin-top: 20px;
                font-size: 24px;
                color: white; /* alterado para branco */
                font-family: Candara, sans-serif; /* alterado para Candara */
		        height: 10vh;                
            }}
            .download a {{
                color: #e1fa02; /* amarelo pastel em notação hexadecimal */
            }} 
            .logo {{
                display: flex;
                justify-content: center;
                align-items: center;
                height: 70vh;
            }}
            .logo img {{
                max-width: 100%;
                max-height: 100%;
            }}                       
            </style>
        </head>
        <body>
            <div class="container">
            <img src="https://python.arrudaconsulting.com.br/wp-content/uploads/2023/04/Ativo-5v.png" alt="Python" width="250" height="110">
            </div>
            <div class="download">
            <p>Download arquivo para as vagas de: <span class="negrito"><a href="data:text/csv;base64,{b64}" download="{slug}.csv" >{q}</a></span> </p>
            </div>
            <div class="logo">
                <img src="https://i.postimg.cc/RVVVX3bJ/Sem-t-tulo.jpg width="450" height="450">
            </div>

        </body>
        </html>        
        
        
        '''
