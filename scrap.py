#python -m pip install --upgrade pip
#pip install selenium google-auth google-auth-oauthlib pandas webdriver-manager pandas_gbq

########################################################################################################
# SCRIPT QUE FAZ O SCRAP DAS VAGAS NO SITE DA CATHO // METODO SCRAP
# NAO ESQUECER DE ALTERAR O NOME DO ARQUIVO JSON DA CHAVE GCP NA LINHA 129 DESTE CODIGO
# COMENTE A LINHA 119 CASO QUEIRA PERCORRER POR TODAS AS PAGINAS 
########################################################################################################

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas_gbq as pdgbq
from google.oauth2 import service_account

import re
import time

def fechaCookies(driver):
    try:
        driver.find_element(By.XPATH, "//*[@id='lgpd-consent-widget']/section/div/div[2]/button[1]").click()

    except:
        None

serv = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=serv)
driver.maximize_window()

driver.get("https://www.catho.com.br/")

time.sleep(2)

driver.find_element(By.NAME, "q").send_keys("engenheiro de dados")

time.sleep(1)

fechaCookies(driver)

driver.find_element(By.NAME, "submit").click()

time.sleep(2)
anuncios = driver.find_element(By.XPATH, "//*[@id='jobTitle']")
paginas = anuncios.text.split(" vagas")

pg = 1
total_pgs = round(int(paginas[0]) / 15)
if total_pgs == 0 and int(paginas[0]) > 1:
    total_pgs = 1

print("Total de anuncios a percorrer:" + str(paginas[0]))
print(f"Total de paginas a percorrer: {total_pgs}" )

lista_df = []

y = 0
#iniciando o loop
while pg <= total_pgs:
    y += 1

    #captura o bloco de vagas
    ul_element = driver.find_element(By.XPATH, "/html/body/div[1]/div[1]/main/div[3]/div/div/section/ul")

    li_elements = ul_element.find_elements(By.TAG_NAME, "li")

    c = 0
    #faz um loop por cada li encontrado
    for li in li_elements:
        c += 1
        titulo = driver.find_element(By.XPATH, f"/html/body/div[1]/div[1]/main/div[3]/div/div/section/ul/li[{c}]/article/article/header/div/div[1]/h2/a")
        empresa = driver.find_element(By.XPATH, f"/html/body/div[1]/div[1]/main/div[3]/div/div/section/ul/li[{c}]/article/article/header/div/p")

        salario = driver.find_element(By.XPATH, f"/html/body/div[1]/div[1]/main/div[3]/div/div/section/ul/li[{c}]/article/article/header/div/div[2]/div[1]")
        regiao = driver.find_element(By.XPATH, f"/html/body/div[1]/div[1]/main/div[3]/div/div/section/ul/li[{c}]/article/article/header/div/div[2]/button/a")
        datapublicacao = driver.find_element(By.XPATH, f"/html/body/div[1]/div[1]/main/div[3]/div/div/section/ul/li[{c}]/article/article/header/div/div[2]/time/span")

        continuarlendo = driver.find_element(By.XPATH, f"/html/body/div[1]/div[1]/main/div[3]/div/div/section/ul/li[{c}]/article/article/div/div[1]/button").click

        descricao = driver.find_element(By.XPATH, f"/html/body/div[1]/div[1]/main/div[3]/div/div/section/ul/li[{c}]/article/article/div/div[1]")

        #print(titulo.text, empresa.text, salario.text, regiao.text, datapublicacao.text, descricao.text)
        
        match = re.search(r'\((\d+)\)', regiao.text)
        
        try:
            vagas = match.group(1)
        except:
            None

        df = pd.DataFrame(
            {
                'titulo': [titulo.text]
                ,'empresa': [empresa.text]
                ,'salario': [salario.text]
                ,'regiao': [regiao.text]
                ,'datapublicacao': [datapublicacao.text]
                ,'vagas': vagas
                ,'descricao': [descricao.text]

            }
        )

        #append da lista
        lista_df.append(df)

    #incremento da pagina
    pg += 1
    if pg <= total_pgs:
        if "page=" in driver.current_url:
            url = driver.current_url.replace("page=" + str(pg-1), "page=" + str(pg))
        else:
            url = driver.current_url + "&page=" + str(pg)

        driver.get(url)
        time.sleep(2)

    if pg == 3: break

#empilhar a lista em unico dataframe
if len(lista_df) > 1:
    df_final = pd.concat(lista_df)
else:
    df_final = lista_df[0]

df_final = df_final.reset_index(drop=True)

chave = service_account.Credentials.from_service_account_file('arquivochave-gcp.json')
df_final.to_gbq("raspagem.scrap", project_id='aula-gcp-arruda', if_exists="replace", credentials=chave)
