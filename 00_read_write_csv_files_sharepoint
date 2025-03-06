client_id = "client_id"
client_secret = "client_secret"
tenant_id = "tenant_id"

import io
import pandas as pd
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType

def get_access_token(tenant_id, client_id, client_secret):
    """Autentica no Microsoft Graph API e retorna o token de acesso"""
    authority_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    auth_data = {
        'client_id': client_id,
        'scope': 'https://graph.microsoft.com/.default',
        'client_secret': client_secret,
        'grant_type': 'client_credentials'
    }
    
    response = requests.post(authority_url, data=auth_data)
    
    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        raise Exception(f"Erro na autenticação: {response.status_code} - {response.text}")

def get_site_id(headers, organisation_domain, site_name):
    """Obtém o ID do site SharePoint"""
    site_url = f"https://graph.microsoft.com/v1.0/sites/{organisation_domain}.sharepoint.com:/sites/{site_name}"
    response = requests.get(site_url, headers=headers)

    if response.status_code == 200:
        return response.json().get("id")
    else:
        raise Exception(f"Erro ao obter Site ID: {response.status_code} - {response.text}")

def get_drive_id(headers, site_id, pasta_raiz):
    """Obtém o ID do drive (pasta raiz) desejado"""
    drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    response = requests.get(drive_url, headers=headers)

    if response.status_code == 200:
        for drive in response.json()['value']:
            if drive['name'] == pasta_raiz:
                return drive['id']
    raise Exception(f"Drive '{pasta_raiz}' não encontrado ou erro na requisição")

def list_files(headers, site_id, drive_id, caminho_pasta):
    """Lista arquivos na pasta especificada"""
    files_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:{caminho_pasta}:/children"
    response = requests.get(files_url, headers=headers)

    if response.status_code == 200:
        return response.json().get('value', [])
    else:
        raise Exception(f"Erro ao listar arquivos: {response.status_code} - {response.text}")

def process_csv_files(headers, csv_files):
    """Baixa e processa os arquivos CSV"""
    all_dataframes = []
    
    for csv_file in csv_files:
        print(f"Baixando: {csv_file['name']}")
        file_url = csv_file.get('@microsoft.graph.downloadUrl')
        
        if file_url:
            response = requests.get(file_url)
            df = pd.read_csv(io.BytesIO(response.content), delimiter=";")
            all_dataframes.append(df)
    
    if all_dataframes:
        return pd.concat(all_dataframes, ignore_index=True)
    else:
        return None

def write_delta_table(lakehouse, tabela, df_spark):
    if df_spark is None:
        print("Erro: DataFrame df_spark é None, não pode escrever a tabela!")
        return

    df_spark.write.format("delta")\
        .mode("overwrite")\
        .option("mergeSchema", "true")\
        .saveAsTable(f"{lakehouse}.{tabela}")

    print(f"Tabela: {tabela} escrita no lakehouse: {lakehouse}")


def authentication(tenant_id, client_id, client_secret, site_name, organisation_domain, pasta_raiz, caminho_pasta, lakehouse, tabela):
    """Função principal para autenticação, listagem e processamento de arquivos do SharePoint"""
    
    # Inicia sessão Spark
    spark = SparkSession.builder.appName("SharePointIntegration").getOrCreate()

    # Obtém token de acesso
    token = get_access_token(tenant_id, client_id, client_secret)
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}

    print("Autenticado com sucesso!\n")

    # Obtém Site ID e Drive ID
    site_id = get_site_id(headers, organisation_domain, site_name)
    drive_id = get_drive_id(headers, site_id, pasta_raiz)

    # Lista arquivos na pasta
    files = list_files(headers, site_id, drive_id, caminho_pasta)
    csv_files = [file for file in files if file['name'].endswith('.csv')]

    # Processa CSVs
    final_df = process_csv_files(headers, csv_files)

    if final_df is not None:
        # Converte para Spark DataFrame e aplica limpeza
        

        df_spark = spark.createDataFrame(final_df)
        
        df_spark = clean_spark_dataframe(df_spark)

        # Cria uma visualização temporária no Spark
        df_spark.createOrReplaceTempView('teste')

        df = spark.sql("SELECT * FROM teste")
        df.show()  # Substitui o display(df) para Spark
    else:
        print("Nenhum arquivo CSV encontrado!")
    
    write_delta_table(lakehouse, tabela, df_spark)


    return df_spark
