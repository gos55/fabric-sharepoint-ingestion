# Adicione aqui os parâmetros do arquivo no Sharepoint

site_name = "Nome do site"     # Nome do site no sharepoint
organisation_domain = "nome_do_dominio"   #nome do tenant d aazure
pasta_raiz = "Documentos"   # Exemplo de pasta raiz dentro do Sharepoint
caminho_pasta = "/Exemplo/Caminho/Pasta_arquivos"    
lakehouse = "Lakehouse_silver"    #Lakehouse destino
tabela = "nome_da_tabela_a_ser_escrita_no_lakehouse"    #Nome da tabela no lakehouse

# Cada tabela terá um dataframe diferente, portanto, selecione as colunas e trate os . e ,. Lembrando que o spark tem linguagem americana, então o que é "," aqui, tem que ser transformado para "."

```python
from pyspark.sql.functions import col, to_date, regexp_replace
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType

def clean_spark_dataframe(df_spark):
    """Realiza limpeza e conversões no DataFrame do Spark"""
    colunas_desejadas = ["coluna_a", "coluna_b", "coluna_c", "coluna_d", "coluna_e", "coluna_f", "coluna_g"]
    df_spark = df_spark.select(*colunas_desejadas)
    return (df_spark
        .withColumn("coluna_f", regexp_replace(col("coluna_f"), ",", ".").cast(FloatType())) 
        .withColumn("coluna_g", regexp_replace(col("coluna_g"), ",", ".").cast(FloatType())) 
        .withColumn("coluna_h", to_date(col("coluna_h"), "dd/MM/yyyy")) 
        .withColumn("coluna_b", col("coluna_b").cast(StringType())) 
        .withColumn("coluna_c", col("coluna_c").cast(IntegerType())) 
        .withColumn("coluna_d", col("coluna_d").cast(IntegerType())) 
        .withColumn("coluna_e", col("coluna_e").cast(IntegerType())) 
        .withColumn("coluna_f", col("coluna_f").cast(DoubleType())) 
        .withColumn("coluna_g", col("coluna_g").cast(DoubleType()))
    ) 
```


## Chame o notebook com o código principal a ser utilizado.

%run 00_read_write_csv_files_sharepoint
