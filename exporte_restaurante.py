from cassandra.cluster import Cluster
from pyspark.sql import *
import csv


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


cluster = Cluster(['35.243.72.182'],port=9042)
session = cluster.connect('mydb3')


df_idiomas = spark.read.format("csv").option("header", "false").option("delimiter", ",").option("inferSchema", "true").option("encoding", "UTF-8").load(r"C:\teste_exporte_view_Cloud_SQL_Export_restaurante_similares(18_05_38).csv")



c = 88874
try:
    for [CNPJ, CNPJ_ordem, CNPJ_dv, razao_social, porte_empresa, CNAE, cod_sit_cadastral, data_sit_cadastral, data_inicio_atividade, natureza_juridica, municipio, UF] in df_idiomas.collect()[88874:200000]:
            
        if razao_social == "" or razao_social == None:
            razao_social = "NÃO INFORMADO"
            
        if data_sit_cadastral == "" or data_sit_cadastral == None or data_sit_cadastral == "0000-00-00":
            data_sit_cadastral = data_inicio_atividade       
        
                

        razao_social = str(razao_social.replace("'",""))

        municipio = str(municipio.replace("'",""))
        
        CNPJ = int(CNPJ)
                    
        query = f"INSERT INTO cnpj (cnpj, cnpj_ordem, cnpj_dv, razao_social, porte_empresa, cnae, situacao_cadastral, data_situacao_cadastral, data_de_inicio_atividade, naturezas_juridica, municipio, uf) VALUES ({CNPJ}, '{CNPJ_ordem}', '{CNPJ_dv}', '{razao_social}', '{porte_empresa}', '{CNAE}', '{cod_sit_cadastral}', '{data_sit_cadastral}', '{data_inicio_atividade}', '{natureza_juridica}', '{municipio}', '{UF}');"
        session.execute(query)
        c += 1
        print(c)
except Exception as e:
    print("ERRO:", str(e))