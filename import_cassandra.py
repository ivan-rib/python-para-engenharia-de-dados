from cassandra.cluster import Cluster
from pyspark.sql import *
import pandas as pd
import csv


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


cluster = Cluster(['34.146.194.241'],port=9042)
session = cluster.connect('mydb3')


query = "SELECT * FROM cnpj "
dados = session.execute(query)

df_dados = pd.DataFrame(dados)

df_dados.to_csv('cnpj_full.csv', sep = ',')

dados_sp = spark.read.format("csv").option("header", "true").option("delimiter", ",").option("inferSchema", "true").option("encoding", "UTF-8").load(r"E:\IVAN\bootcam_dados\bootcamp_soulcode\atividade_natal\cnpj_full.csv")
 
dados_sp.write.parquet(r"E:\IVAN\bootcam_dados\bootcamp_soulcode\atividade_natal\Parquet")





        