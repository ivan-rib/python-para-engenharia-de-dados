#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import pandas as pd
from pyspark.sql import *


# In[2]:


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


# In[3]:


con = mysql.connector.connect(user = "root", password = 'admin', host = '10.112.160.3', database = 'NOVO_MYDB2')
cursor = con.cursor()


# In[ ]:


query = "select * from comercio_varejista limit 0, 10000;"
cursor.execute(query)
dados = cursor.fetchall()

df1 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = "select * from comercio_varejista limit 10001, 20000;"
cursor.execute(query)
dados = cursor.fetchall()

df2 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


df.write.parquet("/output/proto.parquet")

