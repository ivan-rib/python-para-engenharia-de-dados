#!/usr/bin/env python
# coding: utf-8

# In[13]:


import csv
from pyspark.sql import SparkSession


# In[14]:


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


# In[18]:


df_tabela_cnae = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://arquivos-projeto/F.K03200$Z.D11211CNAE.CSV")


# In[ ]:


df_tabela_df_tabela_cnaecoalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/cnaes2-csv")

