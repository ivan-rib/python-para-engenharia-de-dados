#!/usr/bin/env python
# coding: utf-8

# In[5]:


import csv
from pyspark.sql import SparkSession


# In[6]:


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


# In[7]:


df_tabela_simples = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").load(r"gs://arquivos-projeto/F.K03200$W.SIMPLES.CSV")


# In[8]:


df_tabela_simples.coalesce(1).write.option("header", "false").option("encoding", "ISO-8859-1").csv("gs://arquivos-projeto/simples-csv")


# In[ ]:




