#!/usr/bin/env python
# coding: utf-8

# In[7]:


import csv
from pyspark.sql import SparkSession


# In[8]:


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


# In[9]:


df_tabela_municipios = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "UTF-8").load(r"gs://arquivos-projeto/F.K03200$Z.D11211MUNIC.CSV")


# In[10]:


df_tabela_municipios.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/municipios-csv")


# In[11]:


df_tabela_municipios.show(50, truncate = False)


# In[6]:


df_tabela_municipios.count()


# In[ ]:




