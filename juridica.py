#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


# In[3]:


df_tabela_juridica = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://arquivos-projeto/F.K03200$Z.D11211NATJU.CSV")


# In[4]:


df_tabela_juridica.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/juridica-csv")


# In[ ]:




