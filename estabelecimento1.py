#!/usr/bin/env python
# coding: utf-8

# In[22]:


import csv
from pyspark.sql import SparkSession


# In[23]:


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


# In[1]:


df_tabela_estabelecimentos1 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y1.D11211ESTABELE.csv")


# In[2]:


df_tabela_estabelecimentos1.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos1-csv")


# In[ ]:





# In[24]:


df_tabela_estabelecimentos2 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y2.D11211ESTABELE.csv")


# In[25]:


df_tabela_estabelecimentos2.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos2-csv")


# In[ ]:





# In[4]:


df_tabela_estabelecimentos3 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y3.D11211ESTABELE.csv")


# In[5]:


df_tabela_estabelecimentos3.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos3-csv")


# In[ ]:





# In[6]:


df_tabela_estabelecimentos4 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y4.D11211ESTABELE.csv")


# In[7]:


df_tabela_estabelecimentos4.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos4-csv")


# In[ ]:





# In[8]:


df_tabela_estabelecimentos5 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y5.D11211ESTABELE.csv")


# In[9]:


df_tabela_estabelecimentos5.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos5-csv")


# In[ ]:





# In[10]:


df_tabela_estabelecimentos6 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y6.D11211ESTABELE.csv")


# In[11]:


df_tabela_estabelecimentos6.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos6-csv")


# In[ ]:





# In[16]:


df_tabela_estabelecimentos7 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y7.D11211ESTABELE.csv")


# In[17]:


df_tabela_estabelecimentos7.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos7-csv")


# In[ ]:





# In[18]:


df_tabela_estabelecimentos8 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y8.D11211ESTABELE.csv")


# In[19]:


df_tabela_estabelecimentos8.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos8-csv")


# In[ ]:





# In[20]:


df_tabela_estabelecimentos9 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_estabelecimento/ESTABELECIMENTO/K3241.K03200Y9.D11211ESTABELE.csv")


# In[21]:


df_tabela_estabelecimentos9.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/estabelecimentos9-csv")


# In[ ]:




