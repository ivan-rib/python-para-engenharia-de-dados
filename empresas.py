#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
from pyspark.sql import SparkSession


# In[2]:


spark = SparkSession.builder.appName("OTR").config("spark.sql.caseSensitive", "True").getOrCreate()


# In[3]:


df_tabela_empresas = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y0.D11211EMPRE.CSV")


# In[4]:


df_tabela_empresas.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa-csv")


# In[ ]:





# In[5]:


df_tabela_empresas1 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y1.D11211EMPRE.CSV")


# In[6]:


df_tabela_empresas1.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa1-csv")


# In[ ]:





# In[7]:


df_tabela_empresas2 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y2.D11211EMPRE.CSV")


# In[8]:


df_tabela_empresas2.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa2-csv")


# In[ ]:





# In[9]:


df_tabela_empresas3 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y3.D11211EMPRE.CSV")


# In[10]:


df_tabela_empresas3.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa3-csv")


# In[ ]:





# In[11]:


df_tabela_empresas4 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y4.D11211EMPRE.CSV")


# In[12]:


df_tabela_empresas4.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa4-csv")


# In[ ]:





# In[13]:


df_tabela_empresas5 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y5.D11211EMPRE.CSV")


# In[14]:


df_tabela_empresas5.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa5-csv")


# In[ ]:





# In[15]:


df_tabela_empresas6 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y6.D11211EMPRE.CSV")


# In[16]:


df_tabela_empresas6.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa6-csv")


# In[ ]:





# In[17]:


df_tabela_empresas7 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y7.D11211EMPRE.CSV")


# In[18]:


df_tabela_empresas7.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa7-csv")


# In[ ]:





# In[19]:


df_tabela_empresas8 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y8.D11211EMPRE.CSV")


# In[20]:


df_tabela_empresas8.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa8-csv")


# In[ ]:





# In[22]:


df_tabela_empresas9 = spark.read.format("csv").option("header", "false").option("delimiter", ";").option("inferSchema", "true").option("encoding", "ISO-8859-1").load(r"gs://repositorio_empresa/EMPRESA/K3241.K03200Y9.D11211EMPRE.CSV")


# In[23]:


df_tabela_empresas9.coalesce(1).write.option("header", "false").option("encoding", "UTF-8").csv("gs://arquivos-projeto/empresa9-csv")


# In[ ]:




