#!/usr/bin/env python
# coding: utf-8

# In[1]:


import mysql.connector
import pandas as pd


# In[2]:


con = mysql.connector.connect(user = "root", password = 'admin', host = '10.112.160.3', database = 'NOVO_MYDB2')
cursor = con.cursor()


# In[3]:


query = f"select * from restaurante_similares limit 00001, 10000;"
cursor.execute(query)
dados = cursor.fetchall()

df0 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[4]:


query = f"select * from restaurante_similares limit 10001, 20000;"
cursor.execute(query)
dados = cursor.fetchall()

df1 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[5]:


query = f"select * from restaurante_similares limit 20001, 30000;"
cursor.execute(query)
dados = cursor.fetchall()

df2 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[6]:


query = f"select * from restaurante_similares limit 30001, 40000;"
cursor.execute(query)
dados = cursor.fetchall()

df3 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[7]:


query = f"select * from restaurante_similares limit 40001, 50000;"
cursor.execute(query)
dados = cursor.fetchall()

df4 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[8]:


query = f"select * from restaurante_similares limit 50001, 60000;"
cursor.execute(query)
dados = cursor.fetchall()

df5 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[9]:


query = f"select * from restaurante_similares limit 60001, 70000;"
cursor.execute(query)
dados = cursor.fetchall()

df6 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[10]:


query = f"select * from restaurante_similares limit 70001, 80000;"
cursor.execute(query)
dados = cursor.fetchall()

df7 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[11]:


query = f"select * from restaurante_similares limit 80001, 90000;"
cursor.execute(query)
dados = cursor.fetchall()

df8 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[12]:


query = f"select * from restaurante_similares limit 90001, 100000;"
cursor.execute(query)
dados = cursor.fetchall()

df9 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[13]:


query = f"select * from restaurante_similares limit 100001, 110000;"
cursor.execute(query)
dados = cursor.fetchall()

df10 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[14]:


df10


# In[15]:


query = f"select * from restaurante_similares limit 120001, 130000;"
cursor.execute(query)
dados = cursor.fetchall()

df11 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[3]:


df11


# In[ ]:


query = f"select * from restaurante_similares limit 130001, 140000;"
cursor.execute(query)
dados = cursor.fetchall()

df12 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = f"select * from restaurante_similares limit 140001, 150000;"
cursor.execute(query)
dados = cursor.fetchall()

df13 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = f"select * from restaurante_similares limit 150001, 160000;"
cursor.execute(query)
dados = cursor.fetchall()

df14 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = f"select * from restaurante_similares limit 160001, 170000;"
cursor.execute(query)
dados = cursor.fetchall()

df15 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = f"select * from restaurante_similares limit 170001, 180000;"
cursor.execute(query)
dados = cursor.fetchall()

df16 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = f"select * from restaurante_similares limit 180001, 190000;"
cursor.execute(query)
dados = cursor.fetchall()

df17 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = f"select * from restaurante_similares limit 190001, 200000;"
cursor.execute(query)
dados = cursor.fetchall()

df18 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = f"select * from restaurante_similares limit 200001, 210000;"
cursor.execute(query)
dados = cursor.fetchall()

df19 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])


# In[ ]:


query = f"select * from restaurante_similares limit 210001, 220000;"
cursor.execute(query)
dados = cursor.fetchall()

df20 = pd.DataFrame(dados, columns=['CNPJ','CNPJ_ordem','CNPJ_dv','razao_social','porte_empresa','CNAE','cod_sit_cadastral','data_sit_cadastral','data_inicio_atividade', 'natureza_juridica', 'municipio', 'UF'])

