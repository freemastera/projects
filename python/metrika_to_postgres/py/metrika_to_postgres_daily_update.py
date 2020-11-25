#!/usr/bin/env python
# coding: utf-8

# In[1]:


#нужны установленные библиотеки requests, pandas, sqlalchemy, psycopg2(для связки sqlalchemy с postgres, вызывать необязательно)
import requests, sqlalchemy, pandas as pd
from sqlalchemy import create_engine


# In[2]:


# Задаем параметры для URL для API
url_param = "https://api-metrika.yandex.net/stat/v1/data"


# In[3]:


# Задаем параметры для API. Подробнее в справке https://yandex.ru/dev/metrika/doc/api2/api_v1/data.html
api_param = {
    'ids':44147844,
    'metrics':'ym:s:users,ym:s:visits,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds',
    'dimensions':'ym:s:date,ym:s:<attribution>TrafficSource,ym:s:<attribution>SourceEngine',
    'date1':'yesterday',
    'date2':'yesterday',
    'sort':'ym:s:date',
    'accuracy':'full',
    'limit':100000
            }
# Задаем параметры header_params
header_params = {
    'GET': '/management/v1/counters HTTP/1.1',
    'Host': 'api-metrika.yandex.net',
    'Content-Type': 'application/x-yametrika+json'
            }


# In[4]:


""" 
Так как у счетчика 44147844 публичный доступ, то токен не нужен. В противном случае его нужно будет указать
ACCESS_TOKEN = 'Ваш_токен'

И нужна дополнительная строка в header_params
'Authorization': 'OAuth ' + ACCESS_TOKEN 

Подробнее https://yandex.ru/dev/metrika/doc/api2/intro/authorization.html/
"""


# In[5]:


# Отправляем get request (запрос GET)
response = requests.get(
   url_param,
   params=api_param,
   headers=header_params
       )


# In[6]:


result = response.json()
        


# In[7]:


json_data = result['data']


# In[8]:


# Делаем плоскую таблицу в dataframe из json с большим уровнем вложенности. 
# Если уровень вложенности небольшой, можно обойтись pandas.json_normalize
dict_data = {}


# In[9]:


for i in range(0, len(json_data)-1):
    dict_data[i] = {
                                'date':json_data[i]['dimensions'][0]['name'],
                                'traffic-source':json_data[i]['dimensions'][1]['name'],
                                'traffic-details':json_data[i]['dimensions'][2]['name'],
                                'users':json_data[i]['metrics'][0],
                                'visits':json_data[i]['metrics'][1],
                                'pageviews':json_data[i]['metrics'][2],
                                'bounceRate':json_data[i]['metrics'][3],
                                'pageDepth':json_data[i]['metrics'][4],
                                'avgVisitDurationSeconds':json_data[i]['metrics'][5]
                          }


# In[10]:


dict_keys = dict_data[0].keys()


# In[11]:


df = pd.DataFrame.from_dict(dict_data, orient='index',columns=dict_keys)


# In[12]:


df


# In[13]:


# Делаем нужные нам преобразования. Например, добавим новый столбец 'avgUserPageviews(среднее кол-во просмотров на пользователя)' 
df['avgUserPageviews'] = df['pageviews'] / df['users']


# In[14]:


#и округлим длинные значения
df[['bounceRate','pageDepth','avgVisitDurationSeconds','avgUserPageviews']] = df[['bounceRate','pageDepth','avgVisitDurationSeconds','avgUserPageviews']].round(1)


# In[15]:


df


# In[16]:


# Преобразуем столбец 'date' из строки в дату
df['date'] = pd.to_datetime(df['date'], format ='%Y-%m-%d').dt.date


# In[17]:


# импорт датафрейма в csv. Подробнее https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html
# df.to_csv(r'D:\python\metrika.csv', index = False, sep=';')


# In[18]:


# импорт датафрейма, как таблицы 'metrika_sources' в БД postgres, cхема 'p_stg'. Самому предварительно создавать в БД пустую таблицу не нужно
# postgresql://user:password@host:port/dbname 
engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')


# In[19]:


df.to_sql('metrika_sources', engine, schema='p_stg', if_exists = 'append', index = False)


# In[ ]:




