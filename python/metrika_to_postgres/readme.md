# ETL. Из Яндекс-метрики в Postgres



## Описание задачи


<img align="left" src="/img/1.jpg">Анастасия работает в отделе маркетинга и каждую неделю делает отчет посещаемости сайта.
Для этого она вручную выгружает статистику за последнюю неделю из разных отчетов Яндекс метрики и сводит их вместе с прошлыми данными в Excel.
Это рутинная и скучная работа, которую хотелось бы автоматизировать. 
К тому же в компании много различных отделов, каждый из которых использует инструменты по своему вкусу: excel, power bi, tableau и.т.д. И им всем нужен постоянный доступ к этим данным через эти инструменты.
Т.е необходимо построить ETL с хранилищем данных, к которому могут подключаться все.



## Решение
Используемый стек: python, postgres, cron 
<img src="/img/2.jpg"> 

[схема](https://miro.com/app/board/o9J_kklpq74=/)

## ETL

* Extract - библиотека ‘request’
* Transform - библиотека ‘pandas’
* Load - библиотека ‘sqlalchemy’

[Jupyter notebook backfill](https://github.com/freemastera/etl-python-metrika/tree/master/notebooks/metrika_to_postgres_backfilling_historical_data.ipynb)

[Jupyter notebook update](https://github.com/freemastera/etl-python-metrika/tree/master/notebooks/metrika_to_postgres_daily_update.ipynb)

Код на python

```python
import requests, sqlalchemy, pandas as pd
from sqlalchemy import create_engine
```


```python
# Задаем параметры для URL для API
url_param = "https://api-metrika.yandex.net/stat/v1/data"
```


```python
# Задаем параметры для API. Подробнее в справке https://yandex.ru/dev/metrika/doc/api2/api_v1/data.html
api_param = {
    'ids':44147844,
    'metrics':'ym:s:users,ym:s:visits,ym:s:pageviews,ym:s:bounceRate,ym:s:pageDepth,ym:s:avgVisitDurationSeconds',
    'dimensions':'ym:s:date,ym:s:<attribution>TrafficSource,ym:s:<attribution>SourceEngine',
    'date1':'2020-01-01', # Дата начала выгрузки исторических данных
    'date2':'2daysAgo', # Дата конца выгрузки исторических данных
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

```


```python
'''
Так как у счетчика 44147844 публичный доступ, то токен не нужен. В противном случае его нужно будет указать
ACCESS_TOKEN = 'Ваш_токен'

И нужна дополнительная строка в header_params
'Authorization': 'OAuth ' + ACCESS_TOKEN 

Подробнее https://yandex.ru/dev/metrika/doc/api2/intro/authorization.html/
'''
```




    "\nТак как у счетчика 44147844 публичный доступ, то токен не нужен. В противном случае его нужно будет указать\nACCESS_TOKEN = 'Ваш_токен'\n\nИ нужна дополнительная строка в header_params\n'Authorization': 'OAuth ' + ACCESS_TOKEN \n\nПодробнее https://yandex.ru/dev/metrika/doc/api2/intro/authorization.html/\n"




```python
 # Отправляем get request (запрос GET)
response = requests.get(
    url_param,
    params=api_param,
    headers=header_params
        )
```


```python
result = response.json()
        
```


```python
json_data = result['data']
```


```python
# Делаем плоскую таблицу в dataframe из json с большим уровнем вложенности. 
dict_data = {}
```


```python
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
```


```python
dict_keys = dict_data[0].keys()
```


```python
df = pd.DataFrame.from_dict(dict_data, orient='index',columns=dict_keys)
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>traffic-source</th>
      <th>traffic-details</th>
      <th>users</th>
      <th>visits</th>
      <th>pageviews</th>
      <th>bounceRate</th>
      <th>pageDepth</th>
      <th>avgVisitDurationSeconds</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2020-01-01</td>
      <td>Internal traffic</td>
      <td>None</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>100.000000</td>
      <td>1.000000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2020-01-01</td>
      <td>Direct traffic</td>
      <td>None</td>
      <td>43.0</td>
      <td>48.0</td>
      <td>78.0</td>
      <td>37.500000</td>
      <td>1.625000</td>
      <td>42.791667</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2020-01-01</td>
      <td>Link traffic</td>
      <td>aromaoils.ru</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>0.000000</td>
      <td>2.000000</td>
      <td>18.000000</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2020-01-01</td>
      <td>Link traffic</td>
      <td>clickhouse.yandex</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>2.0</td>
      <td>0.000000</td>
      <td>1.000000</td>
      <td>18.500000</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2020-01-01</td>
      <td>Link traffic</td>
      <td>developers.google.com</td>
      <td>9.0</td>
      <td>9.0</td>
      <td>13.0</td>
      <td>55.555556</td>
      <td>1.444444</td>
      <td>216.333333</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>32570</th>
      <td>2020-11-12</td>
      <td>Ad traffic</td>
      <td>Google Ads</td>
      <td>12.0</td>
      <td>17.0</td>
      <td>33.0</td>
      <td>29.411765</td>
      <td>1.941176</td>
      <td>60.882353</td>
    </tr>
    <tr>
      <th>32571</th>
      <td>2020-11-12</td>
      <td>Ad traffic</td>
      <td>Yandex.Direct: Undetermined</td>
      <td>10.0</td>
      <td>11.0</td>
      <td>13.0</td>
      <td>45.454545</td>
      <td>1.181818</td>
      <td>11.000000</td>
    </tr>
    <tr>
      <th>32572</th>
      <td>2020-11-12</td>
      <td>None</td>
      <td>None</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>100.000000</td>
      <td>1.000000</td>
      <td>0.000000</td>
    </tr>
    <tr>
      <th>32573</th>
      <td>2020-11-12</td>
      <td>Social network traffic</td>
      <td>VKontakte</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>100.000000</td>
      <td>1.000000</td>
      <td>8.000000</td>
    </tr>
    <tr>
      <th>32574</th>
      <td>2020-11-12</td>
      <td>Social network traffic</td>
      <td>Linked in</td>
      <td>4.0</td>
      <td>4.0</td>
      <td>6.0</td>
      <td>50.000000</td>
      <td>1.500000</td>
      <td>19.500000</td>
    </tr>
  </tbody>
</table>
<p>32575 rows × 9 columns</p>
</div>




```python
# Делаем нужные нам преобразования. Например, добавим новый столбец 'avgUserPageviews(среднее кол-во просмотров на пользователя)' 
df['avgUserPageviews'] = df['pageviews'] / df['users']
```


```python
#и округлим длинные значения
df[['bounceRate','pageDepth','avgVisitDurationSeconds','avgUserPageviews']] = df[['bounceRate','pageDepth','avgVisitDurationSeconds','avgUserPageviews']].round(1)
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>date</th>
      <th>traffic-source</th>
      <th>traffic-details</th>
      <th>users</th>
      <th>visits</th>
      <th>pageviews</th>
      <th>bounceRate</th>
      <th>pageDepth</th>
      <th>avgVisitDurationSeconds</th>
      <th>avgUserPageviews</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2020-01-01</td>
      <td>Internal traffic</td>
      <td>None</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>100.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2020-01-01</td>
      <td>Direct traffic</td>
      <td>None</td>
      <td>43.0</td>
      <td>48.0</td>
      <td>78.0</td>
      <td>37.5</td>
      <td>1.6</td>
      <td>42.8</td>
      <td>1.8</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2020-01-01</td>
      <td>Link traffic</td>
      <td>aromaoils.ru</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>2.0</td>
      <td>18.0</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2020-01-01</td>
      <td>Link traffic</td>
      <td>clickhouse.yandex</td>
      <td>1.0</td>
      <td>2.0</td>
      <td>2.0</td>
      <td>0.0</td>
      <td>1.0</td>
      <td>18.5</td>
      <td>2.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2020-01-01</td>
      <td>Link traffic</td>
      <td>developers.google.com</td>
      <td>9.0</td>
      <td>9.0</td>
      <td>13.0</td>
      <td>55.6</td>
      <td>1.4</td>
      <td>216.3</td>
      <td>1.4</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>32570</th>
      <td>2020-11-12</td>
      <td>Ad traffic</td>
      <td>Google Ads</td>
      <td>12.0</td>
      <td>17.0</td>
      <td>33.0</td>
      <td>29.4</td>
      <td>1.9</td>
      <td>60.9</td>
      <td>2.8</td>
    </tr>
    <tr>
      <th>32571</th>
      <td>2020-11-12</td>
      <td>Ad traffic</td>
      <td>Yandex.Direct: Undetermined</td>
      <td>10.0</td>
      <td>11.0</td>
      <td>13.0</td>
      <td>45.5</td>
      <td>1.2</td>
      <td>11.0</td>
      <td>1.3</td>
    </tr>
    <tr>
      <th>32572</th>
      <td>2020-11-12</td>
      <td>None</td>
      <td>None</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>100.0</td>
      <td>1.0</td>
      <td>0.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>32573</th>
      <td>2020-11-12</td>
      <td>Social network traffic</td>
      <td>VKontakte</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>1.0</td>
      <td>100.0</td>
      <td>1.0</td>
      <td>8.0</td>
      <td>1.0</td>
    </tr>
    <tr>
      <th>32574</th>
      <td>2020-11-12</td>
      <td>Social network traffic</td>
      <td>Linked in</td>
      <td>4.0</td>
      <td>4.0</td>
      <td>6.0</td>
      <td>50.0</td>
      <td>1.5</td>
      <td>19.5</td>
      <td>1.5</td>
    </tr>
  </tbody>
</table>
<p>32575 rows × 10 columns</p>
</div>




```python
# Преобразуем столбец 'date' из строки в дату
df['date'] = pd.to_datetime(df['date'], format ='%Y-%m-%d').dt.date
```


```python
# импорт датафрейма в csv. Подробнее https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html
df.to_csv(r'D:\python\metrika.csv', index = False, sep=';')
```


```python
# импорт датафрейма, как таблицы 'metrika_sources' в БД postgres, cхема 'p_stg'. Самому предварительно создавать в БД пустую таблицу не нужно
# postgresql://user:password@host:port/dbname 
engine = create_engine('postgresql://postgres:admin@localhost:5432/postgres')
```


```python
df.to_sql('metrika_sources', engine, schema='p_stg', index = False)
```



## Хранилище данных 

Postgres

<img src="/img/15.jpg">

## Sheduler

Cron в unix системах (bash скрипт) или планировщик заданий в Windows (bat скрипт). 
В системе должен быть установлен python и библиотеки: request,pandas,sqlalchemy,psycopg2

*  [metrika\_etl\_backfill.bat](https://github.com/freemastera/etl-python-metrika/tree/master/metrika_etl_backfill.bat) - скрипт для загрузки исторических данных (в примере установлен период с 1 января 2020 по позавчера) 
*  [metrika\_etl\_update.bat](https://github.com/freemastera/etl-python-metrika/tree/master/metrika_etl_update.bat) - скрипт для ежедневного обновления (в примере данные за вчерашний день)

## Подключение BI систем к базе данных


Для подключения Bi систем к postgres необходимо будет установить  [PostgreSQL ODBC driver](https://www.postgresql.org/ftp/odbc/versions/msi/). 
Чтобы он отображался в Excel или Power bi в качестве коннектора по умолчанию, необходимо добавить его в качестве источника данных ODBC в свою систему.


### Для системы Windows
1. Панель управления\Все элементы панели управления\Администрирование
2. Затем дважды щелкните "Источники данных (ODBC)".
Возможно, потребуется щелкнуть ссылку категории "Производительность и обслуживание" или "Система и безопасность" в зависимости от версии операционной системы.
 
3. В диалоговом окне "Администратор источников данных ODBC" выполните одну из следующих операций:
	* Перейдите на вкладку "Пользовательский DSN", чтобы создать источник данных, видимый только для создавшего его пользователя и доступного только на компьютере, используемом для создания.
	* Перейдите на вкладку "Системный DSN", чтобы создать источник данных, видимый для всех пользователей с правами доступа на данном компьютере.
	* Перейдите на вкладку "Файловый DSN", чтобы создать источник данных, к которому могут получить доступ все пользователи, на компьютерах которых установлены такие же драйверы ODBC
 
4. Нажмите добавить и выберите установленный драйвер для которого задается источник
<img src="/img/3.jpg">

5. Введите аутентификационные данные для доступа к postgres

<img src="/img/4.jpg">

Теперь коннектор к postgres будет доступен по умолчанию в качестве источника данных ODBC в программах вроде Excel или Power Bi

### Подключаемся к postgres в excel

Данные → Получить данные → Из других источников → Из ODBC

<img src="/img/5.jpg">

Выбираем Postgres

<img src="/img/6.jpg">

Выбираем нужную таблицу

<img src="/img/7.jpg">

Выгружаем данные в Excel
<img src="/img/8.jpg">

Мы можем также настроить параметры обновления по своему усмотрению.
Открываем свойства подключения.

<img src="/img/9.jpg">

В настройках подключения задаем, каким образом и как часто мы хотим обновлять данные.
В данном примере, добавим обновление при открытии файла.

<img src="/img/10.jpg">


### Подключаемся к postgres в Power bi
Get data → Other → ODBC
<img src="/img/11.jpg">

Выбираем postgres
<img src="/img/12.jpg">

Выбираем нужную таблицу

<img src="/img/13.jpg">

Загружаем данные из БД и начинаем работу

<img src="/img/14.jpg">
