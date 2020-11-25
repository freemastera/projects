# ETL. Из Яндекс-метрики в Postgres



## Описание задачи

<img align="left" src="/python/metrika_to_postgres/img/1.jpg" width="150" height="150">Анастасия работает в отделе маркетинга и каждую неделю делает отчет посещаемости сайта.
Для этого она вручную выгружает статистику за последнюю неделю из разных отчетов Яндекс метрики и сводит их вместе с прошлыми данными в Excel.
Это рутинная и скучная работа, которую хотелось бы автоматизировать. 
К тому же в компании много различных отделов, каждый из которых использует инструменты по своему вкусу: excel, power bi, tableau и.т.д. И им всем нужен постоянный доступ к этим данным через эти инструменты.
Т.е необходимо построить ETL с хранилищем данных, к которому могут подключаться все.

## Решение
Используемый стек: python, postgres, cron 

<img src="/python/metrika_to_postgres/img/2.jpg" width="571" height="362"> 

[схема](https://miro.com/app/board/o9J_kklpq74=/)

## ETL

* Extract - библиотека ‘request’
* Transform - библиотека ‘pandas’
* Load - библиотека ‘sqlalchemy’

[Jupyter notebook backfill](https://github.com/freemastera/etl-projects/blob/master/python/metrika_to_postgres/notebooks/metrika_to_postgres_backfilling_historical_data.ipynb)

[Jupyter notebook update](https://github.com/freemastera/etl-projects/blob/master/python/metrika_to_postgres/notebooks/metrika_to_postgres_daily_update.ipynb)


## Хранилище данных 

Postgres

<img src="/python/metrika_to_postgres/img/15.jpg">

## Sheduler

Cron в unix системах (bash скрипт) или планировщик заданий в Windows (bat скрипт). 
В системе должен быть установлен python и библиотеки: request,pandas,sqlalchemy,psycopg2

*  [metrika\_etl\_backfill.bat](https://github.com/freemastera/etl-projects/blob/master/python/metrika_to_postgres/metrika_etl_backfill.bat) - скрипт для загрузки исторических данных (в примере установлен период с 1 января 2020 по позавчера) 
*  [metrika\_etl\_update.bat](https://github.com/freemastera/etl-projects/blob/master/python/metrika_to_postgres/metrika_etl_update.bat) - скрипт для ежедневного обновления (в примере данные за вчерашний день)

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
<img src="/python/metrika_to_postgres/img/3.jpg">

5. Введите аутентификационные данные для доступа к postgres

<img src="/python/metrika_to_postgres/img/4.jpg">

Теперь коннектор к postgres будет доступен по умолчанию в качестве источника данных ODBC в программах вроде Excel или Power Bi

### Подключаемся к postgres в excel

Данные → Получить данные → Из других источников → Из ODBC

<img src="/python/metrika_to_postgres/img/5.jpg">

Выбираем Postgres

<img src="/python/metrika_to_postgres/img/6.jpg">

Выбираем нужную таблицу

<img src="/python/metrika_to_postgres/img/7.jpg">

Выгружаем данные в Excel
<img src="/python/metrika_to_postgres/img/8.jpg">

Мы можем также настроить параметры обновления по своему усмотрению.
Открываем свойства подключения.

<img src="/python/metrika_to_postgres/img/9.jpg">

В настройках подключения задаем, каким образом и как часто мы хотим обновлять данные.
В данном примере, добавим обновление при открытии файла.

<img src="/python/metrika_to_postgres/img/10.jpg">


### Подключаемся к postgres в Power bi

Get data → Other → ODBC

<img src="/python/metrika_to_postgres/img/11.jpg">

Выбираем postgres

<img src="/python/metrika_to_postgres/img/12.jpg">

Выбираем нужную таблицу

<img src="/python/metrika_to_postgres/img/13.jpg">

Загружаем данные из БД и начинаем работу

<img src="/python/metrika_to_postgres/img/14.jpg">
