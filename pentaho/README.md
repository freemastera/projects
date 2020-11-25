# Pentaho Data Integration (PDI)


[Pentaho DI](https://sourceforge.net/projects/pentaho/) - это ETL (Extract, Transform, Load) инструмент для получения, объединения, очистки и подготовки разнообразных данных из любого источника в любой среде. Основная задача которого свести все имеющиеся данные в единое хранилище (DWH) и предоставить бизнес-пользователям собранную и пригодную для анализа информацию.
Pentaho DI имеет:
* Графический конструктор ETL процедур, значительно упрощающий создание процедур загрузки и обработки данных.
* Большую библиотеку встроенных элементов для доступа к данным, обработки и очистки данных.
* Огромные возможности для загрузки, объединения и преобразования данных по требуемым условиям.


## Описание проекта  
<img align="left" src="/pentaho/img/1.png">
Виктор - аналитик данных в компании. Одна из задач - это еженедельные отчеты по продажам для руководства. 


Виктор каждый понедельник сводит данные из разрозненных источников в excel. Сами данные неполные, часто содержат ошибки и разного рода опечатки.  


Вместо того, чтоб тратить время на анализ, вынужден много времени сводить эти отчеты вместе и устранять типовые ошибки. Часть ошибок, например, неполные данные, можно решить только с участием коллег. Пока их обнаружишь, пока запросишь необходимую информацию, пока придет ответ - так и рабочий день к концу подойдет. А ведь надо еще анализ провести и отчет для руководства с рекомендациями сделать. 


Руководству тоже сильно не нравится, что в итоге получает отчет не до обеда, как хотелось бы, а в конце рабочего дня. Сам отчет регулярный и процесс повторяется каждую неделю. 


Виктору хотелось бы автоматизировать часть работы с получением, сведением и обработкой данных. На основе источника с обработанными данными создать автообновляемый дашборд в BI системе и с самого утра заниматься только анализом. И до обеда успеть сделать и отправить руководству полный отчет. 


Желательно, чтобы он еще обновлялся ежедневно, а не раз в неделю. 
Т.к помимо еженедельной отчетности, планируется ежедневно мониторить основные метрики и в случае необходимости, принять меры, не дожидаясь понедельника.
 
### Исходные данные:
Пример реализован на основе датасета [Tableau Samplestore](https://github.com/Data-Learn/data-engineering/blob/master/DE-101/Module-01/Lab/Sample%20-%20Superstore%20-%20Dashboard.xlsx).
Данные можно разделить на три основных блока
* Данные о покупателях
* Данные о продуктах
* Данные о продажах
### Данные о покупателях
Компания представлена по всей стране. На данный момент открыто 4 региональных представительства, каждое со своим отделом продаж и региональным менеджером: CENTRAL, WEST, EAST, SOUTH. 
В компании нет единого формата отчетности и каждый отдел отдает данные клиентов по своему:
* Отдел CENTRAL: [Одним файлом в формате Excel - CustomerData_Central.xlsx](https://github.com/freemastera/etl-projects/blob/master/pentaho/customers/central/Customers_Central.xlsx)
* Отдел WEST: [В формате csv](https://github.com/freemastera/etl-projects/tree/master/pentaho/customers/west), причем не одним, а несколькими файлами, в разбивке по основным городам
 <img src="/pentaho/img/2.png"> 

* Отдел EAST: [Таблица в Google Sheets](https://docs.google.com/spreadsheets/d/1L94twRun-QpgjgrnpDhYr-BVacnjHJ-EI5IGjV4fy60/edit?usp=sharing)
* Отдел SOUTH: [CSV файл в сжатом виде - zip формат](https://github.com/freemastera/etl-projects/blob/master/pentaho/customers/east/Customers_East.zip)


Общее у них то, что поля передаются одинаковые, но их порядок, формат, а иногда и название может отличаться


|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | customer id  | Уникальный идентификатор клиента  | 
| 2  | customer name | Имя и Фамилия клиента  | 
| 3  | segment | Сегмент  | 
| 4  | country | Страна  | 
| 5  | city  | Город  | 
| 6  | state  | Штат | 
| 7  | postal code  | Почтовый индекс | 
| 8  | region  | Региональное представительство | 

	

### Данные о продуктах
Хранятся в 2 программах. Одна из которых отдает данные в формате JSON, вторая в xml


|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | product_id  | Уникальный идентификатор продукта | 
| 2  | category | Категория  | 
| 3  | sub_category | Подкатегория  | 
| 4  | product_name | Название продукта  | 

	
### Данные о продажах
Хранятся в транзакционной базе данных (OLTP) PostgreSQL, которая находится в облаке на Amazon RDS.

|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | id  | Уникальный идентификатор строки  | 
| 2  | order_id | Идентификатор заказа | 
| 3  | order_date | Дата заказа | 
| 4  | ship_date | Дата доставки | 
| 5  | ship_mode | Класс доставки | 
| 6  | customer_id | Идентификатор клиента | 
| 7  | product_id  | Идентификатор продукта | 
| 8  | sales | Выручка | 
| 9  | quantity | Количество товаров | 
| 10 | discount  | Скидка в процентах | 
| 11  | profit | Прибыль | 

 <img src="/pentaho/img/3.png"> 
 
### Данные о возвратах хранятся в базе MySQL


|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | order_id  | Идентификатор заказа | 
| 2  | returned | Y - был возврат  | 

<img src="/pentaho/img/4.png">   

## ETL - этапы
Cхема (https://miro.com/app/board/o9J_kl_F6p8=/)

<img src="/pentaho/img/5.png"> 

Подробнее о каждом этапе:
### 1. Extraction
Извлечение данных из каждого источника
### 2. Checking 
Проверка, заполняемых отделом продаж данных на наличие обязательных полей: customer_id, customer name
### 3. Merging
Объединение потоков данных

### 4. Cleaning and validation
#### Cleaning
Удаление дубликатов, устранение ошибок, таких как опечатки или ошибки связанные с форматом данных. 
В нашем случае это:
* Поле country, где у каждого отдела содержится свое значение: United States, US,USA,United States of America
* Поле City, где содержатся лишние символы.
Примеры: Chicago$, El Paso_,_Houston, Fort Worth#
* В поле state могут содержатся разного рода опечатки, которые очень сложно предсказать
Примеры: Arisona, Arizon, Cacifornia, Calfornia ,Colorato, и пр. Для исправления будем использовать алгоритм нечеткого поиска и метод расстояние Левенштейна.
* Значение поля discount необходимо перевести в проценты
* Тип даты у полей order_date и ship_date необходимо  привести к единому формату dd-mm-yyyy
#### Validation
Исправление ошибок с точки зрения бизнес логики. 
Например, дата доставки не может быть раньше, чем дата заказа.

### 5. Transformation
Нормализация баз данных. Будем использовать схему звезда

### 6. Loading
Загрузка обработанных данных в хранилище.


## Логическая модель данных
  
<img src="/pentaho/img/6.png"> 

### Таблица dw_calendar 
Данные автоматически сгенерированы с 2000 по 2030 год.

|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | date_id | Уникальный идентификатор строки: yyyymmdd  | 
| 2  | year| Год | 
| 3  | quarter | Квартал | 
| 4  | month | Месяц | 
| 5  | week | Неделя | 
| 6  | date | Дата: yyyy-mm-dd | 
| 7  | weekday  | День недели. Короткое название:sun, mon и.т.д | 
| 8  | leap| Високосный | 

<img src="/pentaho/img/7.png"> 

### Таблица dw_customers


|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | cust_id | Уникальный идентификатор (суррогатный ключ) | 
| 2  | customer_id | Уникальный идентификатор клиента | 
| 3  | customer_name | Имя клиента | 
| 4  | segment | Сегмент | 
| 5  | city | Город| 
| 6  | state | Штат | 
| 7  | country  | Страна | 
| 8  | postal_code| Почтовый индекс | 
| 9  | region| Регион |  

<img src="/pentaho/img/8.png"> 

### Таблица dw_products
Названия и категории продукта хоть и редко, но обновляются. Поэтому были добавлены дополнительные поля, чтобы сохранить и старые данные тоже.

|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | prod_id | Уникальный идентификатор (суррогатный ключ) | 
| 2  | product_id | Уникальный идентификатор продукта | 
| 3  | category | Категория | 
| 4  | sub_category | Подкатегория | 
| 5  | product_name | Название продукта| 
| 6  | start_date | Начальная дата названия | 
| 7  | end_date | Последняя дата, когда продукт был с таким названием | 
| 8  | version | Версия продукта | 
| 9  | current | Y - текущая версия, N - нет |  
| 10  | lastupdate | Дата последнего обновления |  

<img src="/pentaho/img/9.png"> 

### Таблица dw_shipping


|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | order_id | Уникальный идентификатор заказа | 
| 2  | ship_mode | Класс доставки | 
| 3  | returned | Y - был возврат товара | 

<img src="/pentaho/img/10.png"> 	

### Таблица фактов dw_sales

|  | Поле | Значение  | 
| ------------- | ------------- | ------------- | 
| 1  | row_id | Уникальный идентификатор (суррогатный ключ) | 
| 2  | order_id | Идентификатор клиента. FK  к таблице dw_shipping | 
| 3  | order_date | Дата заказа. FK  к таблице dw_calendar | 
| 4  | ship_date | Дата доставки. FK  к таблице dw_calendar | 
| 5  | cust_id | FK  к таблице dw_customers | 
| 6  | prod_id | FK  к таблице dw_products | 
| 7  | sales | Выручка | 
| 8  | quantity| Количество купленных товаров | 
| 9  | discount | Размер скидки |  
| 10  | profit | Прибыль |  

<img src="/pentaho/img/11.png"> 

## Создание таблиц в хранилище по схеме звезда
### Код
```sql
--creating schema
create schema pentaho_dw;
```
```sql
--creating table customers
--DROP table pentaho_dw.customers;
CREATE TABLE pentaho_dw.customers (
        cust_id serial primary key NOT NULL,
        customer_id varchar(20),
        customer_name varchar(30),
        segment varchar(20),
        city varchar(20),
        state varchar(20),
        country varchar(20),
        postal_code varchar(10),
        region varchar(50)
);
```
```sql
--creating table calendar
--DROP table pentaho_dw.calendar;
CREATE TABLE pentaho_dw.calendar
(
date_id serial  NOT NULL,
year        int NOT NULL,
quarter     int NOT NULL,
month       int NOT NULL,
week        int NOT NULL,
date        date NOT NULL,
week_day    varchar(20) NOT NULL,
leap  varchar(20) NOT NULL,
CONSTRAINT PK_calendar PRIMARY KEY ( date_id )
CONSTRAINT date_unique UNIQUE (date)
);
```
```sql
--inserting into table calendar
--TRUNCATE table pentaho_dw.calendar;
insert into pentaho_dw.calendar 
select 
to_char(date,'yyyymmdd')::int as date_id,  
       extract('year' from date)::int as year,
       extract('quarter' from date)::int as quarter,
       extract('month' from date)::int as month,
       extract('week' from date)::int as week,
       date::date,
       to_char(date, 'dy') as week_day,
       extract('day' from
               (date + interval '2 month - 1 day')
              ) = 29
       as leap
  from generate_series(date '2000-01-01',
                       date '2030-01-01',
                       interval '1 day')
       as t(date);
```
```sql
--creating table products
--DROP table pentaho_dw.products;           
CREATE TABLE pentaho_dw.products (
        prod_id serial primary key NOT NULL,
        product_id varchar (20) NOT NULL DEFAULT 'N/A',
        category varchar(15) NOT NULL DEFAULT 'N/A',
        sub_category varchar(20) NOT NULL DEFAULT 'N/A',
        product_name varchar(255) NOT NULL DEFAULT 'N/A',
        start_date date,
        end_date date,
        "version" smallint NOT NULL DEFAULT 1,
        "current" varchar(3) NOT NULL DEFAULT 'Y',
        lastupdate date
);
```
```sql
--creating table shipping
--DROP table pentaho_dw.shipping;           
CREATE TABLE pentaho_dw.shipping (
        order_id varchar (25) primary key NOT NULL,
        ship_mode varchar (25) NOT NULL DEFAULT 'N/A',
        returned varchar (1)
);
```
```sql
--creating fact table sales
--DROP TABLE pentaho_dw.sales;
CREATE TABLE pentaho_dw.sales (
        row_id serial PRIMARY KEY NOT NULL,
        order_id varchar (25) NOT NULL REFERENCES pentaho_dw.shipping (order_id),
        order_date date NOT NULL REFERENCES pentaho_dw.calendar (date),
        ship_date date REFERENCES pentaho_dw.calendar (date),
        cust_id serial NOT NULL REFERENCES pentaho_dw.customers (cust_id),
        prod_id serial NOT NULL REFERENCES pentaho_dw.products (prod_id),
        sales numeric NOT NULL DEFAULT '0',
        quantity int NOT NULL DEFAULT '0',
        discount numeric NOT NULL DEFAULT '0',
        profit numeric NOT NULL DEFAULT '0'
);
```

## ETL в Pentaho
У нас будет 3 transformation и 3 job:

### Transformations:

#### 1. Products_transformation
  
<img src="/pentaho/img/12.png"> 

Сначала извлекаются данные о продуктах. Часть этих данных хранится в формате json, другая в xml. Затем все это сводится в один файл, сортируются по product_id и удаляются дубликаты. Затем данные выгружаются в хранилище (postgres)
  
<img src="/pentaho/img/13.png"> 

Вставляются новые данные по продуктам product_id, если есть. Также обновляется дата последнего апдейта. 
Если данные о продукте изменились, то старые данные сохраняются. Добавляется новая версия и даты изменений. В результате, при желании, можно будет увидеть старое название продукта или раздел в котором он ранее был.


#### 2. Customers_transformation
  
<img src="/pentaho/img/14.jpg"> 

На этом шаге извлекается, обрабатывается и загружается в хранилище информация о покупателях.
Четыре региона со своими отделами продаж. Формат у всех тоже отличается. 
* Отдел CENTRAL: Одним файлом в формате Excel - CustomerData_East.xlsx
* Отдел WEST: В формате csv, причем не одним, а несколькими файлами, в разбивке по основным городам
<img src="/pentaho/img/16.png"> 

* Отдел EAST: Таблица в Google Sheets
* Отдел SOUTH: CSV файл в сжатом виде - zip формат.

Т.к часть данных заполняется вручную отделом продаж, то именно на этом шаге больше всего потенциальных ошибок и незаполненных обязательных полей.
Сначала извлекаются данные по покупателям от каждого отдела. Сразу происходит проверка на наличие пропущенных обязательных полей. В нашем случае это customer_id или customer_name. В случае, если строки с этими пропущенными полями будут обнаружены, они будут отфильтрованы и выложены в отдельную папку(у каждого отдела своя) на сервере в формате excel. Этот файл потом будет отправлен на почту соответствующему отделу, с просьбой дозаполнить недостающие данные.

Строки с заполненными данными идут дальше, объединяются и приводятся к единому формату. 
Далее идет очистка данных. Сначала удаляются дубли.
Затем, значение поля country приводится к общему виду. Как мы ранее выяснили оно может отличаться, в зависимости от отдела.

<img src="/pentaho/img/17.png">

Потом удаляются лишние символы из поля city

<img src="/pentaho/img/18.png"> 

На следующем шаге исправляются опечатки в названии штата. Их очень много и всех их не предугадаешь. Например, у customer_id EB-13705 Cacifornia вместо California.

<img src="/pentaho/img/19.png"> 

Но у нас есть файл с правильными названиями штатов и используя алгоритм  Левенштейна, мы можем решить эту задачу.

<img src="/pentaho/img/20.png"> 

<img src="/pentaho/img/21.png"> 

Далее идет выгрузка новых значений в БД postgres с созданием суррогатного ключа cust_id. И последним шагом обновление данных.


#### 3. Sales_transformation

<img src="/pentaho/img/22.jpg"> 
  

Здесь извлекаются данные о продажах, которые хранятся в облаке amazon rds 
  
<img src="/pentaho/img/23.png"> 

И данные о возвратах товара. Хранятся в БД MySQL

<img src="/pentaho/img/24.png">   

Затем эти данные объединяются через right join.

<img src="/pentaho/img/25.png"> 

Поле discount преобразуется в проценты. Поля с датами(order_date,ship_date) приводятся к формату yyyymmdd.


Проверка на ошибки
У нас есть файл в формате csv с названиями товаров. Мы делаем проверку, что каждого product_id было название, иначе выводит ошибку в лог.
<img src="/pentaho/img/26.png">   

Затем мы делаем проверку на ошибку с точки зрения бизнес логики. А именно дата доставки, не может быть раньше, чем дата заказа. 
Добавляем новое поле number of days

<img src="/pentaho/img/27.png"> 

Если значение этого поля отрицательное, то выгружаем такие строки в xls и отправляем в отдел продаж с просьбой актуализировать информацию.


Когда данные очищены и обработаны, то выгружаем поля order_id,ship_mode,returned в таблицу dw_shipping.
Загружаем необходимые поля в таблицу фактов dw_sales. До выгрузки достаем суррогатные ключи cust_id и prod_id из таблиц dw_customers и  dw_products. Эти поля в таблице фактов у нас будут FK к ним
<img src="/pentaho/img/28.png">   

### Три job:
#### 1. main_job. Основной job, который запускает все три ранее сделанных трансформации. Запускается через крон (или планировщик задач в windows) в 4 часа утра. 
Создается исполняемый файл sheduling_job.bat с командой вида
```
"C:\pdi-ce-9.0.0.0-423\data-integration\kitchen.bat" /file:"C:\Users\User\Desktop\Pentaho\MainJob.kjb" /level:Basic”
```
 <img src="/pentaho/img/29.png">    



#### 2. check_customer
Запускается в 5 часов утра. Проверяет наличие файлов с ошибками, где необходима помощь коллег из отдела продаж. 
* строки с пустыми значениями customer_id, customer name (customers_transformation). 
* строки, где дата доставки раньше , чем дата заказа (sales_transformation). 
В случае если файла нет, то  ничего не происходит, если файл есть он отправляется на почту в соответствующий этому региону отдел продаж. В копии все заинтересованные лица.
Сами файлы после этого перемещаются в другую папку на сервере, где хранится вся история. К названию файла добавляется текущая дата.
В самом письме указывается папка на сервере, куда положить исправленные строки. У каждого отдела своя папка и доступ только к ней.
<img src="/pentaho/img/30.png">    

Письма в почтовом клиенте в случае обнаружения ошибок
  
<img src="/pentaho/img/31.jpg">   
  
<img src="/pentaho/img/32.jpg">   


#### 3. check_corrections
Запускается в 10 часов утра. 
Проверяет наличие исправленных файлов. Если их нет, то ничего не происходит. Если есть, то прогоняет эти строки, аналогично main_job.
В конце отправляется письмо всем заинтересованным лицам.
