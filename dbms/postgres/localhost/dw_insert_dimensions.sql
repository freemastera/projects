insert into dw.dim_customers (customer_id,customer_name,segment)
select customer_id,customer_name,segment from (select distinct customer_id,customer_name,segment from stg.orders) a;


insert into dw.dim_dates(order_date)
select distinct order_date from stg.orders;

insert into dw.dim_managers(manager_id, person,region)
select 100+row_number() over (), person,region from (select distinct person,region from stg.people) a;


insert into dw.dim_orders(order_id, ship_mode,ship_date,country,state,city,postal_code,returned)
select order_id, ship_mode,ship_date,country,state,city,postal_code,returned from (select distinct orders.order_id, ship_mode,ship_date,country,state,city,postal_code,returned from stg.orders left join stg.returns on stg.returns.order_id=orders.order_id) a;


insert into dw.dim_products (prod_id, product_id,product_name,category,sub_category)
select 100+row_number() over (),product_id,product_name,category,subcategory from (select distinct product_id,product_name,category,subcategory from stg.orders) a;