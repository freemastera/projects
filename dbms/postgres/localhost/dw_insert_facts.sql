insert into dw.facts (id,sales,quantity,discount,profit,customer_id,order_id,manager_id,prod_id,order_date)
select
100+row_number() over (),
sales,
quantity,
discount,
profit,
customer_id,
order_id,
manager_id,
prod_id,
order_date
from 
(SELECT distinct
sales,
quantity,
discount,
profit,
c.customer_id,
ord.order_id,
m.manager_id,
p.prod_id,
d.order_date
from stg.orders o inner join dw.dim_customers c on o.customer_id = c.customer_id
join dw.dim_dates d on o.order_date = d.order_date
join dw.dim_managers m on o.region = m.region
join dw.dim_products p on o.product_id = p.product_id and p.product_name= o.product_name 
join dw.dim_orders ord on o.order_id = ord.order_id) a