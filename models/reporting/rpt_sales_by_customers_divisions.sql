{{config(materialized = 'view', schema = 'reporting_dev')}}

select
c.divisionname,
c.companyname,
c.contactname,
c.city,
c.country,
count(distinct o.orderid) as total_orders,
count(distinct o.productid) as total_products,
sum(o.quantity) as total_quantity,
sum(o.linesalesamount) as total_sales,
avg(o.margin) as avg_margin
from
{{ref('fct_orders')}} as o inner join 
{{ref('dim_customers')}} as c on o.customerid = c.customerid
where c.divisionname = '{{ var('v_division', 'Europe')}}'  
group by c.divisionname, c.companyname, c.contactname, c.city, c.country