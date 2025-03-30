{{config(materialized = 'view', schema = 'reporting_dev')}}
 
{% set order_linenumbers = get_order_linenos() %}
 
select
orderid,
 
{% for linenumber in order_linenumbers %}
 
sum(case when lineno = {{linenumber}} then linesalesamount end ) as lineno{{linenumber}}_Sales,
 
{% endfor %}
 
sum(linesalesamount) as total_sales
 
from {{ref('fct_orders')}}
 
group by orderid
 