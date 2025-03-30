{{config(materialized= 'table',schema= 'transforming_dev')}}
 
select
 
c.customerid,
c.companyname,
c.contactname,
c.city,
c.country,
d.divisionname,
c.address,
c.fax,
c.phone,
c.postalcode,
IFF(c.stateprovince='', 'NA',c.stateprovince) as statename
 
from
 
{{ ref('stg_customers') }} as c
 
left join {{ref('lkp_divisions')}} as d on
c.divisionid = d.divisionid