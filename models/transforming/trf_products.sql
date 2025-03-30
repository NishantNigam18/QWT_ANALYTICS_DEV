{{config(materialized= 'table', schema= 'transforming_dev')}}

select
p.productid,
p.productname,
s.companyname as suppliercompany,
s.contactname as suppliercontact,
s.city as suppliercity,
s.country as suppliercountry,
c.categoryname,
p.quantityperunit,
p.unitcost,
p.unitprice,
p.unitsinstock,
p.unitsonorder,
IFF(P.unitsonorder>P.unitsinstock,'Not Available','Available') as StockAvailability,

from  {{ref('stg_products')}} as p
inner join {{ref('trf_suppliers')}} as s on p.SupplierID=s.SupplierID
inner join {{ref('lkp_categories')}} as c on p.categoryid=c.categoryid