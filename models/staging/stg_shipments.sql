{{config(materialized= 'table')}}

select 
orderid,
lineno,
shipperid,
customerid,
productid,
employeeid,
SPLIT_PART(shipmentdate,' ',1):: date as shipmentdate,
status
from 
{{source('qwt_raw', 'raw_shipments')}}