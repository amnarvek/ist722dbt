select 
    {{ dbt_utils.generate_surrogate_key(['p.productid']) }} 
       as productkey, 
    p.productid,
    p.productname,
    p.supplierid,
    c.categoryname,
    c.description as categorydescription
from {{ source('northwind','Products')}} p
    inner join {{ source('northwind','Categories')}} c on p.categoryid = c.categoryid
