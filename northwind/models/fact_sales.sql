with stg_orders as 
(
    select
        orderid,  
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['orderid']) }} as orderkey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey,

    from {{source('northwind','Orders')}}
),
stg_order_details as
(
    select 
        orderid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        sum(Quantity) as quantity, 
        sum(Quantity*UnitPrice) as extendedpriceamount,
        sum(Quantity*UnitPrice*Discount) as discountamount,
        sum(Quantity*UnitPrice*(1-Discount)) as soldamount
    from {{source('northwind','Order_Details')}}
    group by productkey, orderid
)
select
    o.employeekey,
    o.customerkey,
    o.orderkey,
    d.productkey,
    o.orderdatekey,
    d.quantity,
    d.extendedpriceamount,
    d.discountamount,
    d.soldamount
from
    stg_orders o join stg_order_details d on o.orderid = d.orderid