with stg_authors as 
(
    select *
        
        
    from {{ source('VISIONBOOKS','AUTHORS') }}
),
stg_publishers as 
(
    select *
       
    from {{ source('VISIONBOOKS','PUBLISHERS') }}
),
stg_title as
(
    select *
       
    from {{ source('VISIONBOOKS', 'TITLES') }}
)
,
stg_sales as 
(

select *
from {{source('VISIONBOOKS', 'SALES')}}


),
stg_discounts as 
(

select *
from {{source('VISIONBOOKS', 'DISCOUNTS')}}


)

select 
   s.SALES_ID,s.QTY*t.PRICE as sales_amount,QTY,s.QTY*t.PRICE*d.DISCOUNT as discount_amount,s.QTY*t.PRICE*(1-d.DISCOUNT) as sold_amount,s.QTY*t.PRICE*t.ADVANCE as advance_amount
from 
    stg_title t
join 
    stg_publishers p on t.PUB_ID = p.PUB_ID
join 
    stg_sales s on s.TITLE_ID=t.TITLE_ID
join 
    stg_discounts d on s.SALES_ID = d.SALES_ID