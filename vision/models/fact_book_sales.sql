with
    stg_title as (select * from {{ source("VISIONBOOKS", "TITLES") }}),
    stg_sales as (select * from {{ source("VISIONBOOKS", "SALES") }}),
    stg_readers as (select * from {{ source("VISIONBOOKS", "ReaderSales") }})
select
    r.reader_id,
    extract(month from s.ord_date) as month,
    extract(year from s.ord_date) as year,
    sum(s.qty) as total_quantity,
    sum(s.qty * t.price) as total_amount
from stg_title t
join stg_sales s on s.title_id = t.title_id
join stg_readers r on r.sales_id = s.sales_id
group by r.reader_id, month, year
