-- with
-- stg_account_billing as (
-- select * from {{ source("visionflix", "account_billing") }}
-- ),
-- stg_plans as (select * from {{ source("visionflix", "plans") }}),
-- stg_dim_date as (select * from {{ source("visionmart", "DateDimension") }})
-- select
-- {{ dbt_utils.generate_surrogate_key(["a.ab_id"]) }} as fact_overall_sales_key,
-- a.ab_account_id as customer_key,
-- a.ab_plan_id as plan_key,
-- month,
-- year,
-- sum((p.plan_price * p.discount) / 100) as visionflix_total_discount,
-- sum((p.plan_price - p.discount)) as visionflix_total_amount
-- from stg_account_billing a
-- inner join stg_plans p on a.ab_plan_id = p.plan_id
-- inner join stg_dim_date d on a.ab_date = d.datetime
-- group by fact_overall_sales_key, customer_key, plan_key, month, year
-- order by customer_key, month, year

with
    stg_order_details as (select * from {{ source("visionmart", "order_details") }}),
    stg_customers as (select * from {{ source("visionmart", "customers") }}),
    stg_dim_date as (select * from {{ source("visionmart", "DateDimension") }}),
    stg_products as (select * from {{ source("visionmart", "products") }}),
    stg_orders as (select * from {{ source("visionmart", "orders") }})
select
    {{ dbt_utils.generate_surrogate_key(["c.customer_id"]) }}
    as fact_customer_order_per_day_key,
    c.customer_id as customer_key,
    month,
    year,
    sum(p.product_retail_price * od.order_qty) as visionmart_total_amount
from stg_orders o
inner join stg_order_details od on o.order_id = od.order_id
inner join stg_dim_date d on o.order_date = d.date
inner join stg_products p on p.product_id = od.product_id
inner join stg_customers c on c.customer_id = o.customer_id
group by customer_key, month, year
order by customer_key, month, year