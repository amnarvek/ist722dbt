with stg_title as (select * from {{ source("VISIONBOOKS", "TITLES") }})
select
    {{ dbt_utils.generate_surrogate_key(["stg_title.title_id"]) }} as publisherkey,
    title_id,
    title,
    price,
    advance,
    royalty,
    ytd_sales,
    notes,
    pubtime,
    pub_id
from stg_title
