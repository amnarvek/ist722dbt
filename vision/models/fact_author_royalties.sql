with
    stg_titleauthors as (select * from {{ source("VISIONBOOKS", "TITLEAUTHORS") }}),
    stg_authors as (select * from {{ source("VISIONBOOKS", "AUTHORS") }}),
    stg_title as (select * from {{ source("VISIONBOOKS", "TITLES") }}),
    stg as (
        select ta.au_id, sum(t.royalty) as royalty_sum
        from stg_title t
        join stg_titleauthors ta on ta.title_id = t.title_id
        join stg_authors a on ta.au_id = a.au_id
        group by ta.au_id
    )

select royalty_sum, au_id
from stg s
