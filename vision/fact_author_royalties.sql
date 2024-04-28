WITH stg_titleauthors AS (
          SELECT * FROM {{ source('VISIONBOOKS', 'TITLEAUTHORS') }}
      ),
stg_authors as 
(
    select *
        
        
    from {{ source('VISIONBOOKS','AUTHORS') }}
),
stg_title as
(
    select *
       
    from {{ source('VISIONBOOKS', 'TITLES') }}
),
stg as (
select ta.AU_ID,
sum(t.ROYALTY) as royalty_sum
from 
    stg_title t
join stg_titleauthors ta on ta.TITLE_ID=t.TITLE_ID
join stg_authors a on ta.AU_ID=a.AU_ID
group by ta.AU_ID
)

select royalty_sum,AU_ID
from stg s


