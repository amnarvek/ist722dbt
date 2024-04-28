 WITH stg_publishers AS (
          SELECT * FROM {{ source('visionbooks', 'publishers') }}
      )
      SELECT 
          {{ dbt_utils.generate_surrogate_key(['stg_publishers.pub_id']) }} AS publisherkey, 
          PUB_ID,
          PUB_NAME,
          CITY,
          STATE,
          COUNTRY
      FROM stg_publishers