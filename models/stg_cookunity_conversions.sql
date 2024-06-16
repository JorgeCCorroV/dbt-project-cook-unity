with stg as (
    
    select 

        ad_id,
        xyz_campaign_id,
        age,
        gender,
        interest,
        impressions,
        clicks,
        spent,
        total_conversion as conversions,
        approved_conversion as approved_conversions

    from {{ source('production', 'conversion_data') }}

)

select * from stg
