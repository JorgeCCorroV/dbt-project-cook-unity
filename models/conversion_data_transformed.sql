with total_conversion_transformed as (
    select 

        AD_ID,
        XYZ_CAMPAIGN_ID,
        AGE,
        GENDER,
        sum(INTEREST) as interest,
        sum(IMPRESSIONS) as impressions,
        sum(CLICKS) as clicks,
        sum(SPENT) as spent,
        sum(TOTAL_CONVERSION) as total_conversion,
        sum(APPROVED_CONVERSION) as approved_conversion,
        case when sum(impressions) = 0 then 0 else (sum(clicks) / sum(impressions)) * 100 end as ctr,
        case when sum(clicks) = 0 then 0 else sum(spent) / sum(clicks) end as cpc,
        case when sum(impressions) = 0 then 0 else (sum(spent) / sum(impressions)) * 1000 end as cpm,
        case when sum(total_conversion) = 0 then 0 else (sum(spent) / sum(total_conversion)) * 100 end as cpa,
        case when sum(clicks) = 0 then 0 else (sum(total_conversion) / sum(clicks)) * 100 end as conversion_rate,
        case when sum(total_conversion) = 0 then 0 else (sum(approved_conversion) / sum(total_conversion)) * 100 end as approved_conversion_rate,
        case when sum(approved_conversion) = 0 then 0 else (sum(spent) / sum(approved_conversion)) * 100 end as cost_per_approved_conversion
        
    from {{ ref('stg_conversion_data') }}
    group by 1,2,3,4
)

select * from total_conversion_transformed
