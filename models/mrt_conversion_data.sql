with conversion_metrics as (
    
    select 

        AD_ID,
        XYZ_CAMPAIGN_ID,
        AGE,
        GENDER,
        sum(INTEREST) as total_interest,
        sum(IMPRESSIONS) as total_impressions,
        sum(CLICKS) as total_clicks,
        sum(SPENT) as total_spent,
        sum(TOTAL_CONVERSION) as total_conversions,
        sum(APPROVED_CONVERSION) as total_approved_conversion,

        {{ dbt_utils.safe_divide('total_clicks', 'total_impressions') }} * 100 as ctr,

        {{ dbt_utils.safe_divide('total_spent', 'total_clicks') }} * 100 as cpc,

        {{ dbt_utils.safe_divide('total_spent', 'total_impressions') }} * 100 as cpm,

        {{ dbt_utils.safe_divide('total_spent', 'total_conversions') }} * 100 as cpa,

        {{ dbt_utils.safe_divide('total_conversions', 'total_clicks') }} * 100 as conversion_rate_clicks,

        {{ dbt_utils.safe_divide('total_approved_conversion', 'total_clicks') }} * 100 as approved_conversion_rate_clicks,

        {{ dbt_utils.safe_divide('total_approved_conversion', 'total_conversions') }} * 100 as approved_conversion_rate_orders,

        {{ dbt_utils.safe_divide('total_spent', 'total_approved_conversion') }} * 100 as cost_per_approved_conversion
        
    from {{ ref('cook_unity_conversion_data') }}
    {{ dbt_utils.group_by(n=4) }}

)

select * from conversion_metrics
