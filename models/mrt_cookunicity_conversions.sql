{% set columns_to_sum =
    [
        'interest', 
        'impressions',
        'clicks',
        'spent',
        'conversions',
        'approved_conversions'
    ]
%}

with conversion_metrics as (
    
    select 

        ad_id,
        xyz_campaign_id,
        age,
        gender,

        {%- for columns in columns_to_sum %}
        
            coalesce(
                sum({{ columns }}),
                0
            ) as total_{{ columns }},
            
        {% endfor %}

        {{ calculate_metric('total_clicks', 'total_impressions') }} as ctr,
        {{ calculate_metric('total_spent', 'total_clicks') }} as cpc,
        {{ calculate_metric('total_spent', 'total_impressions') }} as cpm,
        {{ calculate_metric('total_spent', 'total_conversions') }} as cpa,
        {{ calculate_metric('total_conversions', 'total_clicks') }} as conversion_rate_clicks,
        {{ calculate_metric('total_approved_conversions', 'total_clicks') }} as approved_conversion_rate_clicks,
        {{ calculate_metric('total_approved_conversions', 'total_conversions') }} as approved_conversion_rate_orders,
        {{ calculate_metric('total_spent', 'total_approved_conversions') }} as cost_per_approved_conversion

        
    from {{ ref('stg_cookunicity_conversions') }}
    {{ dbt_utils.group_by(n=4) }}

)

select * from conversion_metrics
