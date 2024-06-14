{% set columns_to_sum =
    [
        'INTEREST', 
        'IMPRESSIONS',
        'CLICKS',
        'SPENT',
        'TOTAL_CONVERSION',
        'APPROVED_CONVERSION'
    ]
%}

with conversion_metrics as (
    
    select 

        AD_ID,
        XYZ_CAMPAIGN_ID,
        AGE,
        GENDER

        {%- for columns in columns_to_sum %}
        
            ,coalesce(
                sum({{ columns }}),
                0
            ) as total_{{ columns }}
            
        {% endfor %}


        
    from {{ ref('cook_unity_conversion_data') }}
    {{ dbt_utils.group_by(n=4) }}

)

select * from conversion_metrics
