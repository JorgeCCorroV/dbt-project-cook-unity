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

{%- set conversion_metrics = [
    {'name': 'ctr', 'numerator': 'total_clicks', 'denominator': 'total_impressions'},
    {'name': 'cpc', 'numerator': 'total_spent', 'denominator': 'total_clicks'},
    {'name': 'cpm', 'numerator': 'total_spent', 'denominator': 'total_impressions'},
    {'name': 'cpa', 'numerator': 'total_spent', 'denominator': 'total_conversions'},
    {'name': 'conversion_rate_clicks', 'numerator': 'total_conversions', 'denominator': 'total_clicks'},
    {'name': 'approved_conversion_rate_clicks', 'numerator': 'total_approved_conversions', 'denominator': 'total_clicks'},
    {'name': 'approved_conversion_rate_orders', 'numerator': 'total_approved_conversions', 'denominator': 'total_conversions'},
    {'name': 'cost_per_approved_conversion', 'numerator': 'total_spent', 'denominator': 'total_approved_conversions'}
] -%}

with conversion_metrics as (
    
    select 

        ad_id,
        xyz_campaign_id,
        age,
        gender

        {%- for columns in columns_to_sum %}
        
            ,coalesce(
                sum({{ columns }}),
                0
            ) as total_{{ columns }}
            
        {% endfor %}

        {% for metric in conversion_metrics %}
            
            ,{{ calculate_metric(metric.numerator, metric.denominator) }} as {{ metric.name }}

        {% endfor %}

        
    from {{ ref('stg_cookunicity_conversions') }}
    {{ dbt_utils.group_by(n=4) }}

)

select * from conversion_metrics
