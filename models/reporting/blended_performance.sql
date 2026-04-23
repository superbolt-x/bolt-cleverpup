{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH sho_data AS (
    SELECT 
        'Shopify' as channel,
        '(not set)' as campaign_name,
        date,
        date_granularity,
        0 as spend,
        0 as clicks,
        0 as impressions,
        0 as paid_purchases,
        0 as paid_revenue, 
        first_orders as shopify_first_orders, 
        orders as shopify_orders, 
        first_order_total_net_sales as shopify_first_sales, 
        total_net_sales as shopify_sales,
        first_order_net_sales as shopify_first_net_sales,
        net_sales as shopify_net_sales,
        0 as sessions,
        0 as engaged_sessions,
        0 as ga4_purchases,
        0 as ga4_revenue 
    FROM {{ source('reporting','shopify_sales') }}
),
    
paid_data as
    (SELECT channel, campaign_id::varchar as campaign_id, campaign_name, date::date, date_granularity, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, 
        COALESCE(SUM(impressions),0) as impressions, COALESCE(SUM(paid_purchases),0) as paid_purchases, COALESCE(SUM(paid_revenue),0) as paid_revenue
    FROM
        (SELECT 'Meta' as channel, campaign_id::varchar as campaign_id, campaign_name, date, date_granularity, 
            spend, link_clicks as clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','facebook_campaign_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, campaign_id::varchar as campaign_id, campaign_name, date, date_granularity,
            spend, clicks, impressions, purchases as paid_purchases, revenue as paid_revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        )
    GROUP BY channel, campaign_id, campaign_name, date, date_granularity),

ga4_data as 
    (SELECT campaign_id::varchar as campaign_id, date, date_granularity, 
    sum(sessions) as sessions, sum(engaged_sessions) as engaged_sessions, sum(purchase) as ga4_purchases, sum(purchase_value) as ga4_revenue
    FROM {{ source('reporting','ga4_campaign_performance_session') }}
    GROUP BY 1,2,3),

paid_ga4_data as (
  SELECT 
    case when campaign_name is null then 'Not Paid' else channel end as channel, campaign_name, date::date, date_granularity,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(clicks, 0)) AS clicks,
    SUM(COALESCE(impressions, 0)) AS impressions,
    SUM(COALESCE(paid_purchases, 0)) AS paid_purchases,
    SUM(COALESCE(paid_revenue, 0)) AS paid_revenue,
    SUM(0) AS shopify_first_orders,
    SUM(0) AS shopify_orders,
    SUM(0) AS shopify_first_sales,
    SUM(0) AS shopify_sales,
    SUM(0) AS shopify_first_net_sales,
    SUM(0) AS shopify_net_sales,
    SUM(COALESCE(sessions, 0)) AS sessions,
    SUM(COALESCE(engaged_sessions, 0)) AS engaged_sessions,
    SUM(COALESCE(ga4_purchases, 0)) AS ga4_purchases,
    SUM(COALESCE(ga4_revenue, 0)) AS ga4_revenue
  FROM paid_data FULL OUTER JOIN ga4_data USING(date,date_granularity,campaign_id)
  GROUP BY 1,2,3,4)
    
SELECT 
    channel,
    campaign_name,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    paid_purchases,
    paid_revenue,
    shopify_first_orders,
    shopify_orders,
    shopify_first_sales,
    shopify_sales,
    shopify_first_net_sales,
    shopify_net_sales,
    sessions,
    engaged_sessions,
    ga4_purchases,
    ga4_revenue 
FROM (
    SELECT * FROM paid_ga4_data
    UNION ALL 
    SELECT * FROM sho_data
)
