{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue,
"offsite_conversion.custom.1855785309156285" as "Purchase - Nomad Collapsible Dog Bowl Set of 2",
"offsite_conversion.custom.1138067731815751" as "Purchase - Nomad Collapsible Dog Bowl",
"offsite_conversion.custom.894757369564418" as "Purchase - Porter Dog Gear Backpack",
"offsite_conversion.custom.1930870874974906" as "Purchase - Park Pack Dog Gear Tote Set",
"offsite_conversion.custom.1548421176294960" as "Purchase - Snackpack Double Insulated Treat Pouch",
"offsite_conversion.custom.812576248225988" as "Purchase - Bistro Box Dog Food Travel Kit",
"offsite_conversion.custom.1403809167981593" as "Purchase - Dog Walk & Train Sling Bag",
"offsite_conversion.custom.1363249288830439" as "Purchase - Ultimate Dog Travel Bundle",
"offsite_conversion.custom.611619652040063" as "Purchase - Dog Walk & Train Bundle",
"offsite_conversion.custom.1855785309156285_value" as "Revenue - Nomad Collapsible Dog Bowl Set of 2",
"offsite_conversion.custom.1138067731815751_value" as "Revenue - Nomad Collapsible Dog Bowl",
"offsite_conversion.custom.894757369564418_value" as "Revenue - Porter Dog Gear Backpack",
"offsite_conversion.custom.1930870874974906_value" as "Revenue - Park Pack Dog Gear Tote Set",
"offsite_conversion.custom.1548421176294960_value" as "Revenue - Snackpack Double Insulated Treat Pouch",
"offsite_conversion.custom.812576248225988_value" as "Revenue - Bistro Box Dog Food Travel Kit",
"offsite_conversion.custom.1403809167981593_value" as "Revenue - Dog Walk & Train Sling Bag",
"offsite_conversion.custom.1363249288830439_value" as "Revenue - Ultimate Dog Travel Bundle",
"offsite_conversion.custom.611619652040063_value" as "Revenue - Dog Walk & Train Bundle"
FROM {{ ref('facebook_performance_by_campaign') }}
