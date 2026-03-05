-- Sales & Traffic by ASIN from SP-API Reports API
-- (GET_SALES_AND_TRAFFIC_REPORT)
-- One row per child ASIN per report date — sessions, page views, buy box %

with source as (
    select * from {{ source('raw_amazon', 'seller_traffic') }}
),

deduplicated as (
    select
        child_asin,
        parent_asin,
        cast(report_date as date) as report_date,
        cast(units_ordered as int64) as units_ordered,
        cast(units_ordered_b2b as int64) as units_ordered_b2b,
        cast(total_order_items as int64) as total_order_items,
        cast(ordered_product_sales_amount as numeric) as ordered_product_sales_amount,
        ordered_product_sales_currency,
        cast(sessions as int64) as sessions,
        cast(page_views as int64) as page_views,
        cast(buy_box_percentage as numeric) as buy_box_percentage,
        cast(unit_session_percentage as numeric) as unit_session_percentage,
        _loaded_at
    from source
    qualify row_number() over(
        partition by child_asin, report_date
        order by _loaded_at desc
    ) = 1
)

select * from deduplicated
