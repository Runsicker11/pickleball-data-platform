with orders_clean as (
    select * from {{ ref('stg_amazon__orders_clean') }}
),

aggregated as (
    select
        date_trunc(purchase_date, day) as purchase_date_utc,
        asin,
        sum(quantity) as quantity,
        sum(sales) as sales,
        sum(corrected_item_price) as item_price,
        sum(corrected_item_promotion_discount) as item_promotion_discount
    from orders_clean
    group by 1, 2
)

select * from aggregated
