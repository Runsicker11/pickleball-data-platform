with source as (
    select * from {{ source('amazon_orders', 'amazon_airbyteGET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL') }}
),

prep as (
    select distinct
        sku,
        asin,
        currency,
        cast(case when quantity = '' then '0' else quantity end as numeric) as quantity,
        cast(case when item_price = '' then '0' else item_price end as numeric) as item_price,
        item_status,
        order_status,
        ship_country,
        sales_channel,
        datetime(cast(purchase_date as timestamp), 'US/Pacific') as purchase_date_pac,
        datetime(cast(purchase_date as timestamp)) as purchase_date,
        amazon_order_id,
        ship_service_level,
        cast(case when item_promotion_discount = '' then '0' else item_promotion_discount end as numeric) as item_promotion_discount
    from source
    where order_status != 'Cancelled'
        and sales_channel != 'Non-Amazon'
        and item_status != 'Cancelled'
),

conversion as (
    select
        *,
        case
            when currency = 'MXN' then item_price * 0.054
            when currency = 'CAD' then item_price * 0.72
            else item_price
        end as corrected_item_price,
        case
            when currency = 'MXN' then item_promotion_discount * 0.054
            when currency = 'CAD' then item_promotion_discount * 0.72
            else item_promotion_discount
        end as corrected_item_promotion_discount
    from prep
)

select
    *,
    corrected_item_price - corrected_item_promotion_discount as sales
from conversion
