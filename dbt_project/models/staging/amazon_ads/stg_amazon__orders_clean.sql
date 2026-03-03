-- Orders from Amazon SP-API Reports API
-- (GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL)
-- One row per order line item — same format as the old Airbyte flat file

with source as (
    select * from {{ source('raw_amazon', 'seller_orders') }}
),

prep as (
    select distinct
        sku,
        asin,
        currency,
        cast(coalesce(quantity, '0') as numeric) as quantity,
        cast(coalesce(item_price, '0') as numeric) as item_price,
        item_status,
        order_status,
        ship_country,
        sales_channel,
        datetime(cast(purchase_date as timestamp), 'US/Pacific') as purchase_date_pac,
        datetime(cast(purchase_date as timestamp)) as purchase_date,
        amazon_order_id,
        ship_service_level,
        cast(coalesce(item_promotion_discount, '0') as numeric) as item_promotion_discount
    from source
    where order_status != 'Cancelled'
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
