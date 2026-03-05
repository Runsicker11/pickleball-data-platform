-- FBA fulfilled shipments from SP-API Reports API
-- (GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL)
-- One row per shipped item — includes buyer info, carrier, tracking

with source as (
    select * from {{ source('raw_amazon', 'seller_fba_shipments') }}
),

deduplicated as (
    select
        amazon_order_id,
        sku,
        shipment_id,
        shipment_item_id,
        buyer_name,
        buyer_email,
        carrier,
        tracking_number,
        fulfillment_center_id,
        cast(quantity_shipped as int64) as quantity_shipped,
        cast(item_price as numeric) as item_price,
        ship_address_1 as ship_address,
        ship_city,
        ship_state,
        ship_postal_code,
        ship_country,
        cast(shipment_date as timestamp) as shipment_date,
        cast(estimated_arrival_date as timestamp) as estimated_arrival_date,
        _loaded_at
    from source
    qualify row_number() over(
        partition by amazon_order_id, sku, shipment_id, shipment_item_id
        order by _loaded_at desc
    ) = 1
)

select * from deduplicated
