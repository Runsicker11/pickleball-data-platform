-- FBA fee estimates per SKU from SP-API Reports API
-- (GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA)
-- One row per SKU/ASIN — referral, pick-pack, fulfillment fee breakdown

with source as (
    select * from {{ source('raw_amazon', 'seller_fba_fees') }}
),

deduplicated as (
    select
        sku,
        asin,
        product_name,
        product_group,
        brand,
        fulfilled_by,
        product_size_tier,
        cast(your_price as numeric) as your_price,
        cast(sales_price as numeric) as sales_price,
        cast(estimated_fee_total as numeric) as estimated_fee_total,
        cast(estimated_referral_fee_per_unit as numeric) as estimated_referral_fee_per_unit,
        cast(estimated_variable_closing_fee as numeric) as estimated_variable_closing_fee,
        cast(estimated_order_handling_fee_per_order as numeric) as estimated_order_handling_fee,
        cast(estimated_pick_pack_fee_per_unit as numeric) as estimated_pick_pack_fee_per_unit,
        cast(estimated_weight_handling_fee_per_unit as numeric) as estimated_weight_handling_fee,
        cast(expected_fulfillment_fee_per_unit as numeric) as expected_fulfillment_fee_per_unit,
        currency,
        _loaded_at
    from source
    qualify row_number() over(
        partition by sku, asin
        order by _loaded_at desc
    ) = 1
)

select * from deduplicated
