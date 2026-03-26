with source as (
    select * from {{ source('raw_merchant_center', 'shopping_ads_program') }}
),

renamed as (
    select
        state,
        region_code,
        eligibility_status,
        review_issues,
        cast(ingested_at as timestamp) as ingested_at

    from source
)

select * from renamed
