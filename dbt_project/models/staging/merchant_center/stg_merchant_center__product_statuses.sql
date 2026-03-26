with source as (
    select * from {{ source('raw_merchant_center', 'product_statuses') }}
),

renamed as (
    select
        product_id,
        title,
        approval_status,
        disapproval_reasons,
        issue_code,
        issue_servability,
        issue_resolution,
        issue_attribute,
        issue_destination,
        issue_description,
        cast(ingested_at as timestamp) as ingested_at

    from source
)

select * from renamed
