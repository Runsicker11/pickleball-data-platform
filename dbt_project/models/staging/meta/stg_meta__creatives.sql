with source as (
    select * from {{ source('raw_meta', 'creatives') }}
),

renamed as (
    select
        cast(creative_id as string) as creative_id,
        cast(ad_id as string) as ad_id,
        ad_name,
        title,
        body,
        cta_type,
        image_url,
        video_id,
        thumbnail_url,
        object_type

    from source
)

select * from renamed
