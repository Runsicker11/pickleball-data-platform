with source as (
    select * from {{ source('raw_paypal', 'transactions') }}
),

renamed as (
    select
        cast(transaction_id as string) as transaction_id,
        transaction_date as transaction_at,
        date(transaction_date) as transaction_date,
        transaction_updated_date as transaction_updated_at,
        cast(transaction_amount as numeric) as transaction_amount,
        cast(fee_amount as numeric) as fee_amount,
        currency_code,
        transaction_status,
        transaction_event_code,
        transaction_subject,
        transaction_note,
        paypal_reference_id,
        paypal_reference_id_type,
        invoice_id,
        custom_field,
        payer_email,
        payer_account_id,
        payer_given_name,
        payer_surname,
        payer_full_name,
        payer_alternate_name,
        first_item_name,
        cast(item_count as int64) as item_count,
        ingested_at

    from source
)

select * from renamed
