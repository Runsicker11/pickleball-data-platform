-- Monthly revenue by stream — QBO as single source of truth
-- Sources: Invoices (wholesale), Deposits (Amazon/Shopify/Affiliate/Sponsorship)
-- NOTE: Sales Receipts are excluded — they flow through "Payments to deposit"
-- or clearing accounts and are later captured as Deposits, so including them
-- would double-count revenue.

with invoices as (
    select
        date_trunc(txn_date, month) as month,
        'Wholesale' as revenue_stream,
        sum(total_amount) as revenue,
        count(*) as transaction_count
    from {{ ref('stg_quickbooks__invoices') }}
    where txn_status in ('Paid', 'Partial', 'Open')
    group by 1
),

deposits_classified as (
    select
        deposit_id,
        txn_date,
        total_amount,
        deposit_to_account_name,
        coalesce(source_summary, '') as source_summary,
        coalesce(memo, '') as memo,
        case
            -- Amazon: bank deposits with AMAZON in memo
            when deposit_to_account_name = 'Bluevine Checking (6606) - 1'
                and lower(memo) like '%amazon%'
                then 'Amazon'
            -- Shopify: deposits via Shopify channels
            when source_summary like '%Shopify%'
                or (deposit_to_account_name = 'Bluevine Checking (6606) - 1'
                    and lower(memo) like '%shopify%')
                then 'Shopify'
            -- Affiliate: bank deposits tagged as Affiliate Revenue
            when source_summary = 'Affiliate Revenue'
                then 'Affiliate'
            -- Affiliate: all PayPal deposits (affiliate commissions)
            when deposit_to_account_name = 'PayPal balance account'
                then 'Affiliate'
            -- Sponsorship
            when lower(source_summary) like '%sponsor%'
                or lower(memo) like '%sponsor%'
                or lower(source_summary) like '%partnership%'
                or lower(memo) like '%partnership%'
                then 'Sponsorship'
            else 'Other'
        end as revenue_stream
    from {{ ref('stg_quickbooks__deposits') }}
),

deposit_amazon as (
    select
        date_trunc(txn_date, month) as month,
        'Amazon' as revenue_stream,
        sum(total_amount) as revenue,
        count(*) as transaction_count
    from deposits_classified
    where revenue_stream = 'Amazon'
    group by 1
),

deposit_shopify as (
    select
        date_trunc(txn_date, month) as month,
        'Shopify' as revenue_stream,
        sum(total_amount) as revenue,
        count(*) as transaction_count
    from deposits_classified
    where revenue_stream = 'Shopify'
    group by 1
),

deposit_affiliate as (
    select
        date_trunc(txn_date, month) as month,
        'Affiliate' as revenue_stream,
        sum(total_amount) as revenue,
        count(*) as transaction_count
    from deposits_classified
    where revenue_stream = 'Affiliate'
    group by 1
),

deposit_sponsorship as (
    select
        date_trunc(txn_date, month) as month,
        'Sponsorship' as revenue_stream,
        sum(total_amount) as revenue,
        count(*) as transaction_count
    from deposits_classified
    where revenue_stream = 'Sponsorship'
    group by 1
),

bank_supplement as (
    -- Bank statement data for Jan 2025, Feb 2025, and Mar 1-13 2025 (before QBO began)
    select
        month,
        revenue_stream,
        revenue,
        transaction_count
    from {{ ref('bank_statement_revenue') }}
),

all_revenue as (
    select * from invoices
    union all select * from deposit_amazon
    union all select * from deposit_shopify
    union all select * from deposit_affiliate
    union all select * from deposit_sponsorship
    union all select * from bank_supplement
)

select
    month,
    revenue_stream,
    revenue,
    transaction_count
from all_revenue
where month is not null
