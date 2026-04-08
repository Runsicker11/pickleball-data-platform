-- Affiliate revenue by partner by month
-- Combines PayPal deposits + bank deposits, matches to partner mapping

with deposits as (
    select
        deposit_id,
        txn_date,
        total_amount,
        deposit_to_account_name,
        coalesce(source_summary, '') as source_summary,
        coalesce(memo, '') as memo
    from {{ ref('stg_quickbooks__deposits') }}
    where deposit_to_account_name in ('PayPal balance account', 'Bluevine Checking (6606) - 1')
),

-- Only affiliate-related deposits (bank tagged as Affiliate Revenue, or PayPal)
affiliate_deposits as (
    -- All PayPal deposits (these are almost all affiliate commissions)
    select
        deposit_id,
        txn_date,
        total_amount,
        'PayPal' as payment_channel,
        memo,
        -- Extract description from PayPal memo if present
        case
            when memo like '%Description:%'
            then trim(substr(memo, strpos(memo, 'Description:') + 12))
            else ''
        end as paypal_description,
        -- Extract PayPal transaction ID from memo for joining to PayPal API data
        regexp_extract(memo, r'Transaction ID:\s*([A-Z0-9]+)') as paypal_txn_id
    from deposits
    where deposit_to_account_name = 'PayPal balance account'

    union all

    -- Bank deposits tagged as Affiliate Revenue
    select
        deposit_id,
        txn_date,
        total_amount,
        'Bank' as payment_channel,
        memo,
        '' as paypal_description,
        null as paypal_txn_id
    from deposits
    where deposit_to_account_name = 'Bluevine Checking (6606) - 1'
      and source_summary = 'Affiliate Revenue'
),

-- paypal_txn_names seed removed; sender name lookup falls back to memo/subject tiers below
with_paypal_sender as (
    select
        d.*,
        cast(null as string) as paypal_sender_name
    from affiliate_deposits d
),

-- Map sender name → partner brand via the partner mapping seed
with_paypal_brand as (
    select
        d.*,
        pm.partner_brand as paypal_partner_brand
    from with_paypal_sender d
    left join {{ ref('paypal_partner_mapping') }} pm
        on lower(trim(d.paypal_sender_name)) = lower(trim(pm.sender_name))
),

-- Also join the Reporting API for transaction_subject (fallback match text when
-- neither a QBO description nor a seed brand is available)
with_paypal_info as (
    select
        d.*,
        pt.transaction_subject as paypal_txn_subject
    from with_paypal_brand d
    left join {{ ref('stg_paypal__transactions') }} pt
        on d.paypal_txn_id = pt.transaction_id
        and pt.transaction_status = 'Success'
),

-- Extract a match key from memo/description for partner lookup
with_match_key as (
    select
        *,
        -- Normalize memo to extract the company identifier
        lower(
            case
                when payment_channel = 'Bank' then
                    -- Bank memos: "TREMENDOUS, PAYOUT" → "tremendous"
                    -- "Pickleball Opco, Purpose Bu" → "pickleball opco"
                    regexp_extract(lower(memo), r'^([a-z0-9 ]+)')
                when paypal_description != '' then
                    lower(paypal_description)
                -- No description in QBO memo — fall back to PayPal transaction_subject
                when paypal_txn_subject is not null then
                    lower(paypal_txn_subject)
                else
                    lower(memo)
            end
        ) as match_text
    from with_paypal_info
),

-- Map to partners using keyword matching
partner_matched as (
    select
        d.deposit_id,
        d.txn_date,
        d.total_amount,
        d.payment_channel,
        d.memo,
        d.match_text,
        case
            -- Tier 1: CSV-export sender name → brand lookup (most reliable)
            when d.paypal_partner_brand is not null and d.paypal_partner_brand != ''
                then d.paypal_partner_brand

            -- Tier 2: PayPal Inc. intermediary — identify by memo/subject keyword
            when d.paypal_sender_name like '%Paypal Inc%' and d.match_text like '%tremendous%'
                then 'CRBN'
            when d.paypal_sender_name like '%Paypal Inc%' and d.match_text like '%shopify%'
                then 'Shopify Collabs'

            -- Tier 3: Bank-direct partners (match on memo prefix)
            when d.payment_channel = 'Bank' and d.match_text like 'tremendous%' then 'CRBN'
            when d.payment_channel = 'Bank' and d.match_text like 'pickleball opco%' then 'Pickleball Central'
            when d.payment_channel = 'Bank' and d.match_text like 'pickleball topco%' then 'Pickleball Central'
            when d.payment_channel = 'Bank' and d.match_text like 'hsi inc%' then 'HSI Inc'
            when d.payment_channel = 'Bank' and d.match_text like 'sport squad%' then 'Joola'
            when d.payment_channel = 'Bank' and d.match_text like 'babolat%' then 'Babolat'
            when d.payment_channel = 'Bank' and d.match_text like 'ronbus%' then 'Ronbus'
            when d.payment_channel = 'Bank' and d.match_text like 'impact radius%' then 'Impact Radius'
            when d.payment_channel = 'Bank' and d.match_text like 'payoneer%' then 'Payoneer'
            when d.payment_channel = 'Bank' and d.match_text like 'reload technolog%' then 'Reload Technologies'
            when d.payment_channel = 'Bank' and d.match_text like 'revolin%' then 'Revolin Sports'
            when d.payment_channel = 'Bank' and d.match_text like 'mobile deposit%' then 'Unknown (Mobile Deposit)'

            -- Tier 4: PayPal description / transaction_subject keyword fallback
            when d.match_text like '%vatic pro%' then 'Vatic Pro'
            when d.match_text like '%hpc commission%' or d.match_text like '%hpc commissions%' or d.match_text like 'hpc,%' then 'HPC'
            when d.match_text like '%adv ss commission%' or d.match_text like '%adv ss commission%' then 'Selkirk'
            when d.match_text like '%gearbox%' then 'Gearbox'
            when d.match_text like '%luzz pickleball%' then 'Luzz'
            when d.match_text like '%body helix%' or d.match_text like '%bodyhelix%' then 'Body Helix'
            when d.match_text like '%impact%media part%' then 'Impact Radius'
            when d.match_text like '%jigsaw health%' then 'Jigsaw'
            when d.match_text like '%volair%' then 'Volair'
            when d.match_text like '%11six24%' or d.match_text like '%david@11six24%' then '11SIX24'
            when d.match_text like '%leor%ver%' then 'LEOREVER'
            when d.match_text like '%shopify inc%' then 'Shopify Collabs'
            when d.match_text like '%tremendous%' then 'CRBN'
            when d.match_text like '%uppromote%' or d.match_text like '%upromote%' then 'UpPromote (Shopify)'
            when d.match_text like '%pickleball central%' or d.match_text like '%pickleballcentral%' then 'Pickleball Central'
            when d.match_text like '%enhance%' then 'Enhance'
            when d.match_text like '%six zero%' then 'Six Zero'
            when d.match_text like '%selkirk%' then 'Selkirk'
            when d.match_text like '%justpaddles%' then 'JustPaddles'
            when d.match_text like '%slyce%' then 'Slyce Sport'
            when d.match_text like '%gearbox%' then 'Gearbox'
            when d.match_text like '%ilavie%' then 'ILAVIE'
            when d.match_text like '%thrive%' then 'Thrive PB'
            when d.match_text like '%neonic%' then 'Neonic Pickleball'
            when d.match_text like '%we pickleball%' or d.match_text like '%winners edge%' then 'WE Pickleball'
            when d.match_text like '%engage pickleball%' then 'Engage Pickleball'
            when d.match_text like '%element 6%' then 'Element 6 Pickleball'
            when d.match_text like '%gruvn%' then 'GRUVN'
            when d.match_text like '%leorever%' or d.match_text like '%leor%ever%' then 'LEOREVER'

            -- Catch-all for unidentified
            else 'Unidentified'
        end as partner_name
    from with_match_key d
),

qbo_data as (
    select
        date_trunc(txn_date, month) as month,
        partner_name,
        payment_channel,
        sum(total_amount) as revenue,
        count(*) as transaction_count
    from partner_matched
    group by 1, 2, 3
),

bank_supplement as (
    -- Bank statement data for Jan 2025, Feb 2025, and Mar 1-13 2025 (before QBO began)
    select month, partner_name, payment_channel, revenue, transaction_count
    from {{ ref('bank_statement_affiliate') }}
)

select * from qbo_data
union all
select * from bank_supplement
