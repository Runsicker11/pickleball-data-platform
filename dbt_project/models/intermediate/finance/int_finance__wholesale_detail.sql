-- Wholesale invoice + line item detail for customer/product analysis

with invoices as (
    select
        invoice_id,
        txn_date,
        customer_id,
        customer_name,
        total_amount,
        balance,
        txn_status
    from {{ ref('stg_quickbooks__invoices') }}
),

line_items as (
    select
        line_item_id,
        invoice_id,
        item_name,
        quantity,
        unit_price,
        amount
    from {{ ref('stg_quickbooks__invoice_line_items') }}
),

customers as (
    select
        customer_id,
        company_name,
        state
    from {{ ref('stg_quickbooks__customers') }}
),

qbo_detail as (
    select
        li.line_item_id,
        i.invoice_id,
        i.txn_date,
        i.customer_name,
        c.company_name,
        c.state,
        li.item_name,
        li.quantity,
        li.unit_price,
        li.amount,
        i.total_amount as invoice_total,
        i.balance as invoice_balance,
        i.txn_status
    from line_items li
    inner join invoices i
        on li.invoice_id = i.invoice_id
    left join customers c
        on i.customer_id = c.customer_id
),

-- Bank statement aggregate rows for Jan 2025, Feb 2025, Mar 1-13 2025 (before QBO began)
-- These are invoice payments deposited via QuickBooks Payments + checks, no customer/product detail
bank_supplement as (
    select
        cast('bank-' || format_date('%Y-%m', month) as string) as line_item_id,
        cast('BANK-' || format_date('%Y-%m', month) as string) as invoice_id,
        month as txn_date,
        'Pre-QBO (Bank Statement)' as customer_name,
        'Pre-QBO (Bank Statement)' as company_name,
        cast(null as string) as state,
        cast(null as string) as item_name,
        cast(null as float64) as quantity,
        cast(null as float64) as unit_price,
        revenue as amount,
        revenue as invoice_total,
        cast(0.0 as float64) as invoice_balance,
        'Paid' as txn_status
    from {{ ref('bank_statement_revenue') }}
    where revenue_stream = 'Wholesale'
)

select * from qbo_detail
union all
select * from bank_supplement
