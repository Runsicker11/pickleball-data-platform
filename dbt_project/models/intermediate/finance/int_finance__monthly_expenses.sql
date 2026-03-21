-- Monthly expenses by category using purchase line items joined with accounts

with line_items as (
    select
        li.line_item_id,
        li.purchase_id,
        li.txn_date,
        li.vendor_name,
        li.amount,
        li.account_id,
        coalesce(a.name, li.account_name) as expense_category,
        a.account_type,
        a.account_sub_type,
        a.classification
    from {{ ref('stg_quickbooks__purchase_line_items') }} li
    left join {{ ref('stg_quickbooks__accounts') }} a
        on li.account_id = a.account_id
),

monthly as (
    select
        date_trunc(txn_date, month) as month,
        expense_category,
        account_type,
        account_sub_type,
        -- Flag owner compensation (salary + payroll taxes) to separate from operating expenses
        account_sub_type in ('PayrollWageExpenses', 'PayrollExpenses', 'PayrollTaxExpenses') as is_owner_compensation,
        sum(amount) as total_amount,
        count(*) as transaction_count
    from line_items
    where txn_date is not null
        and expense_category is not null
        and classification = 'Expense'
    group by 1, 2, 3, 4, 5
),

bank_supplement as (
    -- Bank statement data for Jan 2025, Feb 2025, and Mar 1-13 2025 (before QBO began)
    select
        month,
        expense_category,
        account_type,
        account_sub_type,
        is_owner_compensation,
        total_amount,
        transaction_count
    from {{ ref('bank_statement_expenses') }}
)

select month, expense_category, account_type, account_sub_type, is_owner_compensation, total_amount, transaction_count from monthly
union all
select month, expense_category, account_type, account_sub_type, is_owner_compensation, total_amount, transaction_count from bank_supplement
