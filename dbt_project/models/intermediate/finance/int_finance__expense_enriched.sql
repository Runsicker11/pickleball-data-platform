-- Expense enrichment: high-level buckets + month-over-month change tracking

with expenses as (
    select
        month,
        expense_category,
        account_type,
        account_sub_type,
        total_amount,
        transaction_count
    from {{ ref('int_finance__monthly_expenses') }}
),

bucketed as (
    select
        *,
        case
            when account_sub_type in (
                'SuppliesMaterialsCogs', 'CostOfLaborCos',
                'OtherCostsOfServiceCos', 'ShippingFreightDeliveryCos'
            ) then 'COGS & Fulfillment'
            when account_sub_type = 'AdvertisingPromotional'
                then 'Marketing & Advertising'
            when account_sub_type in ('DuesSubscriptions')
                or (account_sub_type = 'OfficeGeneralAdministrativeExpenses'
                    and lower(expense_category) like '%software%')
                then 'SaaS & Tools'
            when account_sub_type in ('CostOfLabor', 'LegalProfessionalFees')
                then 'Professional Services'
            when account_sub_type = 'PayrollWageExpenses'
                then 'Payroll & Benefits'
            when account_sub_type = 'Insurance'
                then 'Facilities & Insurance'
            when account_sub_type in ('Travel', 'TravelMeals')
                then 'Travel & Events'
            when account_sub_type in ('ShippingFreightDelivery', 'SuppliesMaterials')
                then 'Supplies & Shipping'
            else 'Other Operating'
        end as expense_bucket
    from expenses
),

with_mom as (
    select
        *,
        lag(total_amount) over (
            partition by expense_category order by month
        ) as prior_month_amount,
        total_amount - lag(total_amount) over (
            partition by expense_category order by month
        ) as mom_change,
        safe_divide(
            total_amount - lag(total_amount) over (
                partition by expense_category order by month
            ),
            lag(total_amount) over (
                partition by expense_category order by month
            )
        ) as mom_change_pct
    from bucketed
)

select
    month,
    expense_category,
    expense_bucket,
    account_type,
    account_sub_type,
    total_amount,
    transaction_count,
    prior_month_amount,
    mom_change,
    mom_change_pct
from with_mom
