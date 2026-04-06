select
    order_id,
    customer_id,
    email,
    responded_at,
    how_did_you_hear,
    order_total,
    total_discounts
from `practical-gecko-373320`.raw_shopify.grapevine_survey_responses
order by responded_at desc
