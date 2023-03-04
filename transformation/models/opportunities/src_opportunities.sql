



SELECT
    opportunity_id::INT,
    opportunity_name,
    last_updated_date::DATE,
    company_size,
    industry,
    account_id,
    account_owner,
    locale,
    access_tier,
    payment_frequency,
    payment_status,
    product1,
    value1,
    state1,
    product2,
    value2,
    state2,
    file_dt::DATE
FROM {{source('salesforce', 'opportunities')}}