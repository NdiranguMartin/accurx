-- This model changes the data types and tests the assumptions of the data.
-- For tests and documentation see opportunities.yml
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
    CASE WHEN state1 = '' THEN NULL else state1 END AS state1,
    product2,
    value2,
    CASE WHEN state2 = '' THEN NULL else state2 END AS state2,
    file_dt::DATE
FROM {{source('salesforce', 'opportunities')}}