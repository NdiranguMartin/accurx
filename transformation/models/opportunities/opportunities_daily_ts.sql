-- This is a timeseries for all the opportunities. It starts from the first time an opportunity was created to 
-- the last day of the data, or to the day payment was cancelled. 

WITH ts AS (
    SELECT GENERATE_SERIES(
        (SELECT MIN(last_updated_date) FROM {{ref('src_opportunities')}}), 
        (SELECT MAX(last_updated_date) FROM {{ref('src_opportunities')}}), 
        '1 day'
        ) AS dt
)
,base AS (
    SELECT 
        opportunity_id,
        opportunity_name,
        industry,
        account_id,
        locale,
        payment_frequency,
        product1,
        product2,
        MIN(last_updated_date) first_update_dt,
        MIN(last_updated_date) FILTER(WHERE state1 = 'Close Won' OR state2 = 'Close Won') close_dt,
        MAX(last_updated_date) last_update_dt,
        MIN(last_updated_date) FILTER (WHERE payment_status = 'Cancelled') cancel_dt
    FROM {{ref('src_opportunities')}}
    GROUP BY 
        1,2,3,4,5,6,7,8
)
,daily_value AS (
    SELECT 
        ts.dt::DATE,
        base.opportunity_id,
        base.opportunity_name,
        base.industry,
        base.account_id,
        base.locale,
        base.payment_frequency,
        base.close_dt,
        base.product1,
        opportunities.value1,
        opportunities.state1,
        base.product2,
        opportunities.value2,
        opportunities.state2,
        base.cancel_dt,
        COALESCE(
            sum(
            CASE WHEN opportunities.value1 IS NOT NULL AND opportunities.state1 = 'Close Won' THEN 1 END
            ) OVER (
            PARTITION BY base.opportunity_id ORDER BY dt
            ),
            0
        ) AS value1_grp,
        COALESCE(
            sum(
            CASE WHEN opportunities.value2 IS NOT NULL AND opportunities.state2 = 'Close Won' THEN 1 END
            ) OVER (
            PARTITION BY base.opportunity_id ORDER BY dt
            ),
            0
        ) AS value2_grp
    FROM ts
    CROSS JOIN base
    LEFT JOIN {{ref('src_opportunities')}} opportunities 
        ON opportunities.opportunity_id = base.opportunity_id
        AND ts.dt = opportunities.last_updated_date
    WHERE 
        ts.dt >= base.first_update_dt
        AND (ts.dt <= COALESCE(base.cancel_dt, current_date))
)
SELECT
    dt,
    opportunity_id,
    opportunity_name,
    industry,
    account_id,
    locale,
    payment_frequency,
    close_dt,
    product1,
    state1,
    value1,
    CASE WHEN value1_grp = 0 THEN 0 ELSE first_value(value1) over (partition by opportunity_id, value1_grp order by dt) END value1_at_dt,
    product2,
    state2,
    value2,
    CASE WHEN value2_grp = 0 THEN 0 ELSE first_value(value2) over (partition by opportunity_id, value2_grp order by dt) END value2_at_dt,
    cancel_dt
FROM daily_value