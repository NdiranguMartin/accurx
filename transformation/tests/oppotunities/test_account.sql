WITH base AS (
    SELECT
        account_id,
        opportunity_id
    FROM {{ref('src_opportunities')}}
    GROUP BY 
        1,2
)
SELECT 
    opportunity_id
FROM base
GROUP BY 1
HAVING count(account_id) > 1