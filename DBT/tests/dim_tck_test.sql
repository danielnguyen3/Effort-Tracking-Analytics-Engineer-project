SELECT 
    TCK_ID
FROM {{ ref('dim_tck_inf') }}
WHERE TCK_ID IS NULL