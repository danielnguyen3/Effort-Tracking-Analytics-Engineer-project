
{{
    config(
        materialized='incremental',
        unique_key =['TCK_ID','CMP_DT'],
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

WITH PRNT_FLG AS (
    SELECT
        PRN_ID
    FROM 
        {{source('BRONZE','JIRA_TRANSF')}}
    WHERE
        PRN_ID != 'Undefined'
)
,UPD_ITR AS (
    SELECT 
        A.TCK_ID
        ,A.PI_ID
        ,CASE 
            WHEN A.UPD_DT BETWEEN PARSE_DATETIME('%Y-%m-%d %H:%M:%S',CONCAT(B.STR_DT,'13:30:01')) 
                            AND DATE_ADD(PARSE_DATETIME('%Y-%m-%d %H:%M:%S',CONCAT(B.END_DT,'13:30:01')), INTERVAL 1 DAY) THEN CONCAT('DDE ',B.PI_ID)  
            ELSE 'Undefined'
        END AS PI_RP_ID,
    FROM 
        `BRONZE.DEV_JIRA_TRANSF` A
    LEFT JOIN (
                SELECT 
                    PI_ID, 
                    MIN(STR_DT) AS STR_DT, 
                    MAX(END_DT) AS END_DT 
                FROM
                    `SILVER.DIM_SPRINT_DT` 
                GROUP BY PI_ID
    ) B
    ON A.PI_ID = CONCAT('DDE ',B.PI_ID)
    AND CAST(A.UPD_DT AS DATE) BETWEEN DATE_ADD(B.STR_DT,INTERVAL 1 DAY) AND DATE_ADD(B.END_DT,INTERVAL 1 DAY)
)
, DEV_EFRT_01 AS (
        SELECT 
        EF.DEV_ID
        , EF.TCK_ID
        , EF.PRN_ID
        , EF.PI_ID
        , EF.UPD_DT
        , EF.STR_PNT
        , CASE WHEN PR.PRN_ID IS NULL THEN FALSE ELSE TRUE END AS PRN_FLG
        , UP.PI_RP_ID
        , CASE WHEN EF.TCK_STS = 'Done' THEN EF.UPD_DT ELSE NULL END AS CMP_DT
    FROM 
        `BRONZE.DEV_JIRA_TRANSF` EF
    LEFT OUTER JOIN PRNT_FLG AS PR
    ON EF.TCK_ID = PR.PRN_ID
    LEFT OUTER JOIN UPD_ITR AS UP
    ON EF.TCK_ID = UP.TCK_ID
    AND EF.PI_ID = UP.PI_ID
)
SELECT
    DEV_ID
    , TCK_ID
    , PRN_ID
    , PI_ID
    , PI_RP_ID
    , UPD_DT
    , CMP_DT
    , STR_PNT
    , PRN_FLG
FROM
    DEV_EFRT_01
{% if is_incremental() %}
WHERE CAST(UPD_DT AS TIMESTAMP) >= (SELECT CAST(MAX(UPD_DT) AS TIMESTAMP) FROM {{this}} WHERE CMP_DT IS NULL)
{% endif %}
ORDER BY UPD_DT DESC --intial load '2025-12-26T11:09:13'