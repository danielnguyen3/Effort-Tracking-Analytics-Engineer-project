{{
    config(materialized='incremental',
            unique_key = 'DEV_ID',
            incremental_strategy ='merge',
            on_schema_change ='append_new_columns'    
    )
}}


WITH DEV_INF_TRNSF AS (
  SELECT 
    JIRA_ID AS DEV_ID
    , Name AS VIE_NM
    , IFNULL(ENGLISH_NAME, 'Undefined') AS ENG_NM
    , IFNULL(SPOKE_NAME, 'Undefined') AS SPK_NM
    , IFNULL(ROLE, 'Undefined') AS ROLE
    , IFNULL(CLV_START_DATE,'1900-01-01') AS CLV_SRT_DT
    , IFNULL(LOOKER_START_DATE, '1900-01-01') AS PJC_SRT_DT
    , DOB_DATE
  FROM {{source("BRONZE","DEV_INF")}}
)

SELECT
    DEV_ID
    , VIE_NM
    , ENG_NM
    , SPK_NM
    , ROLE
    , CLV_SRT_DT
    , PJC_SRT_DT
FROM DEV_INF_TRNSF
WHERE DEV_ID IS NOT NULL AND DEV_ID !=''
{% if is_incremental() %}
AND CLV_SRT_DT >= (SELECT MAX(CLV_SRT_DT) FROM {{this}})
{% endif %}