
{{
    config(
        materialized='incremental',
        unique_key ='PI_ID',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}


WITH DATES_BOUND AS (
  SELECT 
    MIN(Start_Date) AS vSTART_DATE,
    MAX(End_Date) AS vEND_DATE
  FROM  {{ source('BRONZE','SPRINT_DT') }}
),
DATES AS (
  SELECT CAL_DT
  FROM UNNEST(GENERATE_DATE_ARRAY(
    (SELECT vSTART_DATE FROM DATES_BOUND),
    (SELECT vEND_DATE FROM DATES_BOUND), 
    INTERVAL 1 DAY
  )) AS CAL_DT
)
,TMP_JIRA_ITR_DT AS (
  SELECT
        CAL_DT
        , PI_Iteration AS PI_ID
        , PI_NUMBER AS PI_NUM
        , ITERATION_NUMBER AS ITR_ID
        , Start_Date AS STR_DT
        , End_Date AS END_DT
  FROM DATES
  INNER JOIN {{ source('BRONZE','SPRINT_DT') }} AS ITERATION_DATE
    ON CAL_DT BETWEEN Start_Date AND End_Date
)
,TMP_JIRA_ITR_DT_01 AS (
    SELECT
    PI_ID,
    PI_NUM,
    ITR_ID,
    MIN(STR_DT) OVER (PARTITION BY PI_ID) AS STR_DT,
    MAX(END_DT) OVER (PARTITION BY PI_ID) AS END_DT,
    CAL_DT
    FROM
    (SELECT 
        A.PI_ID,
        A.PI_NUM,
        ITR_ID,
        A.CAL_DT,
        A.STR_DT,
        A.END_DT
    FROM 
    (SELECT * FROM TMP_JIRA_ITR_DT WHERE ITR_ID != 'Extended' AND ITR_ID != 'Planning Week') A
    UNION ALL
    ---Append Buffer Week data and convert Buffer week into Iteration 5
    SELECT
        CONCAT(PI_NUM,' Iteration 5') AS PI_ID,
        PI_NUM,
        ITR_ID,
        CAL_DT,
        STR_DT,
        END_DT
    FROM
        TMP_JIRA_ITR_DT
        WHERE
        ITR_ID = 'Extended'
        UNION ALL
        -- Planning Week becomes Iteration 5 of the *Previous PI* (MODIFIED)
        SELECT
        CONCAT(
            -- Calculates Previous PI Number (e.g., PI05 -> PI04)
            CONCAT(
            'PI',
            LPAD(
                CAST(CAST(SUBSTR(PI_NUM, 3) AS INT64) - 1 AS STRING),
                2,
                '0'
            )
            ),
            ' Iteration 5'
        ) AS PI_ID,
        -- Set the PI_NUM to the Previous PI (Current PI - 1)
        CONCAT(
            'PI',
            LPAD(
            CAST(CAST(SUBSTR(PI_NUM, 3) AS INT64) - 1 AS STRING),
            2,
            '0'
            )
        ) AS PI_NUM,
        'Iteration 5' AS ITR_ID,
        CAL_DT,
        STR_DT,
        END_DT
        FROM
        TMP_JIRA_ITR_DT
        WHERE
        ITR_ID = 'Planning Week'
    )
)
SELECT 
    PI_ID
    , PI_NUM
    , ITR_ID
    , CAL_DT
    , STR_DT
    , END_DT
    , CASE WHEN CURRENT_DATE() BETWEEN STR_DT AND END_DT THEN TRUE ELSE FALSE END AS ACT_FLG
FROM 
    TMP_JIRA_ITR_DT_01
{% if is_incremental() %}
WHERE CAL_DT >= (
  SELECT MAX(End_Date) FROM {{ source('BRONZE','SPRINT_DT') }} 
  WHERE End_Date > COALESCE((SELECT MAX(CAL_DT) FROM {{ this }}), '1900-01-01')
)
{% endif %}