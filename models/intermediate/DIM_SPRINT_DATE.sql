
{{
    config(
        materialized='incremental',
        unique_key ='PI_ITERATION_NUMBER',
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
  SELECT CALENDAR_DATE
  FROM UNNEST(GENERATE_DATE_ARRAY(
    (SELECT vSTART_DATE FROM DATES_BOUND),
    (SELECT vEND_DATE FROM DATES_BOUND), 
    INTERVAL 1 DAY
  )) AS CALENDAR_DATE
)
,TMP_JIRA_ITR_DT AS (
  SELECT
        CALENDAR_DATE
        , PI_Iteration AS PI_ITERATION_NUMBER
        , PI_Number AS PI_NUMBER
        , Iteration_Number AS ITERATION_NUMBER
        , Start_Date AS ITERATION_START_DATE
        , End_Date AS ITERATION_END_DATE
        , CASE WHEN CURRENT_DATE() BETWEEN Start_Date AND End_Date THEN TRUE ELSE FALSE END AS CURRENT_FLAG
  FROM DATES
  INNER JOIN {{ source('BRONZE','SPRINT_DT') }} AS ITERATION_DATE
    ON CALENDAR_DATE BETWEEN Start_Date AND End_Date
)
,TMP_JIRA_ITR_DT_01 AS (
    SELECT
    PI_ITERATION_NUMBER,
    PI_NUMBER,
    MIN(ITERATION_START_DATE) OVER (PARTITION BY PI_ITERATION_NUMBER) AS ITERATION_START_DATE,
    MAX(ITERATION_END_DATE) OVER (PARTITION BY PI_ITERATION_NUMBER) AS ITERATION_END_DATE,
    CALENDAR_DATE
    FROM
    (SELECT 
        A.PI_ITERATION_NUMBER,
        A.PI_NUMBER,
        A.CALENDAR_DATE,
        A.ITERATION_START_DATE,
        A.ITERATION_END_DATE
    FROM 
    (SELECT * FROM TMP_JIRA_ITR_DT WHERE ITERATION_NUMBER != 'Extended' AND ITERATION_NUMBER != 'Planning Week') A
    UNION ALL
    ---Append Buffer Week data and convert Buffer week into Iteration 5
    SELECT
        CONCAT(PI_NUMBER,' Iteration 5') AS PI_ITERATION_NUMBER,
        PI_NUMBER,
        CALENDAR_DATE,
        ITERATION_START_DATE,
        ITERATION_END_DATE
        FROM
        TMP_JIRA_ITR_DT
        WHERE
        ITERATION_NUMBER = 'Extended'
        UNION ALL
        -- Planning Week becomes Iteration 5 of the *Previous PI* (MODIFIED)
        SELECT
        CONCAT(
            -- Calculates Previous PI Number (e.g., PI05 -> PI04)
            CONCAT(
            'PI',
            LPAD(
                CAST(CAST(SUBSTR(PI_NUMBER, 3) AS INT64) - 1 AS STRING),
                2,
                '0'
            )
            ),
            ' Iteration 5'
        ) AS PI_ITERATION_NUMBER,
        -- Set the PI_NUMBER to the Previous PI (Current PI - 1)
        CONCAT(
            'PI',
            LPAD(
            CAST(CAST(SUBSTR(PI_NUMBER, 3) AS INT64) - 1 AS STRING),
            2,
            '0'
            )
        ) AS PI_NUMBER,
        CALENDAR_DATE,
        ITERATION_START_DATE,
        ITERATION_END_DATE
        FROM
        TMP_JIRA_ITR_DT
        WHERE
        ITERATION_NUMBER = 'Planning Week'
    )
)
SELECT 
    PI_ITERATION_NUMBER
    , PI_NUMBER
    , CALENDAR_DATE
    , ITERATION_START_DATE
    , ITERATION_END_DATE
FROM 
    TMP_JIRA_ITR_DT_01
{% if is_incremental() %}
WHERE CALENDAR_DATE >= (
  SELECT MAX(End_Date) FROM {{ source('BRONZE','SPRINT_DT') }} 
  WHERE End_Date > COALESCE((SELECT MAX(CALENDAR_DATE) FROM {{ this }}), '1900-01-01')
)
{% endif %}