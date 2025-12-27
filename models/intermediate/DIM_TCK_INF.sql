WITH TCK_INF_01 AS (
    SELECT DISTINCT
        TCK_ID
        , PRN_ID
        , TCK_NM
        , TCK_STS
        , STR_DT
        , END_DT
        , COUNT(PI_ID) OVER(PARTITION BY TCK_ID) AS NO_DLAY_PI
    FROM {{source('BRONZE','JIRA_TRANSF')}}
    WHERE TCK_ID IS NOT NULL
)
SELECT DISTINCT
        TCK_ID
        , PRN_ID
        , TCK_NM
        , TCK_STS
        , STR_DT
        , END_DT
        , CASE WHEN NO_DLAY_PI > 1 THEN TRUE ELSE FALSE END AS DLAY_FLG
FROM TCK_INF_01
