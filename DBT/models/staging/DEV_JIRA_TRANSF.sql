WITH JIRA_TRANSF_01 AS ( --Cast Data type and rename column
    SELECT 
        CAST(Issue_Type AS STRING) AS TCK_TP
        , CAST(Summary AS STRING) AS TCK_NM
        , CAST(Assignee AS STRING) AS ASN_NM
        , CAST(Status AS STRING) AS TCK_STS
        , CAST(Sprint AS STRING) AS PI_ID
        , CAST(Est__Story_Points AS FLOAT64) AS STR_PNT
        , CAST(`Key` AS STRING) AS TCK_ID
        , CAST(parent AS STRING) AS PRN_ID
        , CAST(Start_date AS DATETIME) AS STR_DT
        , CAST(End_date AS DATETIME) AS END_DT
        , CAST(Updated AS DATETIME) AS UPD_DT
        , CAST(accountId AS STRING) AS DEV_ID
    FROM {{source("BRONZE","JIRA_RAW")}}
)
,JIRA_TRNSF_02 AS (-- Handeling NULL values
    SELECT
        TCK_ID
        , COALESCE(PRN_ID,'Undefined')  AS PRN_ID
        , COALESCE(DEV_ID,'Undefined')  AS DEV_ID
        , COALESCE(PI_ID,'Undefined')   AS PI_ID
        , COALESCE(ASN_NM,'Undefined')  AS ASN_NM
        , COALESCE(TCK_NM,'Undefined')  AS TCK_NM
        , COALESCE(TCK_TP,'Undefined')  AS TCK_TP
        , COALESCE(TCK_STS,'Undefined') AS TCK_STS
        , COALESCE(STR_PNT,0)           AS STR_PNT
        , COALESCE(STR_DT,'1900-01-01') AS STR_DT
        , COALESCE(END_DT,'1900-01-01') AS END_DT
        , COALESCE(UPD_DT,'1900-01-01') AS UPD_DT
    FROM JIRA_TRANSF_01
)
,JIRA_TRNSF_03 AS(-- Unpivot a group of PI_ID values
    SELECT 
        TCK_ID
        , PRN_ID
        , DEV_ID
        , PI_ID
        , ASN_NM
        , TCK_NM
        , TCK_TP
        , TCK_STS
        , STR_PNT
        , STR_DT
        , END_DT
        , UPD_DT
    FROM JIRA_TRNSF_02 UNPIVOT, UNNEST(SPLIT(PI_ID,';')) AS PI_ID
)
    SELECT
            TCK_ID
            , PRN_ID
            , DEV_ID
            , PI_ID
            , ASN_NM
            , TCK_NM
            , TCK_TP
            , TCK_STS
            , STR_PNT
            , STR_DT
            , END_DT
            , UPD_DT
    FROM JIRA_TRNSF_03
    WHERE TCK_ID IS NOT NULL