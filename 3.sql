/*
This Oracle SQL query is part of an analysis process for telephony call center operations. It consists of two main parts:

1. Multiple JOIN operations connect different tables related to call center activities:
   - ID_PERIOD: Unique identifier for the period of time being analyzed.
   - ID_INTRA_PERIOD: Sub-identifier for the internal period.
   - ID_SKILL, ID_PRODUCT, ID_CAMPAIGN: Various categorizations related to the calls.
   - Various fields for analyzing call response time, abandonment, duration, and other metrics.

2. The main SELECT statement extracts these details and calculates specific metrics such as calls answered under or over 60 seconds, pause times, and active calls.

The results provide a comprehensive view of call interactions, allowing identification of any abnormal behavior in the call handling process. By monitoring different metrics, it can help in overseeing call center performance and adherence to support guidelines.

The query is designed for execution in an Oracle SQL environment and is specific to the analysis of telephony activities within a call center environment.
*/


SELECT 
  P.ID_PERIOD "ID_PERIOD",
  PI.ID_INTRA_PERIOD "ID_INTRA_PERIOD",
  NVL(S.ID_SKILL, -99) "ID_SKILL",
  NVL(S.ID_PRODUCT, -99) "ID_PRODUCT",
  NVL(S.ID_CAMPAIGN, -99) "ID_CAMPAIGN",
  A.ACD "NR_DAC",
  A.SPLIT "CD_SKILL",
  A.acdcalls "QT_CALLS_ANSWERED",
  A.ACDCALLS1 + A.ACDCALLS2 + A.ACDCALLS3 + A.ACDCALLS4 + A.ACDCALLS5 + A.ACDCALLS6 "QT_CALLS_ANSWERED_UNDER_60S",
  A.ACDCALLS7 + A.ACDCALLS8 + A.ACDCALLS9 + A.ACDCALLS10 "QT_CALLS_ANSWERED_OVER_60S",
  TO_DATE(P.ID_PERIOD, 'YYYYMMDD') "DT_REF",
  A.AUXOUTOFFCALLS AS "ACTIVE_CALLS",
  A.AUXOUTOFFTIME AS "TMP_CH_ACTIVE",
  A.I_AUXTIME10 "QT_PAUSE_TIME_10",
  A.I_AUXTIME11 "QT_PAUSE_TIME_11"
FROM 
  TELEPHONY.OD_CMS_HSPLIT A 
  LEFT JOIN TELEPHONY.DI_PERIOD P ON P.DT_PERIOD = A.ROW_DATE 
  LEFT JOIN TELEPHONY.DI_INTRA_PERIOD PI ON PI.NM_INTERVAL_START = A.STARTTIME 
  LEFT JOIN TELEPHONY.DI_SKILL S ON S.CD_SKILL = DECODE(A.SPLIT, NULL, -98, 0, -98, 65535, -98, A.SPLIT)
    AND TRUNC(A.ROW_DATE) >= S.DT_ACTIVATION 
    AND TRUNC(A.ROW_DATE) <= DECODE(S.DT_REMOVED, NULL, TRUNC(SYSDATE), S.DT_REMOVED)
    AND S.PLATFORM_FLAG = A.PLATFORM_FLAG 
    AND S.CD_CTI = A.CD_CTI
WHERE 
  P.ID_PERIOD = 20230101;


