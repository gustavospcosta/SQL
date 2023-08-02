/*
This Oracle SQL query is responsible for merging data from temporary outage logs into a master outage log table (TB_MIS_LOG_OUTAGE). It consists of two main parts:

1. The USING clause constructs a temporary view based on three tables:
   - TMP_MIS_LOG_OUTAGE: Temporary outage log details.
   - A subquery that groups the outage log data by outage number and filters by a specific log type ('TICKET MOVED TO OUTAGES MDU QUEUE').
   - A join with an employee login information table.

   The temporary view includes columns like OUTAGE_NUMBER, START_DATE, TICKET_LOG_ID, LOG_TYPE, LOG_DATE, USER_LOGIN, EMPLOYEE_ID, USER_NAME, REF_MONTH, FILE_NAME, and LOAD_DATE.

2. The MERGE INTO clause performs the main logic:
   - WHEN MATCHED: The existing records in the master outage log table are updated based on the temporary view.
   - WHEN NOT MATCHED: New records are inserted from the temporary view into the master outage log table.

The query plays a vital role in maintaining and updating the log information related to outages, ensuring that the data in the master table is always up-to-date with the latest logs. This allows for accurate tracking and reporting of outage incidents, which is essential for system monitoring and incident management.

The query leverages Oracle's MERGE statement, allowing for efficient upsert operations (insert or update), and is designed for execution in an Oracle SQL environment.
*/


MERGE INTO MISADM.TB_MIS_LOG_OUTAGE AS OUTAGE_LOG
USING (
    SELECT TO_NUMBER(T1.OUTAGE) AS OUTAGE_NUMBER,
           TO_DATE(T1.START_DATE, 'rrrr/mm/dd HH24:MI:SS') AS START_DATE,
           T1.TICKET_LOG_ID,
           T1.LOG_TYPE,
           TO_DATE(T1.LOG_DATE, 'rrrr/mm/dd HH24:MI:SS') AS LOG_DATE,
           T1.USER_LOGIN,
           T3.EMPLOYEE_ID,
           T1.USER_NAME,
           TO_DATE(T1.REFERENCE_MONTH || '01', 'YYYYMMDD') AS REF_MONTH,
           T1.FILE_NAME,
           T1.LOAD_DATE,
           NULL AS UPDATE_DATE
    FROM MISADM.TMP_MIS_LOG_OUTAGE AS TEMP_OUTAGE_LOG T1,
         (
             SELECT OUTAGE,
                    MIN(LOG_DATE) AS LOG_DATE
             FROM MISADM.TMP_MIS_LOG_OUTAGE
             WHERE UPPER(LOG_TYPE) = 'TICKET MOVED TO OUTAGES MDU QUEUE'
             GROUP BY OUTAGE
         ) AS T2,
         (
             SELECT LOGIN_ID,
                    EMPLOYEE_ID
             FROM TB_LOGIN_INFO
             WHERE LOGIN_TYPE = 'IDM'
         ) AS T3
    WHERE T1.OUTAGE = T2.OUTAGE(+)
      AND T1.LOG_DATE = T2.LOG_DATE(+)
      AND T1.USER_LOGIN = T3.LOGIN_ID(+)
      AND T1.TICKET_LOG_ID IS NOT NULL
) AS TMP
ON (TMP.OUTAGE_NUMBER = OUTAGE_LOG.OUTAGE_NUMBER AND OUTAGE_LOG.TICKET_LOG_ID = TMP.TICKET_LOG_ID)
WHEN MATCHED THEN
    UPDATE SET OUTAGE_LOG.START_DATE = TMP.START_DATE,
               OUTAGE_LOG.LOG_TYPE = TMP.LOG_TYPE,
               OUTAGE_LOG.LOG_DATE = TMP.LOG_DATE,
               OUTAGE_LOG.USER_LOGIN = TMP.USER_LOGIN,
               OUTAGE_LOG.EMPLOYEE_ID = TMP.EMPLOYEE_ID,
               OUTAGE_LOG.USER_NAME = TMP.USER_NAME,
               OUTAGE_LOG.REF_MONTH = TMP.REF_MONTH,
               OUTAGE_LOG.FILE_NAME = TMP.FILE_NAME,
               OUTAGE_LOG.UPDATE_DATE = SYSDATE
WHEN NOT MATCHED THEN
    INSERT (
        OUTAGE_LOG.OUTAGE_NUMBER,
        OUTAGE_LOG.START_DATE,
        OUTAGE_LOG.TICKET_LOG_ID,
        OUTAGE_LOG.LOG_TYPE,
        OUTAGE_LOG.LOG_DATE,
        OUTAGE_LOG.USER_LOGIN,
        OUTAGE_LOG.EMPLOYEE_ID,
        OUTAGE_LOG.USER_NAME,
        OUTAGE_LOG.REF_MONTH,
        OUTAGE_LOG.FILE_NAME,
        OUTAGE_LOG.LOAD_DATE,
        OUTAGE_LOG.UPDATE_DATE
    )
    VALUES (
        TMP.OUTAGE_NUMBER,
        TMP.START_DATE,
        TMP.TICKET_LOG_ID,
        TMP.LOG_TYPE,
        TMP.LOG_DATE,
        TMP.USER_LOGIN,
        TMP.EMPLOYEE_ID,
        TMP.USER_NAME,
        TMP.REF_MONTH,
        TMP.FILE_NAME,
        TMP.LOAD_DATE,
        TMP.UPDATE_DATE
    );
