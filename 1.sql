/*
This Oracle SQL query is part of an analysis process for technical support chat consultations. It consists of two main parts:

1. A common table expression (CTE) named "cte" is defined at the beginning of the query, which contains the following details from the table 'TB_TECH_SUPPORT_CHAT_CONSULT':
   - CHAT_ID: Unique identifier for the chat.
   - USER_NAME: Name of the user involved in the chat.
   - EMPLOYEE_ID: Identification number of the employee handling the chat.
   - CHAT_START: Start time of the chat.
   - CHAT_END: End time of the chat.
   - FIRST_INTERACTION: Timestamp of the first interaction in the chat.
   - LAST_INTERACTION: Timestamp of the last interaction in the chat.
   - NEXT_FIRST_INTERACTION: Utilizing the LEAD() window function to fetch the 'FIRST_INTERACTION' value of the next row, ordered by 'CHAT_START'.

2. The main SELECT statement extracts all columns from the CTE, along with an additional calculated column 'QUEUE_JUMP_STATUS':
   - If 'NEXT_FIRST_INTERACTION' is NOT NULL and 'FIRST_INTERACTION' is greater than 'NEXT_FIRST_INTERACTION', it returns 'Delayed', otherwise NULL. This logic is used to detect any delays or queue-jumping situations in the sequence of chats.

The results provide a comprehensive view of chat interactions, allowing identification of any abnormal behavior in the chat handling process. By pinpointing where interactions may have been delayed or jumped ahead in the queue, it can help in monitoring service quality and adherence to support guidelines.

The query is designed for execution in an Oracle SQL environment and is ordered by 'CHAT_START' to ensure chronological analysis.
*/


WITH cte AS ( SELECT CHAT_ID, USER_NAME, EMPLOYEE_ID, CHAT_START, CHAT_END, FIRST_INTERACTION, LAST_INTERACTION, LEAD(FIRST_INTERACTION) OVER (ORDER BY CHAT_START) AS NEXT_FIRST_INTERACTION FROM TB_TECH_SUPPORT_CHAT_CONSULT ORDER BY CHAT_START ) 
SELECT CHAT_ID, USER_NAME, EMPLOYEE_ID, CHAT_START, CHAT_END, FIRST_INTERACTION, LAST_INTERACTION, CASE WHEN NEXT_FIRST_INTERACTION IS NOT NULL AND FIRST_INTERACTION > NEXT_FIRST_INTERACTION THEN 'Delayed' ELSE NULL END AS QUEUE_JUMP_STATUS FROM cte;
