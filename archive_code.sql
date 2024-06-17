/*----------------------------------------------------*/
-- V1
/*----------------------------------------------------*/
-- EXPLAIN
WITH AA AS (
    SELECT O.NAME,
        O.ACCOUNT_CLASS__C,
        O.CLASS__C,
        O.CLOSEDATE,
        O.CLOSED_WON_DATE__C,
        O.CONTRACT_TYPE__C,
        O.CONTRACT_SHIP_DATE__C,
        O.FORECASTED_GMV__C,
        O.ISCLOSED,
        DATE_TRUNC('MONTH', O.CLOSEDATE) AS CLOSED_MONTH,
        O.ISWON,
        O.OMS_TRANSACTION_ID__C,
        O.PRODUCT_TYPE__C,
        O.RECORDTYPEID,
        O.RECORD_TYPE_NAME__C,
        NVL(O.PRODUCT_TYPE__C, O.RECORD_TYPE_NAME__C) AS PRODUCT_LABEL,
        O.STAGENAME,
        --FROM ACCOUNT
        A.ACCOUNT_TYPE__C,
        A.ACCOUNT_NUMBER__C,
        A.ACTIVE_PRODUCTS__C,
        O.ACCOUNTID,
        A.FULL_ACCOUNT_ID__C,
        A.ID,
        A.NAME AS ACCOUNT_NAME,
        A.NEW_CLASS__C,
        A.PARENTID,
        A.COMPANY_SIZE__C,
        A.CLASSIFICATION__C,
        --WINDOW FUNCTIONS
        ROW_NUMBER() OVER (
            PARTITION BY A.ID
            ORDER BY O.CLOSEDATE
        ) AS ACCOUNT_CLOSEDATE_ROW,
         FIRST_VALUE(PRODUCT_LABEL) OVER (
            PARTITION BY A.ID 
            ORDER BY O.CLOSEDATE
        ) AS FIRST_PRODUCT,
         FIRST_VALUE(A.CLASSIFICATION__C) OVER (
            PARTITION BY A.ID 
            ORDER BY O.CLOSEDATE
        ) AS ACCOUNT_CLASS,
         FIRST_VALUE(A.BILLINGCOUNTRY) OVER (
            PARTITION BY A.ID 
            ORDER BY O.CLOSEDATE
        ) AS COUNTRY
    FROM PROD_ANALYTICS.SALESFORCE.OPPORTUNITY O
        JOIN PROD_ANALYTICS.SALESFORCE.ACCOUNT A ON O.ACCOUNTID = A.ID
    WHERE ( --Preseason and other products that have closed/won
            NOT O.ISDELETED
            AND O.ISCLOSED
            AND O.ISWON
        )
        OR ( --Marketplace products
            UPPER(O.RECORD_TYPE_NAME__C) = 'MARKETPLACE SHIPMENTS' --JUST THOSE ON DIGITAL MARKETPLACE
            AND UPPER(O.STAGENAME) NOT IN ('CANCELED', 'REJECTED')
            AND NOT O.ISDELETED
        )
),
-- GROUP BY ACCOUNT AND CLOSE MONTH AND CALCULATE CHURN AND RETENTION DATES FOR THOSE MONTHS
MG AS (
    SELECT ID AS ACCOUNT_ID,
        CLOSED_MONTH,
        -- RETENTION WINDOW OF N=12
        ----N
        DATEADD('MONTH', 12, CLOSED_MONTH) AS RET_DATE_MAX,
        ----N+1
        DATEADD('MONTH', 13, CLOSED_MONTH) AS CHURN_DATE,
        MAX(ACCOUNT_CLOSEDATE_ROW) AS DEALS_CLOSED,
        MIN(ACCOUNT_CLOSEDATE_ROW) AS MIN_ACCOUNT_CLOSEDATE_ROW,
        MAX(FIRST_PRODUCT) AS FIRST_PRODUCT
    FROM AA
    GROUP BY ACCOUNT_ID,
        CLOSED_MONTH
) --SELECT * FROM MG  ORDER BY 1,2 LIMIT 100;
,
NEW AS (
    SELECT DATE_TRUNC("MONTH", W.WEEK_OF) AS MONTH_IDX,
        COUNT(DISTINCT N.ACCOUNT_ID) AS NEW_ACCOUNTS,
        --window of n-1
        SUM(NEW_ACCOUNTS) OVER (ORDER BY MONTH_IDX ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS NEW_ACCOUNTS_ROLLING
    FROM PROD_ANALYTICS.MP_COMMON_GLOBAL.WEEK W
        LEFT JOIN MG AS N ON DATE_TRUNC("MONTH", W.WEEK_OF) = N.CLOSED_MONTH
        AND N.MIN_ACCOUNT_CLOSEDATE_ROW = 1
    GROUP BY MONTH_IDX
),
RETAINED AS(
    SELECT DATE_TRUNC("MONTH", W.WEEK_OF) AS MONTH_IDX,
        COUNT(DISTINCT R.ACCOUNT_ID) AS RETAINED_ACCOUNTS
    FROM PROD_ANALYTICS.MP_COMMON_GLOBAL.WEEK W
        LEFT JOIN MG AS R ON DATE_TRUNC("MONTH", W.WEEK_OF) BETWEEN R.CLOSED_MONTH AND R.RET_DATE_MAX
    GROUP BY MONTH_IDX
),
CHURNED AS (
    SELECT DATE_TRUNC("MONTH", W.WEEK_OF) AS MONTH_IDX,
        COUNT(
            DISTINCT IFF(R.ACCOUNT_ID IS NULL, C.ACCOUNT_ID, NULL)
        ) AS CHURNED_ACCOUNTS,
        FROM PROD_ANALYTICS.MP_COMMON_GLOBAL.WEEK W
        LEFT JOIN MG AS C ON DATE_TRUNC("MONTH", W.WEEK_OF) = C.CHURN_DATE
        LEFT JOIN MG AS R ON DATE_TRUNC("MONTH", W.WEEK_OF) BETWEEN R.CLOSED_MONTH AND R.RET_DATE_MAX
        AND C.ACCOUNT_ID = R.ACCOUNT_ID
    GROUP BY MONTH_IDX
)
SELECT R.MONTH_IDX AS "CLOSED MONTH",
    N.NEW_ACCOUNTS AS "NEW ACCOUNTS",
    N.NEW_ACCOUNTS_ROLLING AS "NEW ACCOUNTS (ROLLING WINDOW)",
    C.CHURNED_ACCOUNTS AS "CHURNED ACCOUNTS",
    (R.RETAINED_ACCOUNTS -  NVL(N.NEW_ACCOUNTS,0)) AS "RETAINED ACCOUNTS (ROLLING WINDOW, NO NEW)"
FROM RETAINED R
    INNER JOIN CHURNED C ON R.MONTH_IDX = C.MONTH_IDX
    INNER JOIN NEW N ON R.MONTH_IDX = N.MONTH_IDX
WHERE R.MONTH_IDX BETWEEN '2019-01-01' AND CURRENT_DATE()
LIMIT 100
;
