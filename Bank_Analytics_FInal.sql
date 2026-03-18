/* =====================================================
   BANK ANALYTICS SQL PROJECT
   Dataset: Bank_Data_Cleaned.csv

   NOTE:
   1. Place the CSV file in your local system.
   2. Update the file path below before execution.
   3. Ensure local_infile is enabled.
===================================================== */

CREATE DATABASE bank_analytics;
USE bank_analytics;
DROP TABLE IF EXISTS bank_data;

-- STEP 2: CREATE TABLE

CREATE TABLE bank_data (

    state_abbr              VARCHAR(10),
    account_id              VARCHAR(50),
    age                     VARCHAR(10),
    bh_name                 VARCHAR(100),
    bank_name               VARCHAR(100),
    branch_name             VARCHAR(100),
    caste                   VARCHAR(50),
    center_id               INT,
    city                    VARCHAR(100),
    client_id               INT,
    client_name             VARCHAR(100),
    close_client            VARCHAR(10),
    closed_date             DATETIME,
    credit_officer_name     VARCHAR(100),
    date_of_birth           DATETIME,
    disb_by                 VARCHAR(100),
    disbursement_date       DATETIME,
    disbursement_date_years VARCHAR(20),
    gender_id               VARCHAR(20),
    home_ownership          VARCHAR(50),
    loan_status             VARCHAR(50),
    loan_transferdate       VARCHAR(50),
    next_meeting_date       DATETIME,
    product_code            VARCHAR(50),
    grade                   VARCHAR(10),
    sub_grade               VARCHAR(10),
    product_id              VARCHAR(50),
    unnamed_27              VARCHAR(50),
    purpose_category        VARCHAR(100),
    region_name             VARCHAR(100),
    religion                VARCHAR(50),
    verification_status     VARCHAR(50),
    state_abbr_0            VARCHAR(10),
    state_name              VARCHAR(100),
    transfer_logic          VARCHAR(10),
    is_delinquent_loan      VARCHAR(5),
    is_default_loan         VARCHAR(5),
    age_t                   INT,
    delinq_2_yrs            INT,
    application_type        VARCHAR(50),
    loan_amount             DOUBLE,
    funded_amount           DOUBLE,
    funded_amount_inv       DOUBLE,
    term                    INT,
    int_rate                DOUBLE,
    total_pymnt             DOUBLE,
    total_pymnt_inv         DOUBLE,
    total_rec_prncp         DOUBLE,
    total_fees              DOUBLE,
    total_rec_int           DOUBLE,
    total_rec_late_fee      DOUBLE,
    recoveries              DOUBLE,
    collection_recovery_fee DOUBLE
);

-- STEP 3: LOAD CSV WITH DATE CONVERSION
/* =====================================================
   NOTE:
   Before running this script, update the file path in
   LOAD DATA section according to your system.
===================================================== */

LOAD DATA LOCAL INFILE 'UPDATE_PATH_HERE/Bank_Data_Cleaned.csv'
INTO TABLE bank_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(
state_abbr, account_id, age, bh_name, bank_name, branch_name,
caste, center_id, city, client_id, client_name, close_client,
@closed_date, credit_officer_name, @date_of_birth,
disb_by, @disbursement_date, disbursement_date_years,
gender_id, home_ownership, loan_status, loan_transferdate,
@next_meeting_date, product_code, grade, sub_grade,
product_id, unnamed_27, purpose_category, region_name,
religion, verification_status, state_abbr_0, state_name,
transfer_logic, is_delinquent_loan, is_default_loan,
age_t, delinq_2_yrs, application_type, loan_amount,
funded_amount, funded_amount_inv, term, int_rate,
total_pymnt, total_pymnt_inv, total_rec_prncp,
total_fees, total_rec_int, total_rec_late_fee,
recoveries, collection_recovery_fee
)
SET
closed_date       = STR_TO_DATE(@closed_date, '%d-%m-%Y %H:%i'),
date_of_birth     = STR_TO_DATE(@date_of_birth, '%d-%m-%Y'),
disbursement_date = STR_TO_DATE(@disbursement_date, '%d-%m-%Y'),
next_meeting_date = STR_TO_DATE(@next_meeting_date, '%d-%m-%Y');

-- =============================================
-- SECTION 1: CORE FINANCIAL KPIs

-- KPI 1: Total Loan Amount Funded
SELECT ROUND(SUM(loan_amount),2) AS total_loan_amount
FROM bank_data;


-- KPI 2: Total Loans Issued
SELECT COUNT(*) AS total_loans_issued
FROM bank_data;


-- KPI 3: Total Funded Amount
SELECT ROUND(SUM(funded_amount),2) AS total_funded_amount
FROM bank_data;


-- KPI 4: Average Interest Rate
SELECT ROUND(AVG(int_rate) * 100,2) AS avg_interest_rate_percent
FROM bank_data;

-- SECTION 2: RISK KPIs

-- KPI 5: Delinquency Distribution
SELECT 
    is_delinquent_loan,
    COUNT(*) AS total_loans
FROM bank_data
GROUP BY is_delinquent_loan;

-- KPI 6: Default Distribution
SELECT 
    is_default_loan,
    COUNT(*) AS total_loans
FROM bank_data
GROUP BY is_default_loan;

-- SECTION 3: SEGMENTATION KPIs

-- KPI 7: Religion-wise Loan Distribution
SELECT 
    religion,
    COUNT(*) AS total_loans,
    ROUND(SUM(loan_amount),2) AS total_amount
FROM bank_data
GROUP BY religion
ORDER BY total_amount DESC;

-- KPI 8: State-wise Loan Distribution
SELECT 
    state_name,
    COUNT(*) AS total_loans,
    ROUND(SUM(loan_amount),2) AS total_amount
FROM bank_data
GROUP BY state_name
ORDER BY total_amount DESC;

-- KPI 9: Monthly Disbursement Trend
SELECT 
    DATE_FORMAT(disbursement_date, '%Y-%m') AS disbursement_month,
    COUNT(*) AS loans_issued,
    ROUND(SUM(loan_amount),2) AS total_disbursed
FROM bank_data
GROUP BY disbursement_month
ORDER BY disbursement_month;

-- KPI 10: Grade-Wise Loan Distribution
SELECT 
    grade,
    COUNT(*) AS total_loans,
    ROUND(SUM(loan_amount),2) AS total_amount
FROM bank_data
GROUP BY grade
ORDER BY total_amount DESC;

-- KPI 11: Default Loan Count
SELECT 
    COUNT(*) AS default_loan_count
FROM bank_data
WHERE is_default_loan = 'Y';

-- KPI 12: Delinquent Client Count
SELECT 
    COUNT(*) AS delinquent_client_count
FROM bank_data
WHERE is_delinquent_loan = 'Y';

-- KPI 13: Delinquent Loan Rate(%)
SELECT 
    ROUND(
        (SUM(CASE WHEN is_delinquent_loan = 'Y' THEN 1 ELSE 0 END) 
        / COUNT(*)) * 100, 2
    ) AS delinquent_rate_percent
FROM bank_data;

-- KPI 14: Default Loan Rate
SELECT 
    ROUND(
        (SUM(CASE WHEN is_default_loan = 'Y' THEN 1 ELSE 0 END) 
        / COUNT(*)) * 100, 2
    ) AS default_rate_percent
FROM bank_data;

-- KPI 15: Loan Status-Wise Distribution
SELECT 
    loan_status,
    COUNT(*) AS total_loans,
    ROUND(SUM(loan_amount),2) AS total_amount
FROM bank_data
GROUP BY loan_status
ORDER BY total_amount DESC;

-- KPI 16: Age Group-Wise Loan Distribution
SELECT 
    age,
    COUNT(*) AS total_loans,
    ROUND(SUM(loan_amount),2) AS total_amount
FROM bank_data
GROUP BY age
ORDER BY total_loans DESC;

-- KPI 17: Average Loan Term
SELECT 
    ROUND(AVG(term),2) AS avg_loan_term_months
FROM bank_data;

-- KPI 18: Non-Verified Loans
SELECT 
    COUNT(*) AS non_verified_loans
FROM bank_data
WHERE verification_status = 'Not Verified';

-- Additional KPI's

-- Additional KPI-1 Total Revenue (Interest + Fees)
SELECT 
    ROUND(SUM(total_rec_int + total_fees),2) AS total_revenue
FROM bank_data;

-- Additonal KPI-2 Recovery Efficiency (%)
SELECT 
    ROUND(
        (SUM(total_rec_prncp) / SUM(loan_amount)) * 100, 2
    ) AS recovery_efficiency_percent
FROM bank_data;

-- Additional KPI-3 Average Loan Size
SELECT 
    ROUND(AVG(loan_amount),2) AS avg_loan_size
FROM bank_data;

-- Additional KPI-4 Top 5 States by Loan Volume
SELECT 
    state_name,
    COUNT(*) AS total_loans
FROM bank_data
GROUP BY state_name
ORDER BY total_loans DESC
LIMIT 5;
commit;

-- ====================================================
-- VIEWS

-- View 1: Core Financial KPIs
CREATE OR REPLACE VIEW vw_core_financial_kpis AS
SELECT 
    COUNT(*) AS total_loans,
    ROUND(SUM(loan_amount),2) AS total_loan_amount,
    ROUND(SUM(funded_amount),2) AS total_funded_amount,
    ROUND(AVG(int_rate) * 100,2) AS avg_interest_rate_percent
FROM bank_data;

SELECT * FROM vw_core_financial_kpis;

-- VIEW-2 RISK ANALYSTIC
CREATE OR REPLACE VIEW vw_risk_kpis AS
SELECT 
    ROUND(
        (SUM(CASE WHEN is_delinquent_loan='Y' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2
    ) AS delinquent_rate_percent,
    
    ROUND(
        (SUM(CASE WHEN is_default_loan='Y' THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2
    ) AS default_rate_percent
FROM bank_data;

-- VIEW-3 MONTHLY TREND
CREATE OR REPLACE VIEW vw_monthly_disbursement AS
SELECT 
    DATE_FORMAT(disbursement_date,'%Y-%m') AS disbursement_month,
    COUNT(*) AS loans_issued,
    ROUND(SUM(loan_amount),2) AS total_disbursed
FROM bank_data
GROUP BY DATE_FORMAT(disbursement_date,'%Y-%m');

-- VIEW-4 RELIGION & STATE SEGEMENT 
CREATE OR REPLACE VIEW vw_segmentation AS
SELECT 
    state_name,
    religion,
    COUNT(*) AS total_loans,
    ROUND(SUM(loan_amount),2) AS total_amount
FROM bank_data
GROUP BY state_name, religion;

-- ======================================================================
-- STORED PROCEDURES

-- PROCEDURE-1 STATE WISE KPI
DELIMITER $$

CREATE PROCEDURE sp_state_kpi(IN input_state VARCHAR(100))
BEGIN
    SELECT 
        COUNT(*) AS total_loans,
        ROUND(SUM(loan_amount),2) AS total_amount,
        ROUND(AVG(int_rate)*100,2) AS avg_interest_rate
    FROM bank_data
    WHERE state_name = input_state;
END $$
DELIMITER ;
CALL sp_state_kpi('PUNJAB');

-- PROCEDURE-2 DATE RANGE KPI
DELIMITER $$

CREATE PROCEDURE sp_date_range_kpi(
    IN start_date DATE,
    IN end_date DATE
)
BEGIN
    SELECT 
        COUNT(*) AS total_loans,
        ROUND(SUM(loan_amount),2) AS total_amount
    FROM bank_data
    WHERE disbursement_date BETWEEN start_date AND end_date;
END $$
DELIMITER ;

CALL sp_date_range_kpi('2018-01-01','2018-12-31');

-- ==================================================================
-- INDEXES

-- Index-1 Disbursement Date (for trends)
CREATE INDEX idx_disbursement_date
ON bank_data(disbursement_date);

-- Index-2 State Name (for segmentation)
CREATE INDEX idx_state_name
ON bank_data(state_name);
commit;



