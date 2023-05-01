## RUN LEGACY TO_DATE()
sdf1_full.createOrReplaceTempView('TBP_LOAN_RAW')

# set Legacy timestamp police
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

sql_tbp_loan_raw_to_silver = """
WITH TBP_SILVER (
    SELECT 
        acc_now_delinq,
        addr_state,
        annual_inc,
        delinq_2yrs,
        CASE WHEN dti < 0 THEN 0
         WHEN dti IS NULL THEN 0
         ELSE dti
        END dti,
        earliest_cr_line,
        TO_DATE(earliest_cr_line, 'MMMM-yyyy') AS earliest_cr_line_DT,
        CAST (SUBSTRING(earliest_cr_line, length(earliest_cr_line) - 3, 4) AS INT) earliest_cr_line_year,
        emp_length,
        CAST ( REPLACE ( REPLACE ( REPLACE (REPLACE( REPLACE ( REPLACE(emp_length, '+ years', ''), 'years', '')
                 , '< 1 year', '0') , 'year', '') , ' ', ''), 'n/a', '0') AS INT) emp_length_int,
        funded_amnt,
        funded_amnt_inv,
        grade,
        home_ownership,
        initial_list_status,
        inq_last_6mths,
        installment,
        int_rate,
        issue_d,
        TO_DATE(issue_d, 'MMMM-yyyy') AS issue_d_DT,
        CAST (SUBSTRING(issue_d, length(issue_d) - 3, 4) AS INT) issue_d_year,
        loan_amnt,
        loan_status,
        CASE WHEN loan_status IN ('Charged Off', 'Default', 'Late (31-120 days)', 
              'Does not meet the credit policy. Status:Charged Off' )
              THEN '0'
              ELSE '1'
        END AS loan_status_good_vs_bad,
        CASE 
         WHEN mths_since_last_delinq IS NOT NULL THEN mths_since_last_delinq
         ELSE 0
        END mths_since_last_delinq,
        CASE
         WHEN mths_since_last_record IS NOT NULL THEN mths_since_last_record
         ELSE 0
        END mths_since_last_record,
        purpose,
        recoveries,
        term,
        CAST(REPLACE(term, 'months', '') AS INT) term_int,
        verification_status,
        
        -- REPORT ONLY
        emp_title,
        chargeoff_within_12_mths,
        last_pymnt_amnt,
        last_pymnt_d,
        next_pymnt_d,
        title,
        total_acc,

        -- ML AND SCORECARD
        contract_id, 
        total_pymnt, 
        total_rec_prncp, 
        ROUND(recoveries / funded_amnt, 3) as recovery_rate,
        (funded_amnt - total_rec_prncp) / funded_amnt as credit_conversion_factor_CCF
        
        -- COMPLEMENT COLS
        ,sub_grade
        ,open_acc
        ,pub_rec
        ,total_acc
        ,total_rev_hi_lim
    FROM TBP_LOAN_RAW
    WHERE 1 = 1
    --    AND issue_d LIKE '%2014'
)
SELECT CASE 
    WHEN recovery_rate > 1 THEN 1
    WHEN recovery_rate < 0 THEN 0
    ELSE recovery_rate
   END as recovery_rate_pct
, ROUND( months_between(TO_DATE('2019-03-01', 'yyyy-MM-dd'), issue_d_DT), 1)  as mths_since_issue_d
, ROUND(months_between(TO_DATE('2019-03-01', 'yyyy-MM-dd'), earliest_cr_line_DT), 1)  as mths_since_earliest_credit_line
, TBP_SILVER.*
FROM TBP_SILVER
WHERE 1 = 1
AND issue_d_year in (2015, 2016, 2017, 2018)
"""

sdf2_silver = spark.sql(sql_tbp_loan_raw_to_silver)
# sdf2_silver.printSchema()