# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC # Analysis on Issued Cases in xQ-yyyy (Jan'2024 -> Mar'2025)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load paths for all need tables</strong>

# COMMAND ----------

import pyspark.sql.functions as F
import datetime
import pandas as pd

start_date = '2024-01-01'
end_date   = '2025-09-30'
save_date  =  end_date[:4]+end_date[5:7]
reject_list = ["N","R","X"]
nottaken_list = ["A","N","R","X"]

NB_PATH = f'/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/TPEXNB02VN/'
PARAM_PATH = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPLAN_PARAMETERS/'
RESULT_PATH = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPOLICY_RESULTS/'
UW_PATH = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TAIS_UW_TRIGGER/'
TRACK_PATH = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPT_TRACK_DTL/'
COND_PATH = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TAUW_RESPONSE_UW_CONDITIONS/'

# COMMAND ----------

nb_cols = ['pol_num', 'issue_date', 'distribution_channel_code', 'agt_code', 'submit_channel', 'reporting_date']
#nb_df = spark.read.format('parquet').load(NB_PATH)

spark.read.format('parquet').load(PARAM_PATH).createOrReplaceTempView('tplan_parameters')
spark.read.format('parquet').load(RESULT_PATH).createOrReplaceTempView('tpolicy_results')
spark.read.format('parquet').load(UW_PATH).createOrReplaceTempView('tais_uw_trigger')
spark.read.format('parquet').load(TRACK_PATH).createOrReplaceTempView('tpt_track_dtl')
spark.read.format('parquet').load(COND_PATH).createOrReplaceTempView('tauw_response_uw_conditions')

# Select only policies issued in 2024-2025
#       (F.col('TYPE') == 'ISSUE') &
nb_df = spark.sql(f"""
WITH AGENT_TIER AS (
SELECT  AGT_CODE,
        CASE WHEN TRMN_DT IS NOT NULL THEN '4.Terminated'
             WHEN TRMN_DT IS NULL THEN
                 CASE
                     WHEN COMP_PRVD_NUM IN ('01','02','08','96') THEN '1.Inforce'
                     WHEN COMP_PRVD_NUM = '04' THEN '2.CSC'
                     WHEN COMP_PRVD_NUM IN ('97','98') THEN '3.SM'
                     ELSE '5.Not-Agency'
                 END
        END AS AGT_RLTNSHP,
        COALESCE(
            CASE 
                WHEN MDRT_DESC IN ('TOT', 'COT') THEN 'MDRT'
                ELSE MDRT_DESC
            END,
            MPRO_TITLE
        ) AS MPRO_TITLE,
        image_date
FROM    vn_published_casm_ams_snapshot_db.tams_agents
WHERE   image_date BETWEEN '{start_date}' AND '{end_date}'
), ALL_POLS AS (
SELECT DISTINCT
        pol.POL_NUM, pol.DIST_CHNL_CD, TO_DATE(pol.POL_ISS_DT) AS ISSUE_DATE, 
        CASE WHEN pol.POL_STAT_CD IN ('R','X') THEN 'REJECTED' 
             WHEN pol.POL_STAT_CD IN ('N') THEN 'DECLINED'
             WHEN pol.POL_STAT_CD IN ('A') THEN 'NOT-TAKEN'
        ELSE 'APPROVED' END AS POLICY_STATUS,
        CASE WHEN pol.NB_USER_ID = 'EPOS' THEN 'EPOS' ELSE 'PAPER' END AS SUBMIT_CHANNEL,
        pol.WA_CD_1,
        LAST_DAY(pol.POL_ISS_DT) AS image_date
FROM   vn_published_casm_cas_snapshot_db.tpolicys pol
WHERE  pol.image_date = '{end_date}'
   AND (TO_DATE(pol.POL_ISS_DT) BETWEEN '{start_date}' AND '{end_date}')
   AND pol.POL_STAT_CD NOT IN ('8','A','N','R','X')
       /*((TO_DATE(pol.POL_ISS_DT) BETWEEN '{start_date}' AND '{end_date}') AND
        (pol.POL_STAT_CD IN ('A','N','R','X'))) 
      OR
       ((TO_DATE(pol.POL_ISS_DT) BETWEEN '{start_date}' AND '{end_date}') AND
        (pol.POL_STAT_CD NOT IN ('8','A','N','R','X')))
       )*/
)
SELECT  pol.POL_NUM, pol.DIST_CHNL_CD, pol.ISSUE_DATE, pol.POLICY_STATUS, pol.SUBMIT_CHANNEL,
        pol.WA_CD_1 AS AGT_CODE, NVL(agt1.MPRO_TITLE, agt1.AGT_RLTNSHP) AS AGENT_TIER, pol.image_date
FROM    ALL_POLS pol
   LEFT JOIN
        AGENT_TIER agt1 ON pol.WA_CD_1 = agt1.AGT_CODE AND LAST_DAY(pol.ISSUE_DATE) = COALESCE(agt1.image_date, '{end_date}')

""").dropDuplicates()
#print(nb_df.count())

nb_df.createOrReplaceTempView('nb')
#check_dup(nb_df, "POL_NUM")
#nb_df.groupBy('submit_channel', 'agent_tier').agg(F.count('pol_num').alias('policy_count')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC The loading amount for a policy number, denoted as \\(L(\_i)\\), is calculated using the following equation:
# MAGIC
# MAGIC \\( L(\_i)= \sum_{i=1}^{m} \left( X_i \times (Y_i - 100) \right) \\)
# MAGIC
# MAGIC where<br>
# MAGIC \\( L(\_i) \\): Loading amount of a Policy<br>
# MAGIC \\( _i \\): The \\( _i \\)-th coverage for a given policy number<br>
# MAGIC \\( X_i \\): Face amount (FACE_AMT) for the \\( _i \\)-th coverage<br>
# MAGIC \\( Y_i \\): Face rating (FACE_RATG) for the \\( _i \\)-th coverage, ranging from 1 to 400<br>
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

nb_string = '''
with first_pol as (
select  CLI_NUM, FRST_ISS_DT, row_number() over (partition by CLI_NUM order by FRST_ISS_DT) rn
from    vn_curated_datamart_db.tpolidm_daily a
    inner join
        vn_published_cas_db.tcoverages b on a.POL_NUM = b.POL_NUM
where   POL_STAT_CD not in ('A','N','R','X')
qualify rn = 1),
nb as (
select  distinct
        pol.PO_NUM
        , cvg.CLI_NUM
        , pol.POL_NUM
        , case WHEN nb.DIST_CHNL_CD in ('01', '02', '08', '50', '*') THEN 'AGENCY'
               ELSE 'BANCA'
         end as CHANNEL                 -- 1.Distribution-Channel (Agency/Banca/Other)
        , br.REGION                     -- 2.Distribution-Region
        , br.BR_CODE as DISTRICT        -- 3.Distribution-District
        , nb.AGT_CODE as SELLER        -- 4.Distribution-Seller
        , nb.AGENT_TIER                -- 5.Seller-Title
        , nb.submit_channel AS METHOD  -- 6.Submission-Method (EPOS/Paper/Other)
        , case WHEN po.ID_TYP='5' THEN 'GIO'
                WHEN par.PLAN_CODE is not null THEN 'SIO'
                else 'FMU'
         end SUBMIT_TYPE                -- 7.Submisison-TYPE (GIO/SIO/FMU)
        , nb.POLICY_STATUS              -- 8.Policy Status (Approved/Rejected)
        , TO_DATE(pol.SBMT_DT) SBMT_DT  -- 8.Submission-Date
        , pol.PROD_CAT                  -- 9.Product-Category (T/U/I)
        , pol.PLAN_CODE                 -- 10.Product-Code
        , cast(pln.MLI_PREM_PRD as int) 
        as PREM_TERM                    -- 11.Product-Term
        , po.SEX_CODE GENDER_PO         -- 12.PolicyOwner-Gender
        , floor(datediff(current_date,po.BIRTH_DT)/365.25)
        as AGE_PO                       -- 13.PolicyOwner-Age
        , po.OCCP_CODE                  -- 14.PolicyOwner-Occupation
        , oth1.WEIGHT/(oth1.HEIGHT/100*1.7)
        as BMI_PO                       -- 15.PolicyOnwer-BMI
        , po.SMKR_CODE SMOKER_PO_IND    -- 16.PolicyOnwer-Smoker Status
        , FLOOR(MONTHS_BETWEEN(pol.POL_ISS_DT,fst1.FRST_ISS_DT) / 12)
        as PO_TENURE
        , case WHEN MONTHS_BETWEEN(pol.POL_ISS_DT,fst1.FRST_ISS_DT) > 5 THEN 'Y'
               else 'N'
        end as EXISTING_PO_IND           -- 17.PolicyOwner-Existing Customer (6 months)
        , ins.SEX_CODE as GENDER_LA      -- 19.Insured-Gender
        , floor(datediff(current_date,ins.BIRTH_DT)/365.25)
        as AGE_LA                        -- 20.Insured-Age
        , ins.OCCP_CODE as OCCP_LA       -- 21.Insured-Occupation
        , oth2.WEIGHT/(oth2.HEIGHT/100*1.7)
        as BMI_LA                        -- 22.Insured-BMI
        , ins.SMKR_CODE as SMOKER_LA_IND -- 23.Insured-Smoker Status
        , FLOOR(MONTHS_BETWEEN(pol.POL_ISS_DT,fst2.FRST_ISS_DT) / 12)
        as LA_TENURE
        , case WHEN MONTHS_BETWEEN(pol.POL_ISS_DT,fst2.FRST_ISS_DT) > 5 THEN 'Y'
               else 'N'
        end as EXISTING_LA_IND           -- 24.Insured-Existing Customer (6 months)
        , case WHEN pol.ISS_USER_ID = 'CAS' THEN True else False end
        as STP_FLAG                      -- 34.STP-Flag
        , TO_DATE(nb.issue_date) 
        as ISSUE_DATE                    -- 35.Issue-Date
        , CAST(pol.TOT_APE AS INT) 
        as TOT_APE                       -- 36.Total APE
        , CAST(pol.BASE_APE AS INT) 
        as BASE_APE                      -- 37.Base APE
        , CAST(pol.RID_APE AS INT)
        as RIDER_APE                     -- 38. Rider APE
        , nb.image_date as IMAGE_DATE    -- 40.Snapshot Date                  
from    vn_published_cas_db.tcoverages cvg inner join
        vn_curated_datamart_db.tpolidm_daily pol on cvg.POL_NUM = pol.POL_NUM inner join
        nb on pol.POL_NUM = nb.POL_NUM inner join
        vn_published_ams_db.tams_agents agt on nb.AGT_CODE = agt.AGT_CODE inner join
        vn_published_ams_db.tams_branch_by_chnl br on agt.BR_CODE = br.BR_CODE inner join
        vn_published_ams_db.tams_locations loc on agt.LOC_CODE = loc.LOC_CD left join
        vn_published_cas_db.tclient_details po on pol.PO_NUM = po.CLI_NUM left join
        vn_published_cas_db.tclient_other_details oth1 on po.CLI_NUM = oth1.CLI_NUM left join
        first_pol fst1 on pol.PO_NUM = fst1.CLI_NUM left join
        vn_published_cas_db.tclient_details ins on pol.INSRD_NUM = ins.CLI_NUM left join
        vn_published_cas_db.tclient_other_details oth2 on po.CLI_NUM = oth2.CLI_NUM left join
        first_pol fst2 on pol.INSRD_NUM = fst2.CLI_NUM left join
        vn_published_cas_db.tplans pln on pol.PLAN_CODE = pln.PLAN_CODE left join
        tplan_parameters par on pol.PLAN_CODE = par.PLAN_CODE and par.PARM_TYP = 'NB_SIMPLIFY_PRODUCT'
where   1=1
        -- Individual policies only
    and po.ID_TYP <> '5'
        -- Exclude low value and single term products
    --and pol.PLAN_CODE not in ('PN001','PA007','PA008','FDB01','BIC01','BIC02','BIC03','BIC04','CA360','CI360','CX360','MI007')
)
select  *
from    nb
'''
nb_pols = sql_to_df(nb_string, 1, spark)
nb_pols.createOrReplaceTempView("nb_pol_list")
#print("Issued Cases: ", nb_pols.count(), nb_pols.select('POL_NUM').distinct().count())

benefits_string = '''
WITH BENEFITS AS (
SELECT  POL_NUM, 
        CONCAT_WS('/', 
        COLLECT_SET(CASE WHEN cvg.CVG_TYP='B' THEN cvg.PLAN_CODE END)) AS BENEFIT_BASIC,
        CONCAT_WS('/', 
        COLLECT_SET(CASE WHEN cvg.CVG_TYP='R' THEN PROD_TYP END)) AS BENEFIT_RIDER
FROM    vn_published_cas_db.tcoverages cvg
   INNER JOIN
        vn_published_cas_db.tplans pln ON cvg.plan_code=pln.plan_code AND cvg.vers_num=pln.vers_num
GROUP BY
        POL_NUM
) SELECT POL_NUM, COALESCE(BENEFIT_BASIC,'NONE') AS BENEFIT_BASIC, COALESCE(BENEFIT_RIDER,'NONE') AS BENEFIT_RIDER
FROM BENEFITS
'''
ben_pols = sql_to_df(benefits_string, 1, spark)

loading_string = '''
with coverages as (
select  cvg.POL_NUM, cvg.CLI_NUM, nb.issue_date
from    nb inner join
        vn_published_cas_db.tcoverages cvg on nb.POL_NUM = cvg.POL_NUM
group by cvg.POL_NUM, cvg.CLI_NUM, nb.issue_date),
loading1 as (
select  POL_NUM, CLI_NUM, TO_DATE(TRXN_DT) TRXN_DT, 
        CONCAT_WS('\n', collect_set(concat(GRP_LIM_TYP, ': ', cast(cast(RATE as int) as string), '%'))) load
from    vn_published_cas_db.twrk_aprv_lim
where   1=1
    --and LINK_TYP='I'
    and RATE > 0
group by POL_NUM, CLI_NUM, TRXN_DT),
loading2 as (
select  cvg.POL_NUM, cvg.CLI_NUM, ld.load as LOADING --collect_set(concat(cast(ld.CLI_NUM as string), ' - ', cast(ld.load as string))) LOADING_AMT
from    coverages cvg inner join
        loading1 ld on cvg.POL_NUM = ld.POL_NUM and cvg.CLI_NUM = ld.CLI_NUM
where   cvg.issue_date >= ld.TRXN_DT  
group by cvg.POL_NUM, cvg.CLI_NUM, ld.load)
select  *
from    loading2
'''
loading_pols = sql_to_df(loading_string, 1, spark)
#print("Loading: ", loading_pols.count(), loading_pols.select('POL_NUM').distinct().count())

leak_string = '''
with leakage as (
select  case WHEN ais.POL_NUM is not null THEN 'AIS'
             WHEN rslt.RULE_ID = 'P022' THEN 'Digtexx'
             else 'Admin'
        end LEAK_SOURCE
        , case WHEN ais.POL_NUM is not null THEN 1
             WHEN rslt.RULE_ID = 'P022' THEN 2
             else 3
        end LEAK_RANK
        , rslt.POL_NUM
        , rslt.CLI_NUM
        , rslt.RULE_ID
        , rslt.ASSIGN_TEAM
        , case  WHEN rslt.RULE_ID='A002' THEN 'Active in agent blacklist with violation [x]'
                WHEN rslt.RULE_ID='A003' THEN 'Product [x] is not allowed [y]'
                WHEN rslt.RULE_ID='A006' THEN 'This policy cannot be shared with other agents ([x] - agent who is client)'
                WHEN rslt.RULE_ID='A009' THEN 'Agent [x] belong to hot location please checking MC limit'
                WHEN rslt.RULE_ID='C003' THEN 'Mobile phone of PO is as the same as Agent [x]'
                WHEN rslt.RULE_ID='C011' THEN 'In policy [x] has restriction code [y],'
                WHEN rslt.RULE_ID='C012' THEN 'Phone number is invalid [x]'
                WHEN rslt.RULE_ID='C026' THEN 'Risk occupation (Class: [x] ? Code [y])'
                WHEN rslt.RULE_ID='C031' THEN 'Invalid face amount ([list of plan code [x] ([y])])'
                WHEN rslt.RULE_ID='C035' THEN 'Product [x]/this client must be <= (O) benefit. Pls contact Role 7 for approval'
                WHEN rslt.RULE_ID='C049' THEN 'Total ADD benefits [x] over 24 bil'
                WHEN rslt.RULE_ID='C052' THEN '[x] Product is already existed'
                WHEN rslt.RULE_ID='C055' THEN 'Total MC [x]>must not exceed [y]k/day'
                WHEN rslt.RULE_ID='C059' THEN 'Client already exist [x]'
                WHEN rslt.RULE_ID='E001' THEN '[x] is unsuccess. ORA-[y]: [z]'
                WHEN rslt.RULE_ID='P012' THEN 'Effective Date should be within 3 working days-not before [x] & after [y]'
                WHEN rslt.RULE_ID='P015' THEN 'Underpayment [x]'
                WHEN rslt.RULE_ID='P019' THEN 'In policy [x] has Medical check/Client medical doc or ME/RC pending case'
                WHEN rslt.RULE_ID='P028' THEN 'redundant premium [x]'
                WHEN rslt.RULE_ID='P029' THEN 'Out of your limit. Pls contact role [x] for approval'
                WHEN rslt.RULE_ID='P034' THEN 'There is a mismatch number of Medical doc. Total doc submitted = [x] files and imported into iEDMS = [y] files.'
                WHEN rslt.RULE_ID='P038' THEN 'Total Life Risk([x]) is over 10 bil'
                WHEN rslt.RULE_ID='P039' THEN 'Total MC Risk([x]) is over 3 mil'
                WHEN rslt.RULE_ID='P044' THEN 'Lack Agent report or FNA [x]. Please check'
                else ERR_EN_MSG
        end RULE_DESC
        , ais.RULE_NM
        , TO_DATE(TRXN_DT) TRXN_DT
from    nb left join
        tpolicy_results rslt on nb.POL_NUM = rslt.POL_NUM left join
        tais_uw_trigger ais on rslt.POL_NUM = ais.POL_NUM
where   1=1
    and  nb.issue_date >= TO_DATE(TRXN_DT)
)
select  *, nvl(RULE_NM,RULE_DESC) LEAK_REASON,
        row_number() over (partition by POL_NUM order by LEAK_RANK) rn
from    leakage
qualify rn = 1
'''
leakage_reasons = sql_to_df(leak_string, 1, spark).select('POL_NUM','CLI_NUM','LEAK_SOURCE','LEAK_REASON')
leak_pols = leakage_reasons.groupBy('POL_NUM','LEAK_SOURCE').agg(F.count('LEAK_REASON').alias('LEAK_REASON'))
#print("Leakage :", leak_pols.count(), leak_pols.select('POL_NUM').distinct().count())

nbpr_sql = '''
select  nb.POL_NUM as POL_NUM
        , CONCAT_WS(' > ',collect_set(tpt.POS_NB_PROG)) as NBPR_REASON
        , count(tpt.PNP_NUM) as NBPR_REASON_LINE_COUNT
        , max(TO_DATE(tpt.TRXN_DATE)) LAST_NBPR_DATE
from    nb inner join 
        vn_published_cas_db.tpt_nb_progress_dtl tpt on nb.POL_NUM = tpt.POL_NUM left join
        tpt_track_dtl trk on tpt.POL_NUM = trk.POL_NUM and tpt.POS_NUM = trk.POS_NUM and tpt.TRXN_DATE = trk.TRXN_DATE
where   1=1
    and trk.TRXN_DESC like '%Pending code:%'
    and nb.issue_date >= trk.TRXN_DATE
group by nb.POL_NUM
'''
nbpr_reasons = sql_to_df(nbpr_sql, 1, spark).select('POL_NUM','NBPR_REASON','LAST_NBPR_DATE')
nbpr_pols = nbpr_reasons.groupBy('POL_NUM').agg(F.count('NBPR_REASON').alias('NBPR_REASON'), F.max('LAST_NBPR_DATE').alias('LAST_NBPR_DATE'))
#print("NBPR: ", nbpr_pols.count(), nbpr_pols.select('POL_NUM').distinct().count())

ais_string = '''
WITH auw AS (
SELECT  POL_NUM,
        UNDERWRITING_STATUS,
        TO_DATE(CREATE_DATE) AS CREATE_DATE,
        ROW_NUMBER() OVER (PARTITION BY POL_NUM ORDER BY CREATE_DATE DESC, REQ_ID DESC) rn
FROM    vn_published_cas_db.tauw_response_masters 
QUALIFY rn = 1
)
SELECT  nb.POL_NUM
        , CONCAT_WS('/', COLLECT_SET(
          CASE WHEN auw.UNDERWRITING_STATUS = 'CLEAN_CASE' THEN 'Clean'
               --WHEN auw.UNDERWRITING_STATUS IN ('REFERRED','REF_ADD_EVID') THEN 'Refer to UW'
               ELSE 'Refer to UW'    -- Revised/Postponed/Declined
          END)
          )
          AS AIS_DECISION
        , CONCAT_WS('/', COLLECT_SET(auw.UNDERWRITING_STATUS)) AS FINAL_UW_DECISION
        , '' FINAL_UW_EXCLUDE
        , TO_DATE(max(auw.CREATE_DATE)) AS FINAL_UW_DECISION_DATE
        , CONCAT_WS('/', collect_set(ais.TRIGGER_LVL)) AS TRIGGER_LEVEL
        , CONCAT_WS('/', collect_set(ais.TRIGGER_CD)) AS TRIGGER_CODES
        , CONCAT_WS('/', collect_set(ais.TRIGGER_DESC)) AS TRIGGER_DESC
        , CONCAT_WS('/', collect_set(ais.RULE_NM)) as AIS_RULE_NAMES
FROM    nb 
   INNER JOIN
        auw ON nb.POL_NUM = auw.POL_NUM
   LEFT JOIN
        tais_uw_trigger ais ON nb.POL_NUM = ais.POL_NUM
WHERE   nb.issue_date >= TO_DATE(auw.CREATE_DATE)
GROUP BY
        nb.POL_NUM--, auw.UNDERWRITING_STATUS
'''
ais_pols = sql_to_df(ais_string, 1, spark)
ais_pols = ais_pols.withColumn(
        "HIT_RULE",
        F.when(F.length(F.col("AIS_RULE_NAMES")) > 1, 1)
        .otherwise(0)
)
#ais_pols.filter(F.col("POL_NUM") == "2806711857").display()
#print("AIS :", ais_pols.count(), ais_pols.select('POL_NUM').distinct().count())

admin_string = '''
WITH results_dedup AS (
  SELECT DISTINCT
         POL_NUM,
         CLI_NUM,
         RULE_ID,
         err_en_msg, TYPE, ASSIGN_TEAM, trxn_dt, rerun_seq
         ,DENSE_RANK() OVER (PARTITION BY POL_NUM ORDER BY rerun_seq DESC) AS rank
  FROM   vn_published_cas_db.tpolicy_results
  QUALIFY rank = 1
), order_results AS (
  SELECT POL_NUM,
         CLI_NUM,
         RULE_ID,
         err_en_msg,
         CASE 
           WHEN TYPE = 'C' THEN 'C-Client'
           WHEN TYPE = 'P' THEN 'P-Policy'
           WHEN TYPE = 'A' THEN 'A-Agent'
           ELSE TYPE
         END AS TYPE,
         ASSIGN_TEAM,
         ROW_NUMBER() OVER(PARTITION BY POL_NUM ORDER BY POL_NUM) AS rn
  FROM   results_dedup
),
sorted_remarks AS (
  SELECT POL_NUM, CLI_NUM, RULE_ID, TYPE, ASSIGN_TEAM, err_en_msg
         --CONCAT(CAST(rn AS STRING), '.', err_en_msg) AS sorted_err_en_msg
  FROM order_results
)
SELECT POL_NUM,
       CONCAT_WS('/', COLLECT_SET(TYPE)) AS TYPES,
       CONCAT_WS('/', COLLECT_SET(RULE_ID)) AS ADMIN_RULES,
       CONCAT_WS('>', COLLECT_LIST(err_en_msg)) AS ADMIN_REMARKS,
       CONCAT_WS('/', COLLECT_SET(ASSIGN_TEAM)) AS ASSIGN_TEAM
FROM sorted_remarks
GROUP BY POL_NUM
'''
admin_pols = sql_to_df(admin_string, 1, spark)
#print("Admin :", ais_pols.count(), ais_pols.select('POL_NUM').distinct().count())


# COMMAND ----------

# DBTITLE 1,Adding Digitexx Remarks/Logs
digitexx_string = '''
WITH extracted_remarks AS (
    SELECT
        POL_NUM,
        -- Extract all text within <REMARK>...</REMARK> tags
        regexp_extract(TEXT_LOG, '(?i)<REMARKS>(.*?)</REMARKS>', 1) AS remarks_content
    FROM
        vn_published_cas_db.tdigitexx_xml_logs
),
split_remarks AS (
    SELECT
        POL_NUM,
        -- Split extracted content into individual remarks
        split(regexp_replace(remarks_content, '(?i)</?REMARK>', ''), '</REMARK><REMARK>') AS remarks_array
    FROM
        extracted_remarks
),
exploded_remarks AS (
    SELECT
        POL_NUM,
        -- Explode to get each remark as a separate row
        explode(remarks_array) AS remark_text
    FROM
        split_remarks
)
SELECT
    POL_NUM,
    -- Concatenate all non-empty trimmed remarks into a single string separated by '/'
    TRIM(CONCAT_WS(' / ', filter(collect_list(remark_text), x -> x != ''))) AS DIGITEXX_REMARKS
FROM
    exploded_remarks
GROUP BY
    POL_NUM
'''
digitexx_remarks = sql_to_df(digitexx_string, 1, spark)

# COMMAND ----------

# DBTITLE 1,Add Claim history information
claim_df = spark.sql(f"""
-- retrieve claim payment dates
WITH results_dedup AS (
  SELECT DISTINCT
         POL_NUM,
         CLI_NUM,
         RULE_ID,
         err_en_msg, TYPE, ASSIGN_TEAM, trxn_dt, rerun_seq
         ,DENSE_RANK() OVER (PARTITION BY POL_NUM ORDER BY rerun_seq DESC) AS rank
  FROM   vn_published_cas_db.tpolicy_results
  QUALIFY rank = 1
), order_results AS (
  SELECT POL_NUM,
         CLI_NUM,
         RULE_ID,
         err_en_msg,
         CASE 
           WHEN TYPE = 'C' THEN 'C-Client'
           WHEN TYPE = 'P' THEN 'P-Policy'
           WHEN TYPE = 'A' THEN 'A-Agent'
           ELSE TYPE
         END AS TYPE,
         ASSIGN_TEAM,
         ROW_NUMBER() OVER(PARTITION BY POL_NUM ORDER BY POL_NUM) AS rn
  FROM   results_dedup
),
sorted_remarks AS (
  SELECT POL_NUM, CLI_NUM, RULE_ID, TYPE, ASSIGN_TEAM, err_en_msg
  FROM order_results
),
clm_his AS (
SELECT POL_NUM, CLI_NUM,
       CONCAT_WS('/', COLLECT_SET(TYPE)) AS TYPES,
       CONCAT_WS('>', COLLECT_LIST(err_en_msg)) AS ADMIN_REMARKS
FROM sorted_remarks
WHERE   TYPE='C-Client'
  AND   err_en_msg like '%Client has claim history%'
GROUP BY POL_NUM, CLI_NUM
), 
clm_type as (
select  cast(fld_valu as int) AS clm_code
        , fld_valu_desc AS benefit
        ,case when cast(fld_valu as int) in (3,7,11,31,36,50,51) then 'Quyền lợi trợ cấp-Medicash'
          when cast(fld_valu as int) in (27,28,29,40,41,42) then 'Quyền lợi điều trị-Healthcare'
          when cast(fld_valu as int) in (9,38,45) then 'Quyền lượi thai sản-Female Benefits' 
          else 'Quyền lợi lớn khác'
     end AS clm_benefit_type
from    vn_published_cas_db.tfield_values
where   fld_nm='CLM_CODE'
), 
t_dnr_sub_a as (
	select
		payee
		,max(pmt_dt) pmt_dt
	from
		vn_published_cas_db.tdnr_details
	where
		dnr_stat_cd <> 'X'
		and pmt_stat_cd not in ('N','W')
		and payo_reas in ('912','913','914','915','916','917','918','919','920','921','927','928','929','930')							
	group by
		payee
),
t_dnr_sub_b as (
	select
		payee
		,pmt_dt
		,max(payo_type) payo_type
	from
		vn_published_cas_db.tdnr_details
	where
		dnr_stat_cd <> 'X'
		and pmt_stat_cd not in ('N','W')
		and payo_reas in ('912','913','914','915','916','917','918','919','920','921','927','928','929','930')							
	group by
		payee
		,pmt_dt
),
t_dnr AS (
select
	a.payee
	,case
		when c.direct_billing = 'Y' then c.clm_recv_dt
		when c.payo_typ = 'ANM' then c.clm_aprov_dt
		when c.payo_typ is null and c.case_owner in ('INSMARTHCM','INSMARTHN') then date_add(c.clm_aprov_dt,3)
		when c.payo_typ in ('TF','PO') then date_add(c.clm_aprov_dt,3)
		when a.pmt_dt is null then c.clm_aprov_dt
		else a.pmt_dt
	end pmt_dt
	,b.payo_type
from	
	t_dnr_sub_a a
	inner join t_dnr_sub_b b on	(a.payee = b.payee and a.pmt_dt = b.pmt_dt)
    inner join vn_published_cas_db.tclaim_details c on a.payee = c.clm_id
),
all_claims as (
SELECT	CLM_ID, POL_NUM, CLI_NUM, CLM_CODE, CLM_EFF_DT, CLM_APP_DT, CLM_APROV_AMT
FROM   	vn_published_cas_db.tclaim_details
UNION
SELECT	CLM_ID, POL_NUM, con.CLI_CNSLDT_NUM AS CLI_NUM, CLM_CODE, CLM_EFF_DT, CLM_APP_DT, CLM_APROV_AMT
FROM   	vn_published_cas_db.tclaim_details clm
  INNER JOIN
        vn_published_cas_db.tclient_consolidations con ON clm.CLI_NUM = con.CLI_NUM
)
SELECT DISTINCT
    clm.CLM_ID
    ,clm.POL_NUM
    ,nb.CLI_NUM
    ,YEAR(clm.CLM_EFF_DT) AS CLM_YR
	  ,SUBSTR(TO_DATE(clm.CLM_EFF_DT),1,7) AS CLM_MTH
    ,SUBSTR(TO_DATE(t_dnr.pmt_dt),1,7) AS PMT_MTH
    ,CAST(NVL(clm.CLM_APROV_AMT,0) AS INT) AS CLM_AMT
    ,typ.clm_benefit_type
    ,CASE WHEN clm.CLM_EFF_DT > nb.ISSUE_DATE THEN 0 ELSE 1 END AS VALID_HIS_CLM_IND
FROM all_claims clm LEFT JOIN
     clm_his his ON clm.cli_num = his.cli_num LEFT JOIN
     nb_pol_list nb on his.POL_NUM = nb.POL_NUM LEFT JOIN
     t_dnr ON clm.clm_id=t_dnr.payee LEFT JOIN
     clm_type typ ON clm.clm_code=typ.clm_code
WHERE 1=1
""")

claim_sum = claim_df.groupBy('CLI_NUM') \
    .agg(
        F.count('CLM_ID').alias('TOTAL_CLAIMS'),
        F.sum('CLM_AMT').alias('TOTAL_CLAIM_AMOUNT'),
        F.concat_ws('/',F.collect_set(F.col('clm_benefit_type'))).alias('CLAIM_BENEFITS'),
        F.countDistinct(F.col("CLM_YR")).alias('CLAIM_YEARS'),
        F.concat_ws('/',F.collect_set(F.col('CLM_MTH'))).alias('CLAIM_MONTHS'),
        F.count(F.when(F.col('clm_benefit_type') == 'Quyền lợi trợ cấp-Medicash', F.col('pol_num'))).alias('MC_CLAIMS'),
        F.sum(F.when(F.col('clm_benefit_type') == 'Quyền lợi trợ cấp-Medicash', F.col('clm_amt'))).alias('MC_CLAIM_AMOUNT'),
        F.count(F.when(F.col('clm_benefit_type') == 'Quyền lợi điều trị-Healthcare', F.col('pol_num'))).alias('HCR_CLAIMS'),
        F.sum(F.when(F.col('clm_benefit_type') == 'Quyền lợi điều trị-Healthcare', F.col('clm_amt'))).alias('HCR_CLAIM_AMOUNT'),
        F.count(F.when(F.col('clm_benefit_type') == 'Quyền lợi thai sản-Female Benefits', F.col('pol_num'))).alias('FEMALE_CLAIMS'),
        F.sum(F.when(F.col('clm_benefit_type') == 'Quyền lợi thai sản-Female Benefits', F.col('clm_amt'))).alias('FEMALE_CLAIM_AMOUNT'),
        F.count(F.when(F.col('clm_benefit_type') == 'Quyền lợi lớn khác', F.col('pol_num'))).alias('OTHER_CLAIMS'),
        F.sum(F.when(F.col('clm_benefit_type') == 'Quyền lợi lớn khác', F.col('clm_amt'))).alias('OTHER_CLAIM_AMOUNT'),
        F.max(F.col("VALID_HIS_CLM_IND")).alias("VALID_HIS_CLM_IND")
).withColumn(
    'CLAIM_AMOUNT_PER_YEAR',
    F.when(F.col('CLAIM_YEARS') > 0, F.col('TOTAL_CLAIM_AMOUNT') / F.col('CLAIM_YEARS')).otherwise(0)
)

# COMMAND ----------

final_df = (
    nb_pols.alias('nb')
    .join(ben_pols.alias('bn'), on='POL_NUM', how='left')
    .join(loading_pols.alias('ld'), on=['POL_NUM','CLI_NUM'], how='left')
    .join(leakage_reasons.alias('lk'), on=['POL_NUM','CLI_NUM'], how='left')
    .join(nbpr_reasons.alias('br'), on='POL_NUM', how='left')
    .join(ais_pols.alias('ai'), on='POL_NUM', how='left')
    .join(admin_pols.alias('ad'), on='POL_NUM', how='left')
    .join(digitexx_remarks.alias('di'), on='POL_NUM', how='left')
    .join(claim_sum.alias('cl'), on='CLI_NUM', how='left')
).select(
    'nb.POL_NUM',
    'nb.PO_NUM',
    'nb.CLI_NUM',
    'CHANNEL',                      # 1.Distribution-Channel
    'REGION',                       # 2.Distribution-District
    'DISTRICT',                     # 3.Distribution-Seller
    'SELLER',                       # 4.Distribution-Seller
    'AGENT_TIER',                   # 5.Seller's Title
    'METHOD',                       # 5.Submission-Method (EPOS/Paper/Other)
    'SUBMIT_TYPE',                  # 6.Submisison-Type (GIO/SIO/FMU)
    'POLICY_STATUS',                # 7.Policy Status
    'SBMT_DT',                      # 7.Submission-Date
    'PROD_CAT',                     # 8.Product-Category (T/U/I)
    'PLAN_CODE',                    # 9.Product-Code
    'PREM_TERM',                    # 10.Product-Term        
    'BENEFIT_BASIC',                # 11.Benefit-Basic
    'BENEFIT_RIDER',                # 12.Benefit-Rider(s)
    'GENDER_PO',                    # 13.PolicyOwner-Gender
    'AGE_PO',                       # 14.PolicyOwner-Age
    'OCCP_CODE',                    # 15.PolicyOwner-Occupation
    'BMI_PO',                       # 16.PolicyOnwer-BMI
    'SMOKER_PO_IND',                # 17.PolicyOnwer-Smoker Status
    'PO_TENURE',
    'EXISTING_PO_IND',              # 18.PolicyOwner-Existing Customer
    'GENDER_LA',                    # 19.Insured-Gender
    'AGE_LA',                       # 20.Insured-Age
    'OCCP_LA',                      # 21.Insured-Occupation
    'BMI_LA',                       # 22.Insured-BMI
    'SMOKER_LA_IND',                # 23.Insured-Smoker Status
    'LA_TENURE',
    'EXISTING_LA_IND',              # 24.Insured-Existing Customer
    'LEAK_REASON',                  # 25.Leakage-Reason
    'LEAK_SOURCE',                  # 26.Leakage-Source
    'NBPR_REASON',                  # 27.NBPR-Reason
    'LAST_NBPR_DATE',               # 28.NBPR-Date
    'AIS_DECISION',                 # 29.AIS-Decision
    'FINAL_UW_DECISION',            # 30.Final UW Decision-Decision
    'LOADING',                      # 31.Final UW Decision-Loading amount
    'FINAL_UW_EXCLUDE',             # 32.Final UW Decision-Exclud Reason 
    'FINAL_UW_DECISION_DATE',       # 33.Final UW Decision-Date
    'STP_FLAG',                     # 34.STP-Flag
    'nb.ISSUE_DATE',                # 35.Issue-Date
    'TOT_APE',                      # 36.Total APE
    'BASE_APE',                     # 37.Base APE
    'RIDER_APE',                    # 38.Rider APE
    'TRIGGER_LEVEL',                # 39.RISK vs RULE
    'TRIGGER_CODES',                # 40.Trigger code
    'TRIGGER_DESC',                 # 41.Trigger description
    'AIS_RULE_NAMES',               # 42.Rule name
    'HIT_RULE',                     # 43.Rule hitting indicator
    'TYPES',                        # 44.Rule type
    'ADMIN_RULES',                  # 45.Rule ID    
    'ADMIN_REMARKS',                # 46.Message
    'ASSIGN_TEAM',                  # 47.Assign to team
    'DIGITEXX_REMARKS',             # 48.Digitexx Remarks
    'TOTAL_CLAIMS',                 # 49.# of claims in history
    'TOTAL_CLAIM_AMOUNT',           # 50.Amount of claims in history
    'CLAIM_BENEFITS',               # 51. of claims in history
    'CLAIM_MONTHS',                 # 52.# of months of claims in history
    'CLAIM_YEARS',                  # 53.# of years of claims in history
    'CLAIM_AMOUNT_PER_YEAR',        # 54.Amount of claim per year in history
    'MC_CLAIMS',                    # 55.# of MC claims in history
    'MC_CLAIM_AMOUNT',              # 56.Amount of MC claims in history
    'HCR_CLAIMS',                   # 57.# of HCR claims in history
    'HCR_CLAIM_AMOUNT',             # 58.Amount of HCR claims in history
    'FEMALE_CLAIMS',                # 59.# of Female benefit claims in history
    'FEMALE_CLAIM_AMOUNT',          # 60.Amount of Female benefit claims in history
    'OTHER_CLAIMS',                 # 61.# of other/major claims in history
    'OTHER_CLAIM_AMOUNT',           # 62.Amount of other/major claims in history
    'VALID_HIS_CLM_IND',            # 63.Indicator whether client has claim history
    'IMAGE_DATE'                    # 64.Snapshot Date
)

final_df = final_df.fillna(
    {'BENEFIT_BASIC': 'NONE', 'BENEFIT_RIDER': 'NONE', 'HIT_RULE': 0, 'TOTAL_CLAIMS': 0, 'TOTAL_CLAIM_AMOUNT': 0, 'CLAIM_YEARS': 0,
     'CLAIM_AMOUNT_PER_YEAR': 0, 'CLAIM_BENEFITS': 'NONE', 'MC_CLAIMS': 0, 'MC_CLAIM_AMOUNT': 0, 'HCR_CLAIMS': 0, 'HCR_CLAIM_AMOUNT': 0,
     'FEMALE_CLAIMS': 0, 'FEMALE_CLAIM_AMOUNT': 0, 'OTHER_CLAIMS': 0, 'OTHER_CLAIM_AMOUNT': 0, 'VALID_HIS_CLM_IND': 0
     }
).dropDuplicates()

# If ASSIGN_TEAM is blank but there are RULES then assign to `UW`
final_df = final_df.withColumn(
    "ASSIGN_TEAM",
    F.when(
        (F.col("ASSIGN_TEAM").isNull() | (F.col("ASSIGN_TEAM") == ""))
        & (F.col("AIS_RULE_NAMES").isNotNull() & (F.col("AIS_RULE_NAMES") != "")),
        "UW"
    ).otherwise(F.col("ASSIGN_TEAM"))
)

# Add TAT time (days) from Submission to Final UW Decision to Issuance
final_df = final_df.withColumn(
    "TAT_SBMT_FUD", F.datediff(F.col("FINAL_UW_DECISION_DATE"), F.col("SBMT_DT"))
).withColumn(
    "TAT_FUD_ISSUE", F.datediff(F.col("ISSUE_DATE"), F.col("FINAL_UW_DECISION_DATE"))
).withColumn(
    "TAT_SBMT_ISSUE", F.datediff(F.col("ISSUE_DATE"), F.col("SBMT_DT"))
)

#check_dup(final_df, "CLI_NUM")
#print("Final:", final_df.count(), final_df.select('POL_NUM').distinct().count())

# COMMAND ----------

# final_df.write.mode(
#     "overwrite"
#     ).option(
#         "partitionOverwriteMode", "dynamic"
#     ).partitionBy(
#         "image_date"
#     ).parquet(
#         f"/mnt/lab/vn/project/VN_NB/NB_ANALYSIS/"
#     )

# final_df.filter(
#     F.col("RULE_NAMES").isNotNull()
#  & (F.col("RULE_NAMES") != "")
# ).groupBy(
#     "AIS_DECISION",
#     "ASSIGN_TEAM"
# ).agg(
#     F.count("POL_NUM").alias("POL_COUNT")
# ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Store and check

# COMMAND ----------

final = final_df.toPandas()

print(final.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC Break base APE into small bands<br>
# MAGIC `< 8m`<br>
# MAGIC `8 - 10m`<br>
# MAGIC `10 - 12m`<br>
# MAGIC `12 - 15m`<br>
# MAGIC `15 - 17m`<br>
# MAGIC `17 - 20m`<br>
# MAGIC `20 - 25m`<br>
# MAGIC `25 - 30m`<br>
# MAGIC `30 - 35m`<br>
# MAGIC `35 - 40m`<br>
# MAGIC `40 - 50m`<br>
# MAGIC `50 - 70m`<br>
# MAGIC `70 - 100m`<br>
# MAGIC `>100m`

# COMMAND ----------

# DBTITLE 1,Categorize APE bands
bin_labels = [
    # LA age band
    ('AGE_LA',
     [-float('inf'), 18, 25, 30, 35, 40, 45, 50, 55, float('inf')],
     ['a.< 18', 'b.18 - 24', 'c.25 - 29', 'd.30 - 34', 'e.35 - 39',
      'f.40 - 44', 'g.45 - 49', 'h.50 - 54', 'i.55+']
    ),
    # Create categories for APE bands
    ('BASE_APE', 
     [-float('inf'), 8000, 10000, 12000, 15000, 17000, 20000, 25000, 30000,
      35000, 40000, 50000, 70000, 100000, float('inf')], 
     ['a.< 8m', 'b.8 - 10m', 'c.10 - 12m', 'd.12 - 15m', 'e.15 - 17m', 'f.17 - 20m', 
      'g.20 - 25m', 'h.25 - 30m', 'i.30 - 35m', 'j.35 - 40m', 'k.40 - 50m', 
      'l.50 - 70m', 'm.70 - 100m', 'n.>100m']
     ),
    ('RIDER_APE',
     [-float('inf'), 2000, 3000, 5000, 7000, 10000, 12000, 15000, 17000, 
      20000, 25000, 30000, 35000, 40000, 50000, 70000, 100000, float('inf')],
     ['a.< 2m', 'b.2 - 3m', 'c.3 - 5m', 'd.5 - 7m', 'e.7 - 10m', 'f.10 - 12m',
      'g.12 - 15m', 'h.15 - 17m', 'i.17 - 20m', 'j.20 - 25m', 'k.25 - 30m', 'l.30 - 35m',
      'm.35 - 40m', 'n.40 - 50m', 'o.50 - 70m', 'p.70 - 100m', 'q.>100m']
     ),
    ('TOTAL_CLAIM_AMOUNT',
     [-float('inf'), 1000, 2000, 3000, 5000, 7000, 10000, 12000, 15000, 17000, 
      20000, 25000, 30000, 35000, 40000, 50000, 70000, 100000, float('inf')],
     ['a.< 1m', 'b.1 - 2m', 'c.2 - 3m', 'd.3 - 5m', 'e.5 - 7m', 'f.7 - 10m', 'g.10 - 12m',
      'h.12 - 15m', 'i.15 - 17m', 'j.17 - 20m', 'k.20 - 25m', 'l.25 - 30m', 'l.30 - 35m',
      'm.35 - 40m', 'n.40 - 50m', 'o.50 - 70m', 'p.70 - 100m', 'q.>100m']
     ),
    ('CLAIM_AMOUNT_PER_YEAR',
     [-float('inf'), 1000, 2000, 3000, 5000, 7000, 10000, 12000, 15000, 17000, 
      20000, 25000, 30000, 35000, 40000, 50000, 70000, 100000, float('inf')],
     ['a.< 1m', 'b.1 - 2m', 'c.2 - 3m', 'd.3 - 5m', 'e.5 - 7m', 'f.7 - 10m', 'g.10 - 12m',
      'h.12 - 15m', 'i.15 - 17m', 'j.17 - 20m', 'k.20 - 25m', 'l.25 - 30m', 'l.30 - 35m',
      'm.35 - 40m', 'n.40 - 50m', 'o.50 - 70m', 'p.70 - 100m', 'q.>100m']
     ),
]

for column, bins, labels in bin_labels:
    create_categorical(final, column, bins, labels)

# COMMAND ----------

# DBTITLE 1,Categorize AIS rule names
# Define AIS rule categories
ais_rule_categories = {
    'Medical Checks and Conditions': [
        'Tumor', 'Cancer', 'Chest pain', 'Other Medical conditions', 'Other digestive disorders',
        'Stomach disorder', 'Urinary tract inflammation', 'Cerebral ischemia', 'Gastrointestinal bleeding',
        'Headache', 'Pneumonia', 'Pulmonary tuberculosis', 'Urinary tract inflamation', 'Vestibular disorder',
        'Blood test', 'Burns', 'Constipation', 'Poisoning', 'Tobacco'
    ],
    'Financial and Affordability': [
        'Financial limits', 'Income factor multiple', 'Affordability check', 'Financial requirements',
        'Standard if recovered'
    ],
    'Policy and Administrative Checks': [
        'NB admin check', 'NB underwriting check', 'Policy with other company', 'Substandard history',
        'Concurrent applications', 'Insurable interest', 'Beneficiary', 'Housewife', 'Adverse agent report',
        'Substandard application history disclosure', 'Retiree', 'Non medical limits'
    ],
    'Document and Submission': [
        'Medical document submitted with application', 'Alpha check', 'FATCA'
    ],
    'Random and Standard Checks': [
        'Random check', 'Life Medical Default Rule', 'life Medical Default Rule'
    ],
    'Build and Physical Attributes': [
        'Build - Adult', 'Build - Juvenile'
    ]
}

# Function to categorize rules
def categorize_rules(rules):
    if rules is None:
        return ''  # or return a default value like 'No Category'
    
    categories = set()
    for rule in rules.split('/'):
        for category, keywords in ais_rule_categories.items():
            if rule.strip() in keywords:
                categories.add(category)
    return ', '.join(categories)

# Apply categorization
final['AIS_RULE_CATEGORY'] = final['AIS_RULE_NAMES'].apply(categorize_rules)

# final.head(5)

# COMMAND ----------

# DBTITLE 1,Categorize ADMIN rules
# Function to categorize remarks
def categorize_admin_rule(remark):
    if remark is None or remark == "":
        return ""
    
    remark_lower = remark.lower()
    if any(phrase in remark_lower for phrase in ["hot location", "blacklist"]):
        return "Agent - High risk"
    elif "agent who is client" in remark_lower:
        return "Agent - Who is client"
    elif any(phrase in remark_lower for phrase in ["claim", "rating", "exclusion", "reject", "rescind"]):
        return "Client - Claim/rating/exclusion/rejected/rescind"
    elif "medical" in remark_lower:
        return "Client - Medical check/doc/pending case"
    elif any(phrase in remark_lower for phrase in ["phone", "email", "address", "birth", "lack information"]):
        return "Client - Information update required"
    elif "sign" in remark_lower:
        return "Client - Signature update required"
    elif "approval" in remark_lower:
        return "NB - Exceed approval limit"
    elif any(phrase in remark_lower for phrase in ["<", "<=", ">", ">=", "less than", "more than", "over", "exceed"]):
        return "Policy - Exceed limit"
    elif "restriction code" in remark_lower:
        return "Policy - Has restriction code"
    elif "invalid face" in remark_lower:
        return "Policy - Invalid face amount"
    elif any(phrase in remark_lower for phrase in ["not allowed", "product", "already existed"]):
        return "Policy - Product not allowed"
    elif "redundant premium" in remark_lower:
        return "Policy - Redundant premium"
    elif "underpayment" in remark_lower:
        return "Policy - Underpayment"
    elif "sign" in remark_lower:
        return "Client - Signature required"
    elif any(phrase in remark_lower for phrase in ["fatca", "pep", "risk occupation", "blacklist"]):
        return "Client - High risk"
    elif "check remark" in remark_lower:
        return "Client - Check remark of Digitexx"
    elif "alpha check" in remark_lower:
        return "Client - Alpha check"
    elif "audio" in remark_lower:
        return "Client - Fail QC call"
    elif any(phrase in remark_lower for phrase in ["relative", "insured"]):
        return "Client - Check relationship/non-relation"
    elif "is unsuccess" in remark_lower:
        return "NB - CAS error"
    elif any(phrase in remark_lower for phrase in ["back effective date", "back date", "backdate"]):
        return "Policy - Backdate not allowed"
    elif "random check" in remark_lower:
        return "Policy - Random check"
    elif "online training" in remark_lower:
        return "Agent - Training required"
    elif "commission" in remark_lower:
        return "Agent - Check commission split"
    elif "insurable" in remark_lower:
        return "Policy - No insurable interest"
    else:
        return "Policy" + remark

# Apply the categorization function to the REMARKS column
final['ADMIN_RULE_CATEGORY'] = final['ADMIN_REMARKS'].apply(categorize_admin_rule)

# COMMAND ----------

# DBTITLE 1,Categorize Digitexx remarks
# Function to categorize remarks
def categorize_remark(remark):
    if remark is None or remark == "":
        return ""
    
    remark_lower = remark.lower()
    if any(phrase in remark_lower for phrase in ["khác nhau", "sai", "khác", "nhiều/ít hơn", "nhiều hơn", "ít hơn"]):
        return "Mismatch between form and supporting documents"
    elif any(phrase in remark_lower for phrase in ["mờ", "không rõ", "hình ảnh lạ"]):
        return "Unclear image"
    elif "thiếu dấu vân tay" in remark_lower:
        return "Missing finger-prints"
    elif "thiếu 1 mặt" in remark_lower:
        return "Missing 1 side"
    elif "thiếu chữ ký" in remark_lower:
        return "Lack of signature"
    elif any(phrase in remark_lower for phrase in ["thiếu", "không có", "không cung cấp", "không đạt"]):
        return "Lack of information on supporting document"
    elif "hết hạn" in remark_lower:
        return "Supporting document expired"
    elif "không đánh dấu đồng ý" in remark_lower:
        return "Missing Acknowledgement"
    else:
        return "Other"

# Apply the categorization function to the REMARKS column
final['DIGITEXX_REMARK_CATEGORY'] = final['DIGITEXX_REMARKS'].apply(categorize_remark)

# COMMAND ----------

final = final.rename(columns={"IMAGE_DATE": "image_date"})

#final.to_parquet('/dbfs/mnt/lab/vn/project/VN_NB/NB_ANALYSIS/', partition_cols=['image_date'], engine='pyarrow', index=False)
final.to_parquet('/dbfs/mnt/lab/vn/project/VN_NB/NB_UW/', partition_cols=['image_date'], engine='pyarrow', index=False)

# COMMAND ----------

#final[final["POLICY_STATUS"] == "REJECTED"].head(5)
