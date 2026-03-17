# Databricks notebook source
# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Track the following metrics:
# MAGIC <strong>AWS access<br>
# MAGIC Contact status<br>
# MAGIC Sales (CC, APE, ticket size...)<strong>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load libl, params and paths

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType
from datetime import datetime
from pyspark.sql import Window

# Campaign ID in-scope
target_names = [#'MIL-VTB-20241111','MIL-VTB-20241129','MIL-VTB-20241209',
                'MIL-VTB-20250401']
control_names = [#'CTR-VTB-20241111'
                 ]
exclusion_list = [#'2805421186','2803768643','2805113905','2807306139','2805412105','2805851825','2807275320','2802791758','2807100373','2806219250'
                  ]
cmpgn_list = ','.join(f"'{name}'" for name in target_names)

cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
ado_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_ADOBE_PWS_DB/'
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
aws_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_AWSSTG_DB/'

output_path = '/dbfs/mnt/lab/vn/project/cpm/VTB/Sales_Tracking/'

##### Delta data source
src_path3 = '/mnt/prod/Published/VN/CDC/VN_PUBLISHED_CAS_DB_SYNCSORT/'
src_path4 = '/mnt/prod/Published/VN/CDC/VN_PUBLISHED_AMS_DB_SYNCSORT/'

##### Delta table
delta_1 = 'TPOLICYS/delta'
delta_2 = 'TCLIENT_POLICY_LINKS/delta'
delta_3 = 'TAMS_AGENTS/delta'
delta_4 = 'TCOVERAGES/delta'

file_type2 = 'delta'

paths_2 = [src_path3, src_path4]
files_2 = [delta_1, delta_2, delta_3, delta_4]

df_list = load_parquet_files(paths_2, files_2, file_type2)

targetmdm_df = spark.read.parquet(f'{cpm_path}TARGETM_DM/').filter(F.col('cmpgn_id').isin(target_names+control_names))
trackingm_df = spark.read.parquet(f'{cpm_path}TRACKINGM/').filter(F.col('cmpgn_id').isin(target_names+control_names))

aws_df = spark.read.parquet(f'{ado_path}AWS_CPM_DAILY/').distinct()

# Automatically retrieve campaign start and end dates
cmpgn_st_dt_str = targetmdm_df.select('batch_start_date').distinct().collect()[0][0] # campaign start date
cmpgn_ed_dt_str = targetmdm_df.select('batch_end_date').distinct().collect()[0][0]  # campaign end date

# Convert the string to a date format
cmpgn_st_dt = datetime.strptime(cmpgn_st_dt_str, '%Y-%m-%d').date()
cmpgn_ed_dt = datetime.strptime(cmpgn_ed_dt_str, '%Y-%m-%d').date()

# Convert month_dt to date format and extract year and month
aws_df = aws_df.withColumn("image_date", 
                           F.last_day(F.to_date(
                               F.concat_ws("-", 
                                           F.substring(F.col("month_dt").cast("string"), 1, 4), 
                                           F.substring(F.col("month_dt").cast("string"), 5, 2)), 
                               "yyyy-MM")))
aws_df = aws_df.filter((F.col("image_date") >= cmpgn_st_dt) &
                       (F.col("image_date") <= cmpgn_ed_dt))

aws_df = aws_df.filter(F.col("tgt_id").isNotNull()) \
                .groupBy(["cust_id","agent_id"]) \
                .agg(F.count("cust_id").alias("no_cus")) \
                .dropDuplicates()

ctct_df = spark.read.parquet(f'{cas_path}TCONTACT_RESULTS/').filter((F.col('trxn_dt')>=cmpgn_st_dt) &
                                                                    (F.col('trxn_dt')<=cmpgn_ed_dt))
                                      
tpolicys_df = df_list['TPOLICYSdelta'].filter((F.col('sbmt_dt')>=cmpgn_st_dt) &
                                              (F.col('sbmt_dt')<=cmpgn_ed_dt))

tcoverages_df = df_list['TCOVERAGESdelta'].filter((F.col('cvg_eff_dt')>=cmpgn_st_dt) &
                                                  (F.col('cvg_eff_dt')<=cmpgn_ed_dt))

tclient_pol_df = df_list['TCLIENT_POLICY_LINKSdelta']

tagents_df = df_list['TAMS_AGENTSdelta']

target_df = targetmdm_df.filter((F.col('cmpgn_id').isin(target_names)) &
                                ~(F.col('tgt_cust_id').isin(exclusion_list)))

control_df = targetmdm_df.filter(F.col('cmpgn_id').isin(control_names))

# Retrieve PD Hierarchy
tams_pd_hierarchy_tmp = spark.read.parquet(f'{ams_path}/TAMS_PD_HIERARCHY/')

# Define the Window specification
window_spec = Window.partitionBy("AGT_CD").orderBy(F.desc("EFFECTIVE_DATE"))

# Add a row number to each record within the partition
tams_pd_hierarchy_df_with_row_num = tams_pd_hierarchy_tmp.withColumn("row_num", F.row_number().over(window_spec))

# Filter to keep only the first record for each "AGT_CD"
tams_pd_hierarchy_df = tams_pd_hierarchy_df_with_row_num.filter(F.col("row_num") == 1).drop("row_num")
del tams_pd_hierarchy_df_with_row_num
tams_pd_hierarchy_df.createOrReplaceTempView('tams_pd_hierarchy')

print("No. records of TARGET:", target_df.count(), ", CONTROL:", control_df.count())
#print("No. records of AWS_CPM_DAILY:", aws_df.count())
#print("No. records of TCONTACT_RESULTS:", ctct_df.count())
#print("No. records of TPOLICYS:", tpolicys_df.count(), ", TCOVERAGES:", tcoverages_df.count())

# COMMAND ----------

target_df.createOrReplaceTempView("target_dm")
control_df.createOrReplaceTempView("control_dm")
trackingm_df.createOrReplaceTempView("trackingm")
aws_df.createOrReplaceTempView("aws_cpm_daily")
ctct_df.createOrReplaceTempView("tcontact_results")
tpolicys_df.createOrReplaceTempView("tpolicys")
tclient_pol_df.createOrReplaceTempView("tclient_policy_links")
tagents_df.createOrReplaceTempView("tams_agents")
tcoverages_df.createOrReplaceTempView("tcoverages")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Generate tracking details with the following information:
# MAGIC <strong>rh_name<br>
# MAGIC sm_name/sm_code<br>
# MAGIC loc_cd<br>
# MAGIC dmgr_name/mgr_code<br>
# MAGIC agt_nm<br>
# MAGIC agt_code<br>
# MAGIC masked_cli_nm<br>
# MAGIC masked_pol_num<br>
# MAGIC aws_ind<br>
# MAGIC ctct_ind<br>
# MAGIC ctct_status<br>
# MAGIC no_sbmt_pols<br>
# MAGIC no_iss_pols<br>
# MAGIC total_ape<br>
# MAGIC avg_case_size<br></strong>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### DERIVE ALL POSSIBLE POs FROM PREV_POL_NUM ('O','I' and 'T')

# COMMAND ----------

all_sales = spark.sql(f"""
select  tgt_cust_id, prev_pol_num, cli_num as new_po_num, link_typ
from    target_dm tgt inner join
		tclient_policy_links tpl on tgt.prev_pol_num=tpl.pol_num and tpl.link_typ in ('O','I','T') and tpl.rec_status='A' 
""")
all_sales.createOrReplaceTempView("all_sales")
#all_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GET ALL CUSTOMERS CONTACT DAILY

# COMMAND ----------

cli_num_path = "/mnt/lab/vn/project/cpm/LEGO/Pre_approved_2/VTB/cli_num_list_pre_batch1.csv"
cli_num_df = spark.read.format("csv").load(cli_num_path, inferSchema=True, header=True)

cli_num_df.createOrReplaceTempView("cli_num_list")

# COMMAND ----------

@F.pandas_udf(StringType()) 
def fix_tone_mark_pandas(s): 
    return s.apply(fix_tone_mark)

cli_conso_df = spark.sql("""
WITH conso_cli AS (
SELECT  cli.segment, cli.PO_NUM, NVL(con.CLI_NUM, cli.CLI_NUM) AS NEW_CLI_NUM
FROM    cli_num_list cli
    LEFT JOIN
        vn_published_cas_db.tclient_consolidations con ON cli.cli_num = con.CLI_CNSLDT_NUM
)
SELECT  con.*, LOWER(dtl.CLI_NM) AS FULLNAME, TO_DATE(dtl.BIRTH_DT) AS DOB, dtl.SEX_CODE AS GENDER
FROM    conso_cli con
    INNER JOIN
        vn_published_cas_db.tclient_details dtl ON con.NEW_CLI_NUM = dtl.CLI_NUM
""")

cli_conso_df = cli_conso_df.withColumn(
    "cleaned_FULLNAME",
    fix_tone_mark_pandas(cli_conso_df["FULLNAME"])
).drop("FULLNAME")

cli_conso_df.createOrReplaceTempView("cli_conso")

# COMMAND ----------

vtb_contact = spark.sql(f"""
/*WITH eligible_agt AS (
  SELECT  DISTINCT agt_code
  FROM    target_dm tgt INNER JOIN
          tpolicys pol ON tgt.prev_pol_num=pol.pol_num
  WHERE   1=1
    AND   cmpgn_id IN ({cmpgn_list})
    AND   btch_id not like '9%'
)*/
SELECT lead.lead_num,
       lead.agt_code,
       --agt.agt_code ori_agt_code,
       --CASE WHEN agt.agt_code IS NOT NULL THEN 'Y' ELSE 'N' END found_ind,
       TO_DAte(lead.create_dt) load_dt,
       TO_DATE(lead.referral_dt) trxn_dt,
       CAST(lead.status_meet_cus AS INT) contact_rslt,
       link.pol_num,
       pol.pol_stat_cd,
       TO_DATe(pol.sbmt_dt) sbmt_dt,
       TO_DATE(pol.pol_iss_dt) pol_iss_dt,
       pol.mode_prem*12/pol.pmt_mode ape
FROM    vn_published_awsstg_db.tawsstg_lead lead
  LEFT JOIN vn_published_awsstg_db.tawsstg_lead_policy link ON lead.lead_num = link.lead_num
  LEFT JOIN tpolicys pol ON link.pol_num=pol.pol_num  
  LEFT JOIN tclient_policy_links cpl ON pol.pol_num=cpl.pol_num AND cpl.link_typ='O' AND cpl.rec_status='A'
  --LEFT JOIN eligible_agt agt ON lead.agt_code = agt.agt_code
WHERE lead.lead_num LIKE 'VT%'
  AND TO_DATE(lead.referral_dt) >= '{cmpgn_st_dt_str}'
  AND (lead.NOTE LIKE '%Pre%' OR lead.NOTE LIKE '%PRE%' OR lead.NOTE LIKE '%pre%')
  --AND   agt.agt_code IS NOT NULL
""")

#vtb_contact.groupBy("load_dt").agg(F.count("lead_num").alias("lead_count")).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### GET ALL CUSTOMERS W/ EPOS SI (final/converted)

# COMMAND ----------

df = spark.sql(f"""
SELECT DISTINCT
        pos.PROPOSAL_ID,
        hdr.STATUS,
        TO_DATE(CREATED_DATE) AS CREATED_DATE,
        EPOS_DETAILS
FROM    vn_published_epos_db.pos_proposal_header hdr
  INNER JOIN
        vn_published_epos_db.pos_proposal pos ON hdr.ID = pos.ID
WHERE   TO_DATE(CREATED_DATE) BETWEEN '{cmpgn_st_dt}' AND '{cmpgn_ed_dt}'
    AND hdr.STATUS in ('final','converted')
""")

# Define schema for the JSON data
json_schema = StructType([
    StructField("proposalId", StringType(), True),
    StructField("applicationId", StringType(), True),
    StructField("fileGroup", StringType(), True),
    StructField("isDeleted", BooleanType(), True),
    StructField("isSyncToAWS", BooleanType(), True),
    StructField("proposalData", StructType([
        StructField("_guid", StringType(), True),
        StructField("proposalName", StringType(), True),
        StructField("proposalId", StringType(), True),
        StructField("isDefaultRidersAdded", BooleanType(), True),
        StructField("proposalNumber", StringType(), True),
        StructField("status", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("basePlanSchemaId", StringType(), True),
        StructField("product", StructType([
            StructField("documentId", StringType(), True),
            StructField("expiryDate", StringType(), True),
            StructField("effectiveDate", StringType(), True),
            StructField("contentGroup", StringType(), True),
            StructField("allowSell", BooleanType(), True),
            StructField("allowSellCorporate", BooleanType(), True),
            StructField("category", ArrayType(StringType()), True),
            StructField("contentType", StringType(), True),
            StructField("source", StringType(), True),
            StructField("goals", ArrayType(StringType()), True),
            StructField("resources", StructType([
                StructField("vi", StructType([
                    StructField("title", StringType(), True),
                    StructField("productName", StringType(), True),
                    StructField("marketingName", StringType(), True),
                    StructField("emailSubject", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("localUri", StringType(), True),
                    StructField("localUrl", StringType(), True),
                    StructField("thumbnail", StringType(), True),
                    StructField("size", IntegerType(), True),
                    StructField("features", ArrayType(StringType()), True),
                    StructField("version", StringType(), True),
                    StructField("active", BooleanType(), True)
                ])),
                StructField("en", StructType([
                    StructField("title", StringType(), True),
                    StructField("productName", StringType(), True),
                    StructField("marketingName", StringType(), True),
                    StructField("emailSubject", StringType(), True),
                    StructField("description", StringType(), True),
                    StructField("localUri", StringType(), True),
                    StructField("localUrl", StringType(), True),
                    StructField("thumbnail", StringType(), True),
                    StructField("size", IntegerType(), True),
                    StructField("features", ArrayType(StringType()), True),
                    StructField("version", StringType(), True),
                    StructField("active", BooleanType(), True)
                ]))
            ])),
            StructField("isAllowSellProduct", BooleanType(), True),
            StructField("visibility", StringType(), True)
        ])),
        StructField("effectiveDate", StringType(), True),
        StructField("reference", StringType(), True),
        StructField("policyExcludeSOS", StringType(), True),
        StructField("createdDate", StringType(), True),
        StructField("version", StringType(), True),
        StructField("isBackDated", BooleanType(), True),
        StructField("basePlan", StructType([
            StructField("baseProtection", IntegerType(), True),
            StructField("basePremium", IntegerType(), True),
            StructField("deathBenefit", StringType(), True),
            StructField("totalPayment", IntegerType(), True),
            StructField("paymentFrequently", StringType(), True),
            StructField("termProtection", IntegerType(), True),
            StructField("termPremium", IntegerType(), True),
            StructField("shortfall", IntegerType(), True),
            StructField("premiumDuration", IntegerType(), True),
            StructField("emLoading", IntegerType(), True),
            StructField("perMille", IntegerType(), True),
            StructField("perMilleDuration", IntegerType(), True),
            StructField("termEMLoading", IntegerType(), True),
            StructField("termPerMille", IntegerType(), True),
            StructField("termPerMilleDuration", IntegerType(), True),
            StructField("topUpPremium", IntegerType(), True),
            StructField("minFaMultiplier", IntegerType(), True),
            StructField("maxFaMultiplier", IntegerType(), True),
            StructField("fundActivities", ArrayType(StructType([
                StructField("plannedPremium", IntegerType(), True),
                StructField("attainAge", IntegerType(), True)
            ]))),
            StructField("annualAdditionalPayment", IntegerType(), True),
            StructField("chartAnnualAdditionalPayment", IntegerType(), True)
        ])),
        StructField("customer", StructType([
            StructField("_guid", StringType(), True),
            StructField("insured", StructType([
                StructField("lastName", StringType(), True),
                StructField("middleName", StringType(), True),
                StructField("firstName", StringType(), True),
                StructField("fullName", StringType(), True),
                StructField("dob", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("age", IntegerType(), True)
            ])),
            StructField("owner", StructType([
                StructField("lastName", StringType(), True),
                StructField("middleName", StringType(), True),
                StructField("firstName", StringType(), True),
                StructField("fullName", StringType(), True),
                StructField("dob", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("age", IntegerType(), True)
            ])),
            StructField("customerId", StringType(), True),
            StructField("financials", StructType([
                StructField("totalIncome", IntegerType(), True)
            ]))
        ])),
        StructField("validStatus", BooleanType(), True),
        StructField("agentId", StringType(), True),
        StructField("peSystem", StringType(), True),
        StructField("proposalPremiums", StructType([
            StructField("basePlan", StructType([
                StructField("premiums", ArrayType(StructType([
                    StructField("paymentMode", StringType(), True),
                    StructField("premium", IntegerType(), True)
                ]))),
                StructField("extraPremiums", ArrayType(StructType([
                    StructField("paymentMode", StringType(), True),
                    StructField("extraPremium", IntegerType(), True)
                ]))),
                StructField("totalPremiums", ArrayType(StructType([
                    StructField("paymentMode", StringType(), True),
                    StructField("totalPremium", IntegerType(), True)
                ])))
            ])),
            StructField("basePlanCode", StringType(), True)
        ])),
        StructField("lastModifiedDate", StringType(), True),
        StructField("expiryDate", StringType(), True)
    ])),
    StructField("isDraft", BooleanType(), True),
    StructField("lastModifiedDate", StringType(), True),
    StructField("report-meta", StructType([
        StructField("id", StringType(), True),
        StructField("adapterId", StringType(), True),
        StructField("system", StringType(), True),
        StructField("format", StringType(), True),
        StructField("landscape", BooleanType(), True),
        StructField("locale", StringType(), True),
        StructField("hasFna", BooleanType(), True)
    ])),
    StructField("report-data", StringType(), True)
])

# Define schema for owner and insured
owner_schema = StructType([
    StructField("lastName", StringType(), True),
    StructField("middleName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("fullName", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True)
])

insured_schema = StructType([
    StructField("lastName", StringType(), True),
    StructField("middleName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("fullName", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Convert the JSON string to JSON format
df_with_json = df.withColumn(
    "EPOS_DETAILS_JSON", 
    F.from_json(F.col("EPOS_DETAILS"), json_schema)
)

# Select and rename columns, including CREATED_DATE
epos_proposals_df = df_with_json.select(
    F.col("PROPOSAL_ID"),
    F.col("STATUS"),
    F.col("CREATED_DATE"),
    F.lower(F.col("EPOS_DETAILS_JSON.proposalData.customer.owner.fullName")).alias("owner_fullName"),
    F.col("EPOS_DETAILS_JSON.proposalData.customer.owner.dob").alias("owner_dob"),
    F.substring(F.col("EPOS_DETAILS_JSON.proposalData.customer.owner.gender"),1,1).alias("owner_gender"),
    F.lower(F.col("EPOS_DETAILS_JSON.proposalData.customer.insured.fullName")).alias("insured_fullName"),
    F.col("EPOS_DETAILS_JSON.proposalData.customer.insured.dob").alias("insured_dob"),
    F.substring(F.col("EPOS_DETAILS_JSON.proposalData.customer.insured.gender"),1,1).alias("insured_gender")
).dropDuplicates()

epos_proposals_df = epos_proposals_df.filter(
    F.col("owner_fullName").isNotNull() |
    F.col("insured_fullName").isNotNull()
)

epos_proposals_df = epos_proposals_df.withColumn(
    "cleaned_owner_fullName",
    fix_tone_mark_pandas(epos_proposals_df["owner_fullName"])
).withColumn(
    "cleaned_insured_fullName",
    fix_tone_mark_pandas(epos_proposals_df["insured_fullName"])
).drop("owner_fullName", "insured_fullName")

# COMMAND ----------

epos_cli_df = epos_proposals_df.alias("a").join(
    cli_conso_df.alias("b"),
    (F.col("a.cleaned_owner_fullName") == F.col("b.cleaned_FULLNAME")) &
    (F.col("a.owner_dob") == F.col("b.DOB")) &
    (F.col("a.owner_gender") == F.col("b.GENDER")),
    "left" 
).join(
    cli_conso_df.alias("c"),
    (F.col("a.cleaned_insured_fullName") == F.col("c.cleaned_FULLNAME")) &
    (F.col("a.insured_dob") == F.col("c.DOB")) &
    (F.col("a.insured_gender") == F.col("c.GENDER")),
    "left"
).select(
    F.coalesce(F.col("b.NEW_CLI_NUM"), F.col("c.NEW_CLI_NUM")).alias("CLI_NUM"),
    "a.PROPOSAL_ID",
    "a.STATUS",
    F.when(F.col("b.NEW_CLI_NUM").isNotNull(), 1).otherwise(0).alias("NEW_PO_HIT"),
    F.when(F.col("c.NEW_CLI_NUM").isNotNull(), 1).otherwise(0).alias("NEW_INSRD_HIT")
).groupBy(
    "CLI_NUM"
).agg(
    F.sum(F.when(F.col("STATUS") == F.lit("final"), 1).otherwise(0)).alias("SI_FINAL"),
    F.sum(F.when(F.col("STATUS") == F.lit("converted"), 1).otherwise(0)).alias("SI_CONVERTED"),
    F.max(F.col("NEW_PO_HIT")).alias("NEW_PO_HIT"),
    F.max(F.col("NEW_INSRD_HIT")).alias("NEW_INSRD_HIT")
)

epos_cli_df.createOrReplaceTempView("epos_cli")

# COMMAND ----------

contact_results = spark.sql(f"""
with ctct as (
select cli_num, to_date(trxn_dt) trxn_dt, contact_rslt, 
        row_number() over (partition by cli_num order by TRXN_DT desc) rank 
from    tcontact_results 
where    to_date(trxn_dt)>='{cmpgn_st_dt_str}')
select  cli_num, trxn_dt, contact_rslt
from    ctct
where   rank=1
""")

contact_results.createOrReplaceTempView("contact_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ### GET AGENTS TRAINED

# COMMAND ----------

# MAGIC %md
# MAGIC ### TARGET GROUP

# COMMAND ----------

lead_details = spark.sql(f"""
select	con.new_cli_num as tgt_role_id
		, tgt.*
		, case when call.cli_num is not null then 1 else 0 end ctct_hit
  		, call.contact_rslt
    	, call.trxn_dt ctct_dt
from	target_dm tgt 
    inner join
		cli_conso con on tgt.tgt_cust_id=con.po_num
    left join
		contact_results call on tgt.tgt_cust_id=call.cli_num
where	tgt.btch_id not like '9%'	-- Exclude UCM batches
""")
lead_details.createOrReplaceTempView("lead_details")
#print("Leads count:", lead_details.count())

cpm_sales = spark.sql(f"""
select	fld.FLD_VALU_DESC product_type 
		, concat_ws('-',cvg.plan_code,pln.plan_shrt_desc) product_name
		, tpl.cli_num new_po_num
		, pol.pol_num new_pol_num
		, cvg.cvg_typ
		, cast(cvg.cvg_prem*1000*cast(pol.pmt_mode as int)/12 as float) anp
		, to_date(pol.sbmt_dt) smbt_dt
		, to_date(pol.pol_iss_dt) pol_iss_dt
		, pol.pol_stat_cd
		--, pol.wa_cd_1 wa_cd_1
		, nagt.agt_code new_agt_code
		, nagt.agt_nm new_agt_nm
		, nagt.rank_cd
from	all_sales all inner join
		tclient_policy_links tpl on all.new_po_num=tpl.cli_num inner join
		tpolicys pol on tpl.pol_num=pol.pol_num and tpl.link_typ in ('O') and tpl.rec_status='A' inner join
		tcoverages cvg on pol.pol_num=cvg.pol_num inner join            
		hive_metastore.vn_published_ams_db.tams_agents nagt on pol.agt_code=nagt.agt_code inner join
		vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code left join 
		vn_published_cas_db.tfield_values fld on pln.INS_TYP=fld.fld_valu and fld.fld_nm like '%INS_TYP%'
where   1=1
	and	pol_stat_cd in ('8','1','3')
	and	((cvg.cvg_typ='B' AND to_date(sbmt_dt) BETWEEN '{cmpgn_st_dt_str}' AND '{cmpgn_ed_dt_str}') OR
		 (cvg.cvg_typ='R' AND to_date(cvg_eff_dt) BETWEEN '{cmpgn_st_dt_str}' AND '{cmpgn_ed_dt_str}')) 	     
""")
cpm_sales.createOrReplaceTempView("cpm_sales")

sales_details = spark.sql(f"""
select distinct
		tgt.cmpgn_id
		, tgt.tgt_agt_regional_head
		, tgt.tgt_agt_loc_sm_name
		, tgt.tgt_agt_loc_code
		, tgt.tgt_agt_code
		--, agt.agt_nm tgt_agt_nm
		, tgt.tgt_role_id
		, tgt.tgt_cust_id
		, tgt.tgt_cli_nm
		, tgt.prev_pol_num
		, tgt.ctct_hit
		, cpm.product_type 
		, cpm.product_name
		, cpm.new_po_num
		, cpm.new_pol_num
		, cpm.cvg_typ
		, cpm.anp
		, cpm.smbt_dt
		, cpm.pol_iss_dt
		, cpm.pol_stat_cd
		--, pol.wa_cd_1 wa_cd_1
		, cpm.new_agt_code
		, cpm.new_agt_nm
		, cpm.rank_cd
		--, cpm.mpro_title
		, case when epos.cli_num is not null and epos.si_final>0 then 1 else 0 end si_final
		, case when epos.cli_num is not null and epos.si_converted>0 then 1 else 0 end si_converted
		, tgt.ctct_dt
		, tgt.contact_rslt
		--, case when trn.agt_code is not null then 1 else 0 end trained_ind
		, case when tgt.tgt_agt_code=cpm.new_agt_code then 1 else 0 end same_agt_ind
from    lead_details tgt left join
		epos_cli epos on tgt.tgt_role_id=epos.cli_num left join
		cpm_sales cpm on tgt.tgt_cust_id=cpm.new_po_num and cpm.smbt_dt>=tgt.batch_start_date
""").dropDuplicates()
#print("Sales details has:", sales_details.filter(F.col("new_pol_num").isNotNull()).count(), " records.")
sales_details.createOrReplaceTempView("sales_details")

sales_result = spark.sql(f"""
WITH dmgr AS (
    SELECT	DISTINCT
				agt.agt_code agt_code,
				agt.agt_nm agt_nm,
				mgr.agt_nm tgt_agt_loc_sm_name,
				drt.agt_cd tgt_agt_loc_sm_code,
				mgr.rank_cd sm_rank,
				rsm.mgr_code tgt_agt_regional_head_code,
				rh.agt_nm tgt_agt_regional_head,
				case when rsm.RANK_CD='Branch' then 'SM'
    				 when rsm.RANK_CD='SM' then 'RSM'
					 when rsm.RANK_CD='RSM' then 'PH'
					 else 'Unknown'
				end as rsm_rank
		FROM 	vn_published_ams_db.TAMS_AGENTS agt
			INNER JOIN
				vn_published_ams_db.TAMS_AGT_RPT_RELS drt
     		 ON	agt.agt_code=drt.sub_agt_cd AND drt.rpt_level = 1
			INNER JOIN
				vn_published_ams_db.TAMS_AGENTS mgr
			 ON	drt.AGT_CD=mgr.AGT_CODE
			LEFT JOIN
				tams_pd_hierarchy rsm
			 ON drt.AGT_CD=rsm.AGT_CD
			LEFT JOIN
				vn_published_ams_db.TAMS_AGENTS rh
			 ON rsm.MGR_CODE=rh.AGT_CODE
WHERE	agt.COMP_PRVD_NUM IN ('52','53')
	--AND	mgr.RANK_CD IS NOT NULL
),
agt as (
select	distinct agt_code, agt_nm
		--, coalesce(mdrt_desc, mpro_title, 'Normal') mpro_title
from 	hive_metastore.vn_published_ams_db.tams_agents
)
SELECT  tgt.cmpgn_id,
        dmgr.tgt_agt_regional_head,
        dmgr.tgt_agt_loc_sm_name,
        tgt.tgt_agt_loc_code,
        --dmgr.mgr_nm dmgr_nm,
        agt.agt_nm tgt_agt_nm,
        tgt.tgt_agt_code,
		--agt.mpro_title,        
		tgt.tgt_cust_id,
        tgt.tgt_cli_nm,
        tgt.prev_pol_num,
        --MAX(NVL(tgt.trained_ind,0)) TRAINED_IND,
        MAX(CASE WHEN aws.cust_id IS NOT NULL THEN 1 ELSE 0 END) AWS_HIT,
        SUM(tgt.ctct_hit) CTCT_HIT,
        MIN(tgt.contact_rslt) CTCT_STATUS,
        MIN(CASE WHEN tgt.contact_rslt='01' THEN tgt.ctct_dt END) CTCT_DT,
        COUNT(DISTINCT tgt.new_po_num) NO_RESPONSES,
		COUNT(DISTINCT CASE WHEN tgt.CVG_TYP='B' THEN tgt.new_pol_num END) NO_SBMT_POLS,
        COUNT(DISTINCT CASE WHEN tgt.CVG_TYP='B' AND tgt.pol_stat_cd IN ('1','3') THEN tgt.new_pol_num END) NO_ISSUE_POLS,
        CAST(SUM(NVL(tgt.anp,0)) AS float) TOTAL_SBMT_APE,
        CAST(SUM(CASE WHEN tgt.pol_stat_cd IN ('1','3') THEN tgt.anp ELSE 0 END) AS float) TOTAL_ISSUE_APE,
        SUM(NVL(tgt.anp,0)) / 
        COUNT(DISTINCT CASE WHEN tgt.cvg_typ='B' THEN tgt.new_pol_num END) AVG_CASE_SIZE_SBMT,
        SUM(CASE WHEN tgt.pol_stat_cd IN ('1','3') THEN tgt.anp ELSE 0 END) /
        COUNT(DISTINCT CASE WHEN tgt.cvg_typ='B' THEN tgt.new_pol_num END) AVG_CASE_SIZE_ISSUE
FROM	sales_details tgt inner join
  		agt on tgt.tgt_agt_code=agt.agt_code left join
		aws_cpm_daily aws on tgt.tgt_cust_id=aws.cust_id left join
		dmgr on tgt.tgt_agt_code=dmgr.agt_code
GROUP BY
    tgt.cmpgn_id,
    dmgr.tgt_agt_regional_head,
	dmgr.tgt_agt_loc_sm_name,
    tgt.tgt_agt_loc_code,
    --dmgr.mgr_nm,
    agt.agt_nm,
	tgt.tgt_agt_code,
 	--agt.mpro_title,
	tgt.tgt_cust_id,
    tgt.tgt_cli_nm,
    tgt.prev_pol_num
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CONTROL GROUP

# COMMAND ----------

ctr_details = spark.sql(f"""
select distinct
		tgt.cmpgn_id
		, tgt.tgt_agt_regional_head
		, tgt.tgt_agt_loc_sm_name
		, tgt.tgt_agt_loc_code
		, tgt.tgt_agt_code
		--, agt.agt_nm tgt_agt_nm
		, tgt.tgt_cust_id
		, tgt.tgt_cli_nm
		, tgt.prev_pol_num
		, fld.FLD_VALU_DESC product_type 
		, concat_ws('-',cvg.plan_code,pln.plan_shrt_desc) product_name
		, tpl.cli_num new_po_num
		, pol.pol_num new_pol_num
		, cvg.cvg_typ
		, cast(cvg.cvg_prem*1000*cast(pmt_mode as int)/12 as float) anp
		, to_date(sbmt_dt) smbt_dt
		, to_date(pol_iss_dt) pol_iss_dt
		, pol_stat_cd
		--, pol.wa_cd_1 wa_cd_1
		, nagt.agt_code new_agt_code
		, nagt.agt_nm new_agt_nm
		, nagt.rank_cd 
from    control_dm tgt inner join
		tclient_policy_links tpl on tgt.tgt_cust_id=tpl.cli_num and tpl.link_typ in ('O') and tpl.rec_status='A' inner join
		tpolicys pol on tpl.pol_num=pol.pol_num inner join
		tcoverages cvg on pol.pol_num=cvg.pol_num inner join            
		--tams_agents agt on tgt.tgt_agt_code=agt.agt_code left join
		tams_agents nagt on pol.agt_code=nagt.agt_code inner join
		vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code left join 
		vn_published_cas_db.tfield_values fld on pln.INS_TYP=fld.fld_valu and fld.fld_nm like '%INS_TYP%'
where   1=1
	and	pol_stat_cd in ('8','1','3')
	and	((cvg.cvg_typ='B' AND sbmt_dt BETWEEN '{cmpgn_st_dt_str}' AND '{cmpgn_ed_dt_str}') OR
		 (cvg.cvg_typ='R' AND cvg_eff_dt BETWEEN '{cmpgn_st_dt_str}' AND '{cmpgn_ed_dt_str}'))		               
""")
#print("Sales details has:", sales_details.count(), " records.")
ctr_details.createOrReplaceTempView("sales_details")

ctr_result = spark.sql(f"""
WITH dmgr AS (
    SELECT	DISTINCT
				agt.agt_code agt_code,
				agt.agt_nm agt_nm,
				mgr.agt_nm tgt_agt_loc_sm_name,
				drt.agt_cd tgt_agt_loc_sm_code,
				mgr.rank_cd sm_rank,
				rsm.mgr_code tgt_agt_regional_head_code,
				rh.agt_nm tgt_agt_regional_head,
				case when rsm.RANK_CD='Branch' then 'SM'
    				 when rsm.RANK_CD='SM' then 'RSM'
					 when rsm.RANK_CD='RSM' then 'PH'
					 else 'Unknown'
				end as rsm_rank
		FROM 	vn_published_ams_db.TAMS_AGENTS agt
			INNER JOIN
				vn_published_ams_db.TAMS_AGT_RPT_RELS drt
     		 ON	agt.agt_code=drt.sub_agt_cd AND drt.rpt_level = 1
			INNER JOIN
				vn_published_ams_db.TAMS_AGENTS mgr
			 ON	drt.AGT_CD=mgr.AGT_CODE
			LEFT JOIN
				tams_pd_hierarchy rsm
			 ON drt.AGT_CD=rsm.AGT_CD
			LEFT JOIN
				vn_published_ams_db.TAMS_AGENTS rh
			 ON rsm.MGR_CODE=rh.AGT_CODE
WHERE	agt.COMP_PRVD_NUM IN ('52','53')
	--AND	mgr.RANK_CD IS NOT NULL
)
SELECT  tgt.cmpgn_id,
        dmgr.tgt_agt_regional_head,
        dmgr.tgt_agt_loc_sm_name,
        tgt.tgt_agt_loc_code,
        --dmgr.mgr_nm dmgr_nm,
        agt.agt_nm tgt_agt_nm,
        tgt.tgt_agt_code,
        tgt.tgt_cli_nm,
        tgt.prev_pol_num,
        MAX(CASE WHEN aws.cust_id IS NOT NULL THEN 1 ELSE 0 END) AWS_HIT,
        COUNT(DISTINCT CASE WHEN call.cli_num IS NOT NULL THEN call.cli_num END) CTCT_HIT,
        MIN(call.contact_rslt) CTCT_STATUS,
        COUNT(DISTINCT trk.new_po_num) NO_RESPONSES,
		COUNT(DISTINCT CASE WHEN trk.CVG_TYP='B' THEN trk.new_pol_num END) NO_SBMT_POLS,
        COUNT(DISTINCT CASE WHEN trk.CVG_TYP='B' AND trk.pol_stat_cd IN ('1','3') THEN trk.new_pol_num END) NO_ISSUE_POLS,
        CAST(SUM(NVL(trk.anp,0)) AS float) TOTAL_SBMT_APE,
        CAST(SUM(CASE WHEN trk.pol_stat_cd IN ('1','3') THEN trk.anp ELSE 0 END) AS float) TOTAL_ISSUE_APE,
        SUM(NVL(trk.anp,0)) / 
        COUNT(DISTINCT CASE WHEN trk.cvg_typ='B' THEN trk.new_pol_num END) AVG_CASE_SIZE_SBMT,
        SUM(CASE WHEN trk.pol_stat_cd IN ('1','3') THEN trk.anp ELSE 0 END) /
        COUNT(DISTINCT CASE WHEN trk.cvg_typ='B' THEN trk.new_pol_num END) AVG_CASE_SIZE_ISSUE
FROM	control_dm tgt left join
		sales_details trk on tgt.tgt_cust_id=trk.tgt_cust_id inner join
  		tams_agents agt on tgt.tgt_agt_code=agt.agt_code left join
		aws_cpm_daily aws on tgt.tgt_agt_code = aws.agent_id left join
		dmgr on tgt.tgt_agt_code=dmgr.agt_code left join 
		(select cli_num, to_date(trxn_dt) trxn_dt, contact_rslt, row_number() over (partition by cli_num order by TRXN_DT desc) rank 
		 from tcontact_results) call
		on tgt.tgt_cust_id = call.cli_num and call.rank = 1 and to_date(trxn_dt)>='{cmpgn_st_dt_str}'
GROUP BY
    tgt.cmpgn_id,
	dmgr.tgt_agt_regional_head,
	dmgr.tgt_agt_loc_sm_name,
    tgt.tgt_agt_loc_code,
    --dmgr.mgr_nm,
    agt.agt_nm,
	tgt.tgt_agt_code,
    tgt.tgt_cli_nm,
    tgt.prev_pol_num
""")

# COMMAND ----------

sales_pd = sales_result.toPandas()
sales_dtl_pd = sales_details.toPandas()
ctr_pd = ctr_result.toPandas()
vtb_contact = vtb_contact.repartition(1)
contact_pd = vtb_contact.toPandas()
#print(contact_pd.shape)

# Lower case all column headers
sales_pd.rename(columns=lambda x: x.lower(), inplace=True)
ctr_pd.rename(columns=lambda x: x.lower(), inplace=True)

# COMMAND ----------

print("No rows in Target:", sales_pd.shape[0])
#sales_pd.to_parquet(f'{output_path}vtb_pre_approved_sales_tracking_details.parquet', index=False)
print("No rows in Control:", ctr_pd.shape[0])
#ctr_pd.to_parquet(f'{output_path}ctr_lego_sales_tracking_details.parquet', index=False)
sales_dtl_pd.to_parquet(f'{output_path}vtb_pre_approved_full_sales_tracking.parquet', index=False)
contact_pd.to_parquet(f'{output_path}vtb_pre_approved_contact_details.parquet', index=False)
#sales_pd.to_csv(f'{output_path}lego_sales_all_leads_details.csv', index=False, header=True, encoding='utf-8-sig')

print("No rows in sales_result:", sales_pd[sales_pd['ctct_status']=='01'].shape[0])
#sales_pd[sales_pd['ctct_status']=='01'].to_csv(f'{output_path}lego_psm_sales_tracking_details.csv', index=False, header=True, encoding='utf-8-sig')
#sales_pd[sales_pd['ctct_hit']>0].head(5)
# List of columns to sum
columns_to_sum = ['no_responses', 'no_sbmt_pols', 'no_issue_pols', 'total_sbmt_ape', 'total_issue_ape']

# Calculate the sum of the specified columns
summarized_tgt_result = sales_pd[columns_to_sum].sum()
summarized_ctr_result = ctr_pd[columns_to_sum].sum()

# Display the summarized result
print("Result of Target:")
print(summarized_tgt_result)

# Display the summarized result
print("Result of Control:")
print(summarized_ctr_result)
