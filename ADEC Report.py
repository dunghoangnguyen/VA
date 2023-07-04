# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType
import pandas as pd
from datetime import datetime, timedelta
import calendar

# Declare all variables here and make changes where applicable
#rpt_yr = 2023
#lmth = -1
#llmth = -2
#rpt_mth = 2305
#rpt_prv_mth = 2304
exclusion_list_sub = ['MI007', 'PA007', 'PA008']
active_sts = ['1', '2', '3', '5']
exclusion_list_full = ['MI007', 'PA007', 'PA008', 'EV001', 'EV002', 'EV003', 'EV004', 'EV005', 'EV006', 'EV007', 'EV008', 'EV009', 'EV010', 'EV011', 'EV012', 'EV013', 'EV014', 'EV015', 'EV016', 'EV017', 'EV018', 'EV019', 'EV101', 'EV102', 'EV103', 'EV104', 'EV105', 'EV201', 'EV202', 'EV203', 'EVS01', 'EVS02', 'EVS03', 'EVS04', 'EVS05','FC103','FC208','FD101','FD102','FD103','FD104','FD105','FD106','FD107','FD108','FD109','FD204','FD205','FD206','FD207','FD208','FD209','FD210','FD211','FD212','FD213','FD214','FS101','FS102','FS103','FS104','FS105','FS106','FS107','FS108','FS109','FS205','FS206','FS207','FS208','FS209','FS210','FS211','FS212','FS213','FS214','FC101','FC102','FC104','FC105','FC106','FC107','FC108','FC109','FC206','FC207','FC209','FC210','FC211','FC212','FC213','FC214','VEH10','VEU14','VEP18','FC205','FS204','FC204','EVX03','FD203','FS203','FC203','FD202','FS202','FC202','VEDCL','VEDEN']
att_sts = ['B' ,'D' ,'E' ,'F' ,'M' ,'T']
p_onl_pmt_desc = "NON-CASH - Online payment"
p_shopee_vc = "SHOPEEVC"
prm_eclaim_codes = ['03' ,'07' ,'11' ,'27']

# Get the last month-end from current system date
#last_mthend = datetime.strftime(datetime.now().replace(day=1) - timedelta(days=1), '%Y-%m-%d')

x = 1 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
last_mthend = last_day_of_x_months_ago.strftime('%Y-%m-%d')
rpt_mth = last_mthend[2:4]+last_mthend[5:7]
print("Selected last_mthend =", last_mthend)

# COMMAND ----------

# Set path to Azure parquet files
tpos_collection_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/TPOS_COLLECTION/'
tclaims_conso_all_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/TCLAIMS_CONSO_ALL/'
tpolicys_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TPOLICYS/'
tclient_policy_links_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_POLICY_LINKS/'
tclient_details_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_DETAILS/'
tpolidm_mthend_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/'
tcustdm_mthend_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/datamarts/TCUSTDM_MTHEND/'
tagtdm_mthend_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TAGTDM_MTHEND/'
tporidm_mthend_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPORIDM_MTHEND/'
vn_plan_code_map_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/VN_PLAN_CODE_MAP/'
tclaim_quota_edit_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLAIM_QUOTA_EDIT/'
tcoverages_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCOVERAGES/'

# Load file and store into Spark dataframe
tpos_collection_df = spark.read.format("parquet").load(tpos_collection_path)
tclaims_conso_all_df = spark.read.format("parquet").load(tclaims_conso_all_path)
tpolicys_df = spark.read.format("parquet").load(tpolicys_path)
tclient_policy_links_df = spark.read.format("parquet").load(tclient_policy_links_path)
tclient_details_df = spark.read.format("parquet").load(tclient_details_path)
tpolidm_mthend_df = spark.read.format("parquet").load(tpolidm_mthend_path)
tcustdm_mthend_df = spark.read.format("parquet").load(tcustdm_mthend_path)
tagtdm_mthend_df = spark.read.format("parquet").load(tagtdm_mthend_path)
tporidm_mthend_df = spark.read.format("parquet").load(tporidm_mthend_path)
vn_plan_code_map_df = spark.read.format("parquet").load(vn_plan_code_map_path)
tclaim_quota_edit_df = spark.read.format("parquet").load(tclaim_quota_edit_path)
tcoverages_df = spark.read.format("parquet").load(tcoverages_path)

# Convert all headers to lowercase
tpos_collection_df = tpos_collection_df.toDF(*[col.lower() for col in tpos_collection_df.columns])
tclaims_conso_all_df = tclaims_conso_all_df.toDF(*[col.lower() for col in tclaims_conso_all_df.columns])
tpolicys_df = tpolicys_df.toDF(*[col.lower() for col in tpolicys_df.columns])
tclient_policy_links_df = tclient_policy_links_df.toDF(*[col.lower() for col in tclient_policy_links_df.columns])
tclient_details_df = tclient_details_df.toDF(*[col.lower() for col in tclient_details_df.columns])
tpolidm_mthend_df = tpolidm_mthend_df.toDF(*[col.lower() for col in tpolidm_mthend_df.columns])
tcustdm_mthend_df = tcustdm_mthend_df.toDF(*[col.lower() for col in tcustdm_mthend_df.columns])
tagtdm_mthend_df = tagtdm_mthend_df.toDF(*[col.lower() for col in tagtdm_mthend_df.columns])
tporidm_mthend_df = tporidm_mthend_df.toDF(*[col.lower() for col in tporidm_mthend_df.columns])
vn_plan_code_map_df = vn_plan_code_map_df.toDF(*[col.lower() for col in vn_plan_code_map_df.columns])
tclaim_quota_edit_df = tclaim_quota_edit_df.toDF(*[col.lower() for col in tclaim_quota_edit_df.columns])
tcoverages_df = tcoverages_df.toDF(*[col.lower() for col in tcoverages_df.columns])

# Filter out non-use data months
tpolicys_df = tpolicys_df.filter(col("image_date") == last_mthend)
tclient_policy_links_df = tclient_policy_links_df.filter(col("image_date") == last_mthend)
tclient_details_df = tclient_details_df.filter(col("image_date") == last_mthend)
#tpos_collection_df = tpos_collection_df.filter(col("image_date") == last_mthend)
#tclaims_conso_all_df = tclaims_conso_all_df.filter(col("reporting_date") == last_mthend)
tpolidm_mthend_df = tpolidm_mthend_df.filter(col("image_date") == last_mthend)
tcustdm_mthend_df = tcustdm_mthend_df.filter(col("image_date") == last_mthend)
tagtdm_mthend_df = tagtdm_mthend_df.filter(col("image_date") == last_mthend)
tporidm_mthend_df = tporidm_mthend_df.filter(col("image_date") == last_mthend)

print("Number of records in tpos_collection: ", tpos_collection_df.count())
print("Number of records in tclaims_conso_all: ", tclaims_conso_all_df.count())
print("Number of records in tpolicys: ", tpolicys_df.count())
print("Number of records in tclient_policy_links: ", tclient_policy_links_df.count())
print("Number of records in tclient_details: ", tclient_details_df.count())
print("Number of records in tpolidm_mthend: ", tpolidm_mthend_df.count())
print("Number of records in tcustdm_mthend: ", tcustdm_mthend_df.count())
print("Number of records in tagtdm_mthend: ", tagtdm_mthend_df.count())
print("Number of records in tporidm_mthend: ", tporidm_mthend_df.count())
print("Number of records in vn_plan_code_map: ", vn_plan_code_map_df.count())
print("Number of records in tclaim_quota_edit_df: ", tclaim_quota_edit_df.count())
print("Number of records in tcoverages_df: ", tcoverages_df.count())

# COMMAND ----------

# Retrieve all payments in reporting month
cash_noncash_details = tpos_collection_df.filter(col("image_date") == last_mthend) \
    .select(
        col("image_date").alias("reporting_date"),
        col("transaction_amount").alias("trxn_amt"),
        col("batch_number").alias("btch_num"),
        when(substring(col("payment_method"), 1, 4) == "CASH", "N").otherwise("Y").alias("onl_ind"),
        col("client_number").alias("cli_num")
)
print("Number of records in cash_noncash_details:", cash_noncash_details.count())

# Summarize all non-cash transactions by customer
cash_noncash_cus_summary = cash_noncash_details \
    .groupBy(col("cli_num").alias("po_num")) \
        .agg(
            count(col("cli_num")).alias("pmt_cnt"),
            count(when(col("onl_ind") == "Y", col("cli_num"))).alias("pmt_onl_cnt"),
            sum(col("trxn_amt")).alias("txn_amt"),
            sum(when(col("onl_ind") == "Y", col("trxn_amt"))).alias("onl_txn_amt")) \
    .orderBy(col("po_num"))
print("Number of records in cash_noncash_cus_summary:", cash_noncash_cus_summary.count())

# Summarize all non-cash transactions by reporting month
cash_noncash_summary = cash_noncash_details \
    .groupBy(col("reporting_date")) \
        .agg(
            countDistinct(col("cli_num")).alias("total_pmt"),
            countDistinct(when(col("onl_ind") == "Y", col("cli_num"))).alias("onl_pmt"),
            sum(col("trxn_amt")).alias("txn_amt"),
            sum(when(col("onl_ind") == "Y", col("trxn_amt"))).alias("onl_txn_amt")) \
    .orderBy(col("reporting_date"))

# payment online activity
digital_payment = tpos_collection_df.filter(
    (col('payment_method') == p_onl_pmt_desc) | (col('bank_code') == p_shopee_vc)) \
        .groupBy(col('client_number').alias('cli_num')) \
            .agg(
                max(col('transaction_date')).alias('lst_trxn'))
            
print("Number of records in digital_payment:", digital_payment.count())

# COMMAND ----------

# CLAIM
# All claim submission in reporting month
claim = tclaims_conso_all_df.join(
    tpolicys_df,
    tclaims_conso_all_df["policy_number"] == tpolicys_df["pol_num"]
).join(
    tclient_policy_links_df,
    (tpolicys_df["pol_num"] == tclient_policy_links_df["pol_num"]) & 
    (tclient_policy_links_df["link_typ"] == "O")
).filter(
    (last_day(tclaims_conso_all_df["claim_received_date"]) == last_mthend) & 
    (tclaims_conso_all_df["reporting_date"] == last_mthend)
).select(
    tclient_policy_links_df["cli_num"].alias('po_num'),
    when((tclaims_conso_all_df['reporting_date'] >= '2020-02-29') & (tclaims_conso_all_df['submit_channel'] == 'CWS'), 'Y').otherwise(
        when(tclaims_conso_all_df['source_code'] == 'CWS', 'Y').otherwise('N')
    ).alias('clm_cws_ind'),
    when((tclaims_conso_all_df['reporting_date'] >= '2020-02-29') & (tclaims_conso_all_df['submit_channel'] == 'EasyClaims'), 'Y').otherwise(
        when(tclaims_conso_all_df['source_code'] == 'EZC', 'Y').otherwise('N')
    ).alias('clm_ezc_ind')
)
print("Number of records in claim:", claim.count())

# All claim online submissions
digital_claim = tclaims_conso_all_df.join(
    tpolicys_df,
    tclaims_conso_all_df["policy_number"] == tpolicys_df["pol_num"]
).join(
    tclient_policy_links_df,
    (tpolicys_df["pol_num"] == tclient_policy_links_df["pol_num"]) & 
    (tclient_policy_links_df["link_typ"] == "O")
).filter(
    when((tclaims_conso_all_df['reporting_date'] >= '2020-02-29') & 
         (tclaims_conso_all_df['submit_channel'].isin(['EasyClaims', 'CWS', 'DirectBilling'])), 1).otherwise(
        when(tclaims_conso_all_df['source_code'].isin(['EZC', 'CWS']), 1).otherwise(0)
    ) == 1
).groupBy(tclient_policy_links_df['cli_num']).agg(
    max(tclaims_conso_all_df['claim_received_date']).alias('lst_trxn')
)
print("Number of records in digital_claim:", digital_claim.count())

# COMMAND ----------

# Customer segmentation data
# Insured/Dependant's current age
c_age = tclient_details_df.join(
    tclient_policy_links_df,
    on=["cli_num", "image_date"]
).where(
    tclient_policy_links_df["link_typ"].isin(["I", "T"]) & 
    (tclient_details_df["image_date"] == last_mthend)
).select(
    tclient_details_df["cli_num"],
    floor(months_between(lit(last_mthend), tclient_details_df["birth_dt"]) / 12).alias("cur_age")
).distinct()

# Nested segmentation
seg = tpolidm_mthend_df.join(
    tcustdm_mthend_df,
    (tpolidm_mthend_df["image_date"] == tcustdm_mthend_df["image_date"]) & 
    (tpolidm_mthend_df["po_num"] == tcustdm_mthend_df["cli_num"]), 
    how="left"
).join(
    c_age,
    tpolidm_mthend_df["insrd_num"] == c_age["cli_num"], 
    how="left"
).where(
    tpolidm_mthend_df["image_date"] == last_mthend
).select(
    tpolidm_mthend_df["po_num"].alias("client_number"),
    when((tcustdm_mthend_df["cur_age"] > 46) & (tcustdm_mthend_df["no_dpnd"] > 0), 1).when(
        (tcustdm_mthend_df["cur_age"] > 46) & ((tcustdm_mthend_df["no_dpnd"].isNull()) | (tcustdm_mthend_df["no_dpnd"] == 0)), 2
    ).when(
        (tcustdm_mthend_df["cur_age"] <= 46) & (tcustdm_mthend_df["no_dpnd"] > 0) & (c_age["cur_age"] < 18), 3
    ).when(
        ((tcustdm_mthend_df["cur_age"] - c_age["cur_age"] >= 18) | (c_age["cur_age"] - tcustdm_mthend_df["cur_age"] >= 18)) &
        (c_age["cur_age"] >= 18) &
        (tcustdm_mthend_df["cur_age"] <= 46) &
        (tcustdm_mthend_df["no_dpnd"] > 0), 4
    ).when(
        ((tcustdm_mthend_df["cur_age"] - c_age["cur_age"] <= 10) | (c_age["cur_age"] - tcustdm_mthend_df["cur_age"] <= 10)) &
        (c_age["cur_age"] >= 18) &
        (tcustdm_mthend_df["cur_age"] <= 46) &
        (tcustdm_mthend_df["no_dpnd"] > 0), 5
    ).when(
        (tpolidm_mthend_df["po_num"] == tpolidm_mthend_df["insrd_num"]) &
        (tcustdm_mthend_df["cur_age"] <= 46) &
        ((tcustdm_mthend_df["no_dpnd"].isNull()) | (tcustdm_mthend_df["no_dpnd"] == 0)), 6
    ).otherwise(7).alias("segment"),
    tcustdm_mthend_df['no_dpnd']
)

# Customer's segmentation
cseg = seg.groupBy(col("client_number").alias("cli_num")).agg(
    min(col("segment")).alias("min_segment"),
    max(when(col("no_dpnd").isNull(), lit(0)).otherwise(col("no_dpnd"))).alias("max_no_dpnd")
).select(
    col("cli_num"),
    when(col("min_segment") == 1, "Empty Nester"). \
    when(col("min_segment") == 2, "Retired Self Insured"). \
    when(col("min_segment") == 3, "Family with Kids"). \
    when(col("min_segment") == 4, "Family"). \
    when(col("min_segment") == 5, "Adult with Dependent"). \
    when(col("min_segment") == 6, "Adult Self Insured"). \
    otherwise("Undefined Segmentation").alias("segment_type"),
    col("max_no_dpnd")
)
print("Number of records in cseg:", cseg.count())

# BACKUP
tinstdm_mthend = tclient_details_df.join(
    tclient_policy_links_df,
    on=["cli_num", "image_date"]
).where(
    tclient_policy_links_df["link_typ"].isin(["I", "T"])
).select(
    tclient_details_df["cli_num"],
    floor(months_between(lit(last_mthend), tclient_details_df["birth_dt"]) / 12).alias("cur_age")
).distinct()
print("Number of records in tinstdm_mthend:", tinstdm_mthend.count())


# COMMAND ----------

#  Get the latest plan code mapping
vn_plan_code_map_latest = vn_plan_code_map_df.groupBy(
    col("plan_code"),
    col("cvg_typ")
).agg(
    max(col("effective_date")).alias("effective_date")
)

vn_plan_code_map_dedup = vn_plan_code_map_df.join(
    vn_plan_code_map_latest,
    on=["effective_date", "plan_code", "cvg_typ"]
).select(vn_plan_code_map_df["*"])
print("Number of records in vn_plan_code_map_dedup:", vn_plan_code_map_dedup.count())


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>INFORMATION</strong>

# COMMAND ----------

# Policy base
policy_base = tpolidm_mthend_df.alias("pol")\
    .join(tagtdm_mthend_df.alias("agt"), 
          (col("pol.wa_code") == col("agt.agt_code")) & 
          (col("pol.image_date") == col("agt.image_date")), "left_outer")\
    .join(tcustdm_mthend_df.alias("cus"), 
          (col("pol.po_num") == col("cus.cli_num")) & 
          (col("pol.image_date") == col("cus.image_date")), "left_outer")\
    .join(vn_plan_code_map_dedup.alias("pln"), 
          col("pol.plan_code") == col("pln.plan_code"), "left_outer")\
    .where((col("pol.pol_eff_dt") <= last_mthend) & col("pol.pol_stat_cd").isin(active_sts) & 
           ~col("pol.plan_code").isin(exclusion_list_sub) & (col("pol.image_date") == last_mthend))\
    .selectExpr(
        "pol.pol_num",
        "pol.plan_code",
        "pol.po_num",
        "pol.insrd_num",
        """case
            when agt.loc_cd is null then (case
                when dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','88','49','51','52','53','58') then 'Banca'
                when dist_chnl_cd in ('48') then 'Affinity'
                when dist_chnl_cd in ('01', '02', '08', '50', '*') then 'Agency'
                when dist_chnl_cd in ('05','06','07','34','36','35') then 'DMTM'
                when dist_chnl_cd in ('09') then 'MI'
                else 'Unknown'
            end)
            when dist_chnl_cd in ('*') then 'Agency'
            else nvl(agt.channel,'Unknown')
        end as Channel""",
        "cus.frst_join_dt as frst_iss_dt",
        "pol.pol_iss_dt",
        "pol.pol_eff_dt",
        "agt.agt_code",
        "agt.loc_cd as loc_code_agt",
        f"""case when months_between(to_timestamp({last_mthend}),cus.frst_join_dt) < 3 then 'New Customer' else 'Existing Customer' end as customer_type""",
        "cus.sex_code as gender",
        "cus.birth_dt",
        """floor(months_between(nvl(pol.frst_iss_dt,pol.pol_iss_dt),cus.birth_dt) / 12) as age_frst_iss""",
        "cus.cur_age as age_curr",
        """case when cus.mobl_phon_num is null then 'N' else 'Y' end as mobile_contact""",
        """case when cus.email_addr is null then 'N' else 'Y' end as email_contact""",
        "cus.occp_code",
        "cus.mthly_incm",
        "cus.addr_typ",
        "cus.full_address",
        "cus.city",
        f"""floor(months_between(cus.image_date,cus.frst_join_dt) /12) as tenure""",
        f"""floor(months_between(pol.image_date,pol.pol_iss_dt)) as pol_tenure""",
        "'INFORCE' as cus_status",
        "pol.pol_trmn_dt as lst_pol_trmn_dt",
        """case when cus.cws_acct_id is not null then 'Y' else 'N' end cws_reg""",
        "pol.tot_ape",
        """case
            when agt.loc_cd is null then (case
                when dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','88','49','51','52','53','58') then round(pol.tot_ape*pln.nbv_margin_banca_other_banks,2) -- 'Banca'
                when dist_chnl_cd in ('48') then round(pol.tot_ape*pln.nbv_margin_other_channel_affinity,2)--'Affinity'
                when dist_chnl_cd in ('01','02','08','50','*') then round(pol.tot_ape*pln.nbv_margin_agency,2)--'Agency'
                when dist_chnl_cd in ('05','06','07','34','36','35') then round(pol.tot_ape*pln.nbv_margin_dmtm,2)--'DMTM'
                when dist_chnl_cd in ('09') then round(pol.tot_ape*-1.34041044648343,2)
                else round(pol.tot_ape*pln.nbv_margin_other_channel,2)
            end)
            when dist_chnl_cd in ('*') then round(pol.tot_ape*pln.nbv_margin_agency,2)
            when agt.loc_cd like 'TCB%' then round(pol.tot_ape*pln.nbv_margin_banca_tcb,2)
            when agt.loc_cd like 'SAG%' then round(pol.tot_ape*pln.nbv_margin_banca_scb,2)
            else round(pol.tot_ape*pln.nbv_margin_other_channel,2)
        end as nbv""",
        "cus.sms_ind",
        "cus.mobl_phon_num"
    )\
    .distinct()\
    .orderBy("pol.pol_num", "frst_iss_dt")
print("Number of records in policy_base:", policy_base.count())
policy_base.display(10)

# COMMAND ----------

# Product table details
product_table = tporidm_mthend_df \
    .join(policy_base, on=['pol_num'], how='inner') \
    .join(vn_plan_code_map_dedup, on=['plan_code'], how='left') \
    .where(
        (~tporidm_mthend_df.plan_code.isin(exclusion_list_sub)) &
        (tporidm_mthend_df.cvg_stat_cd.isin(active_sts)) &
        (tporidm_mthend_df.cvg_eff_dt <= last_mthend) &
        (policy_base.po_num.isNotNull()) &
        (tporidm_mthend_df.image_date == last_mthend)
    ) \
    .select(
        policy_base.po_num,
        policy_base.pol_num,
        tporidm_mthend_df.cvg_typ,
        tporidm_mthend_df.plan_code,
        vn_plan_code_map_dedup.customer_needs,
        tporidm_mthend_df.cvg_reasn,
        when(vn_plan_code_map_dedup.customer_needs == 'Accident', 1).otherwise(0).alias('prod_acc'),
        when(vn_plan_code_map_dedup.customer_needs == 'Critical Illness', 1).otherwise(0).alias('prod_ci'),
        when(vn_plan_code_map_dedup.customer_needs == 'Disability', 1).otherwise(0).alias('prod_dis'),
        when(vn_plan_code_map_dedup.customer_needs == 'GLH', 1).otherwise(0).alias('prod_glh'),
        when(vn_plan_code_map_dedup.customer_needs == 'Investments', 1).otherwise(0).alias('prod_inv'),
        when(vn_plan_code_map_dedup.customer_needs == 'Life Protection', 1).otherwise(0).alias('prod_lp'),
        when(vn_plan_code_map_dedup.customer_needs.isin(['Long Term Savings', 'LT Savings']), 1).otherwise(0).alias('prod_lts'),
        when(vn_plan_code_map_dedup.customer_needs == 'Medical', 1).otherwise(0).alias('prod_med'),
        tporidm_mthend_df.rid_anp,
        tporidm_mthend_df.face_amt,
        policy_base.pol_iss_dt,
        policy_base.pol_eff_dt,
        policy_base.pol_tenure
    ) \
    .orderBy('po_num', 'pol_num')
print("Number of records in product_table:", product_table.count())
product_table.display(10)

# COMMAND ----------

# Product table summary
product_table_summary = product_table \
    .groupBy('po_num') \
    .agg(
        count('pol_num').alias('prod_cnt'),
        when(sum('prod_acc') > 0, 1).otherwise(0).alias('need_acc'),
        when(sum('prod_ci') > 0, 1).otherwise(0).alias('need_ci'),
        when(sum('prod_dis') > 0, 1).otherwise(0).alias('need_dis'),
        when(sum('prod_glh') > 0, 1).otherwise(0).alias('need_glh'),
        when(sum('prod_inv') > 0, 1).otherwise(0).alias('need_inv'),
        when(sum('prod_lp') > 0, 1).otherwise(0).alias('need_lp'),
        when(sum('prod_lts') > 0, 1).otherwise(0).alias('need_lts'),
        when(sum('prod_med') > 0, 1).otherwise(0).alias('need_med')
    ) \
    .orderBy('po_num')
print("Number of records in product_table_summary:", product_table_summary.count())
product_table_summary_mtd = product_table \
    .where(product_table.pol_tenure == 0) \
    .groupBy('po_num') \
    .agg(
        count('pol_num').alias('prod_cnt'),
        when(sum('prod_acc') > 0, 1).otherwise(0).alias('need_acc'),
        when(sum('prod_ci') > 0, 1).otherwise(0).alias('need_ci'),
        when(sum('prod_dis') > 0, 1).otherwise(0).alias('need_dis'),
        when(sum('prod_glh') > 0, 1).otherwise(0).alias('need_glh'),
        when(sum('prod_inv') > 0, 1).otherwise(0).alias('need_inv'),
        when(sum('prod_lp') > 0, 1).otherwise(0).alias('need_lp'),
        when(sum('prod_lts') > 0, 1).otherwise(0).alias('need_lts'),
        when(sum('prod_med') > 0, 1).otherwise(0).alias('need_med')
    ) \
    .orderBy('po_num')
print("Number of records in product_table_summary_mtd:", product_table_summary_mtd.count())
product_table_summary_mtd.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC <font size="8">Customer Detail Information</font>

# COMMAND ----------

# Customer table (simple)
#from pyspark.sql.functions import when, min, max, sum

policy_base.cache()

customer_table = policy_base \
    .groupBy(
        'po_num', 'gender', 'birth_dt', 'age_curr', 'mobile_contact',
        'email_contact', 'occp_code', 'mthly_incm', 'cus_status',
        'cws_reg', 'full_address', 'city', 'sms_ind', 'mobl_phon_num'
    ) \
    .agg(
        when(min('age_frst_iss').isNull(), "0. Unknown")
            .when(min('age_frst_iss') <= 20, "1. <=20")
            .when((min('age_frst_iss') >= 21) & (min('age_frst_iss') <= 30), "2. 21-30")
            .when((min('age_frst_iss') >= 31) & (min('age_frst_iss') <= 40), "3. 31-40")
            .when((min('age_frst_iss') >= 41) & (min('age_frst_iss') <= 50), "4. 41-50")
            .when((min('age_frst_iss') >= 51) & (min('age_frst_iss') <= 60), "5. 51-60")
            .when(min('age_frst_iss') > 60, "6. >60")
            .alias('age_iss_group'),
        when(max('age_curr').isNull(), "0. Unknown")
            .when(max('age_curr') <= 20, "1. <=20")
            .when((max('age_curr') >= 21) & (max('age_curr') <= 30), "2. 21-30")
            .when((max('age_curr') >= 31) & (max('age_curr') <= 40), "3. 31-40")
            .when((max('age_curr') >= 41) & (max('age_curr') <= 50), "4. 41-50")
            .when((max('age_curr') >= 51) & (max('age_curr') <= 60), "5. 51-60")
            .when(max('age_curr') > 60, "6. >60")
            .alias('age_curr_group'),
        max('pol_iss_dt').alias('pol_iss_dt'),
        max('pol_eff_dt').alias('pol_eff_dt'),
        min('frst_iss_dt').alias('frst_iss_dt'),
        max('lst_pol_trmn_dt').alias('lst_trmn_dt'),
        max('tenure').alias('tenure'),
        sum('tot_ape').alias('tot_ape'),
        sum(when(policy_base.pol_tenure == 0, policy_base.tot_ape)).alias('ape_mtd'),
        sum('nbv').alias('nbv'),
        sum(when(policy_base.pol_tenure == 0, policy_base.nbv)).alias('nbv_mtd')
    )

customer_base_dedup = policy_base \
    .select(
        'po_num', 'channel', 'customer_type',
        'agt_code', 'loc_code_agt', 'pol_eff_dt'
    ) \
    .orderBy(policy_base.po_num, policy_base.pol_eff_dt.desc()) \
    .dropDuplicates(['po_num'])

customer_base_all = customer_table.alias('ct') \
    .join(
        customer_base_dedup.alias('cbd'),
        (col('ct.po_num') == col('cbd.po_num')) &
        (col('ct.pol_eff_dt') == col('cbd.pol_eff_dt')),
        'left'
    ) \
    .join(
        cash_noncash_cus_summary.alias('cncs'),
        col('ct.po_num') == col('cncs.po_num'),
        'left'
    ) \
    .select(
        'ct.*',
        when(col('cncs.po_num').isNull(), 0).otherwise(col('cncs.pmt_cnt')).alias('pmt_cnt'),
        when(col('cncs.po_num').isNull(), 0).otherwise(col('cncs.pmt_onl_cnt')).alias('pmt_onl_cnt'),
        'cbd.channel',
        'cbd.customer_type',
        'cbd.agt_code',
        'cbd.loc_code_agt'
    )
print("Number of records in customer_base_all:", customer_base_all.count())
customer_base_all.display(10)

# COMMAND ----------

# Customer table (final)
#from pyspark.sql.functions import when, greatest, col

customer_table_final = customer_base_all.alias("a") \
    .join(product_table_summary.alias("b"), 
          col("a.po_num") == col("b.po_num"), "left_outer") \
    .join(cseg.alias("c"), 
          col("a.po_num") == col("c.cli_num"), "left_outer") \
    .join(product_table_summary_mtd.alias("d"), 
          col("a.po_num") == col("d.po_num"), "left_outer") \
    .join(claim.alias("clm"), 
          col("a.po_num") == col("clm.po_num"), "left_outer") \
    .join(claim.alias("clm_cws"), 
          (col("a.po_num") == col("clm_cws.po_num")) & 
          (col("clm_cws.clm_cws_ind") == 'Y'), "left_outer") \
    .join(claim.alias("clm_ezc"), 
          (col("a.po_num") == col("clm_ezc.po_num")) & 
          (col("clm_ezc.clm_ezc_ind") == 'Y'), "left_outer") \
    .join(tpolicys_df.alias("pol")
          .join(tclient_policy_links_df.alias("cpl"), 
                (col("pol.pol_num") == col("cpl.pol_num")) & 
                (col("cpl.link_typ") == 'O') & (col("cpl.rec_status") == 'A'))
          .join(tcoverages_df.alias("cvg"), 
                col("pol.pol_num") == col("cvg.pol_num"))
          .join(tclaim_quota_edit_df.alias("cqe"), 
                col("cvg.plan_code") == col("cqe.plan_code"))
          .where((col('cvg.cvg_stat_cd').isin(active_sts)) & 
                 (col('cqe.clm_code').isin(prm_eclaim_codes)))
          .select(col('cpl.cli_num').alias('cli_num')).distinct()
          .alias('clm_reg'), 
          col('a.po_num') == col('clm_reg.cli_num'), 'left_outer') \
    .join(digital_payment.alias('pmt'), 
          col('a.po_num') == col('pmt.cli_num'), 'left_outer') \
    .join(digital_claim.alias('e_clm'), 
          col('a.po_num') == col('e_clm.cli_num'), 'left_outer') \
    .join(tcustdm_mthend_df.alias('tc'), 
          (col('a.po_num') == col('tc.cli_num')) & 
          (col('tc.image_date') == last_mthend), 'left_outer') \
    .select(
        "a.po_num",
        "a.gender",
        "a.birth_dt",
        "a.age_curr",
        "a.age_iss_group",
        "a.age_curr_group",
        "a.customer_type",
        "a.loc_code_agt",
        "a.mobile_contact",
        "a.email_contact",
        "a.occp_code",
        "a.mthly_incm",
        "a.full_address",
        "a.city",
        "a.pol_iss_dt",
        "a.pol_eff_dt",
        "a.frst_iss_dt",
        "a.agt_code",
        "c.segment_type",
        "c.max_no_dpnd",
        "a.channel",
        "a.tenure",
        when((col('a.cws_reg') == 'Y') | (col('tc.cws_ind') == 'Y'), 'Y').otherwise('N').alias('cws_reg'),
        greatest(col('tc.cws_lst_login_dt_legacy'), col('tc.cws_lst_login_dt')).alias('cws_lst_login_dt'),
        when(col('clm_reg.cli_num').isNotNull(), 'Y').otherwise('N').alias('claim_reg'),
        when(col('clm.po_num').isNotNull(), 'Y').otherwise('N').alias('clm_ind'),
        when(col('clm_cws.po_num').isNotNull(), 'Y').otherwise('N').alias('clm_cws_ind'),
        when(col('clm_ezc.po_num').isNotNull(), 'Y').otherwise('N').alias('clm_ezc_ind'),
        col('e_clm.lst_trxn').alias('e_clm_lst_trxn_dt'),
        col('a.pmt_cnt'),
        col('a.pmt_onl_cnt'),
        when(col('a.pmt_cnt') > 0, 'Y').otherwise('N').alias('pmt_ind'),
        when(col('a.pmt_onl_cnt') > 0, 'Y').otherwise('N').alias('pmt_onl_ind'),
        when(col('pmt.cli_num').isNotNull(), 'Y').otherwise('N').alias('e_pmt_reg'),
        col('pmt.lst_trxn').alias('e_pmt_lst_trxn_dt'),
        col('tc.move_ind').alias('move_reg'),
        col('tc.move_lst_login_dt'),
        when(when((col('a.cws_reg') == 'Y') | (col('tc.cws_ind') == 'Y'), 'Y').otherwise('N') == 'Y', 'Y').otherwise('N').alias('digital_reg_ind'),
        when(when(when((col('a.cws_reg') == 'Y') | (col('tc.cws_ind') == 'Y'), 'Y').otherwise('N') == 'Y', 'Y').otherwise(col('tc.move_ind')) == 'Y', 'Y').otherwise('N').alias('digital_reg_ind_v2'),
        col("a.tot_ape"),
        col("a.ape_mtd"),
        col("a.nbv"),
        col("a.nbv_mtd"),
        col("b.prod_cnt"),
        col("d.prod_cnt").alias("prod_cnt_mtd"),
        (col("b.need_acc") + col("b.need_ci") + col("b.need_dis") + col("b.need_glh") + col("b.need_inv") + col("b.need_lp") + col("b.need_lts") + col("b.need_med")).alias("need_cnt"),
        when((col("b.need_acc") + col("b.need_ci") + col("b.need_dis") + col("b.need_glh") + col("b.need_inv") + col("b.need_lp") + col("b.need_lts") + col("b.need_med")) > 1, "MULTIPLE").otherwise("SINGLE").alias("need_type"),
        "a.sms_ind",
        "a.mobl_phon_num"
    ).distinct()
print("Number of records in customer_table_final:", customer_table_final.count())
customer_table_final.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store customer table final data</strong>

# COMMAND ----------

customer_table_final.write.mode("overwrite").parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/customer_table_{rpt_mth}")
