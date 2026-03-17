# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##0. Import libraries and declare local variables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, LongType, DoubleType
import pyspark.sql.window as Window
import pandas as pd

# First snapshot date
min_snapshot_str = '2024-08-31'
max_snapshot_str = '2024-12-31'

# List of all campaign ids
campaign_list = ['MIL-AVY-20250114','MIL-AVY-20250204','MIL-AVY-20250214','MIL-AVY-20250305','MIL-AVY-20250317',  # AWO Pilot
                 'MIL-MAT-20250114','MIL-MAT-20250204','MIL-MAT-20250214','MIL-MAT-20250305','MIL-MAT-20250317',
                 'MIL-PRD-20240901',                                        # Pre-approved UW
                 'CTR-PRD-20240901','CTR-AVY-20250114','CTR-MAT-20250114'   # Control groups
                ]
campaign_str = ','.join(f"'{key}'" for key in campaign_list)

# Columns from TARGETM_DM to select
tgt_cols = ['tgt_id', 'cmpgn_id', 'btch_id', 'batch_start_date', 'batch_end_date', 'tgt_cust_id', 'prev_pol_num', 
            'tgt_agt_code', 'tgt_agt_loc_code', 'tgt_agt_regional_head']

# Columns from TRACKINGM to select
trk_cols = ['tgt_id', 'cmpgn_id', 'btch_id', 'new_pol_cust_id', 'new_own_prev_cli_type', 'new_pol_sbmt_dt', 'new_cvg_eff_dt',
            'new_pol_num', 'new_pol_plan_cd', 'new_pol_ape', 'new_pol_cvg_typ', 'new_pol_nbv', 'new_pol_stat', 'new_pol_ind', 'assign_agent_ind',
            'new_pol_writing_agt']

# Columns from TPOLIDM_MTHEND to select
pol_cols = ['po_num', 'pol_num', 'plan_code', 'insrd_num', 'pol_stat_cd', 'sa_code', 'plan_nm_by_chnl', 'rel_to_insrd', 'image_date']

# Columns from TCUSTDM_MTHEND to select
cus_cols = ['cli_num', 'cur_age', 'occp_code', 'image_date']

LEAD_PATH = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
DM_PATH = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
CAS_PATH = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'

# COMMAND ----------

# Derive exchange rate from the latest snapshot date
exrt_string = f'''
with xrt as (
select  cast(XCHNG_RATE as int) ex_rate,
        row_number() over (partition by XCHNG_RATE_TYP order by FR_EFF_DT DESC) rn
from    vn_published_cas_db.texchange_rates
where   XCHNG_RATE_TYP='U'
    and FR_CRCY_CODE='78'
    and to_date(FR_EFF_DT) <= '{max_snapshot_str}'
qualify rn=1
) select ex_rate from xrt
'''
exrt_df = sql_to_df(exrt_string, 1, spark)
ex_rate = exrt_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Load campaign leads and Customers/Agents related information tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### _Lead tables to include_:<br>
# MAGIC > <b>Target<br>
# MAGIC >><i>`target data`<br>
# MAGIC
# MAGIC > <b>Tracking<br>
# MAGIC >><i>`trackingm data`
# MAGIC
# MAGIC > <b>Datamart<br>
# MAGIC >><i>`tpolidm_mthend data`<br>
# MAGIC >>`tcustdm_mthend data`<br>
# MAGIC >>`tams_agents data`

# COMMAND ----------

# Load TARGET and TRACKING data and apply filters
target_raw = spark.read.format('parquet').load(LEAD_PATH + 'TARGETM_DM')
target_df = target_raw.filter(
    (F.col('cmpgn_id').isin(campaign_list)) & (~F.col('btch_id').startswith('9')) # Exclude UCM leads
).select(*tgt_cols)

# Convert batch_start_date of all batches to the first one
target_df = target_df.withColumn(
    'group',
    F.when(F.col("cmpgn_id").like("MIL-%"), "Target")
     .when(F.col("cmpgn_id").like("CTR-%"), "Control")
    .otherwise(None)  # You can set this to any default value if needed
).withColumn(
    'campaign_name',
    F.when(F.col('cmpgn_id').like('%-PRD-%'), F.lit('Pre-approved UW'))
    .otherwise(F.lit('AWO'))
).withColumn(
    'batch_start_date',
    F.when(F.col('campaign_name') == F.lit('Pre-approved UW'), F.lit('2024-09-01'))
    .otherwise(F.lit('2025-01-14'))
)

tracking_raw = spark.read.format('parquet').load(LEAD_PATH + 'TRACKINGM')
tracking_df = tracking_raw.filter(
    (F.col('cmpgn_id').isin(campaign_list)) & (~F.col('btch_id').startswith('9')) # Exclude UCM leads
).select(*trk_cols)

# Identify the date where campaign ends
end_date = target_df.agg(F.max('batch_end_date')).collect()[0][0]

# Load CONTACT data
ctct_df = spark.read.parquet(CAS_PATH + 'TCONTACT_RESULTS/').filter(
    (F.col('trxn_dt') > min_snapshot_str) &
    (F.col('trxn_dt') <= end_date)
).select('cli_num', 'trxn_dt', 'contact_rslt')

# Load latest Policy datamart as of `end_date`
tpolidm_df = spark.read.format('parquet').load(DM_PATH + 'TPOLIDM_MTHEND').filter(
    F.col('image_date').isin([min_snapshot_str, max_snapshot_str])
).select(*pol_cols)

# Load latest Customer datamart as of `end_date`
tcustdm_df = spark.read.format('parquet').load(DM_PATH + 'TCUSTDM_MTHEND').filter(
    F.col('image_date').isin([min_snapshot_str, max_snapshot_str])
).select(*cus_cols)

# Load and derive customers' occupation
occp_cols = {
    "industry": F.split(F.col("occp_desc"), " - ").getItem(0),
    "occupation": F.split(F.col("occp_desc"), " - ").getItem(1)
}
occp_df = spark.sql("""
select  occp_code, occp_desc
from    vn_published_cas_db.toccupation
""") \
    .withColumns(occp_cols) \
    .drop('occp_desc')

# Merge TCUSTDM_MTHEND with occupation
tcustdm_df = tcustdm_df.join(F.broadcast(occp_df), 'occp_code', 'left')
tcustdm_df = tcustdm_df.withColumnRenamed('cli_num', 'po_num')

#tpolidm_df.limit(2).display()
#ctct_df.limit(2).display()
target_df.groupBy('group','campaign_name','batch_start_date').agg(F.countDistinct('tgt_cust_id').alias('cus_count')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC > <b>Sales<br>
# MAGIC >><i>`po_num (po type: owner, main insured, beneficiary, oth insured)`<br>
# MAGIC >>`product (code, insurance type, base/rider only indicator)`<br>
# MAGIC >>`APE / income`<br>
# MAGIC >>`sales agent information (channel, location, title...)`<br>
# MAGIC >>`rider type (HCR/MC)`

# COMMAND ----------

# MAGIC %md
# MAGIC ### _Feature tables to include_:<br>
# MAGIC > <b>Customer<br>
# MAGIC >><i>`respond`<br>
# MAGIC >>`product portfolio`<br>
# MAGIC >>`APE / income`<br>
# MAGIC >>`demographic (age, tenure, location, occupation class)`<br>
# MAGIC >>`rider type (HCR/MC)`
# MAGIC
# MAGIC > <b>Agent<br>
# MAGIC >><i>`active`<br>
# MAGIC >>`tier`<br>
# MAGIC >>`# customers servicing`<br>
# MAGIC >>`common info (tenure, location, region, rank type)`

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Customer

# COMMAND ----------

# Select the features needed from the INCOME_BASED_DECILE
inc_cols = ['po_num', 'client_tenure', 'mthly_incm', 'no_dpnd', 'multi_prod', 'pol_count', 'inforce_pol', 
            'investment_pol', 'endow_pol', 'term_pol', 'whole_pol', 'health_indem_pol',
            'total_ape', 'rider_ape', 'rider_cnt', 'income_decile', 'city', 'image_date']
INC_PATH = '/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'

# Load data from parquet files and merge with snapshot being 1 month prior to campaign dates
inc_agency_raw = spark.read.format('parquet').load(INC_PATH + 'INCOME_BASED_DECILE_AGENCY/') \
    .filter(F.col('image_date').isin([min_snapshot_str, max_snapshot_str])) \
    .select(*inc_cols)
inc_pd_raw = spark.read.format('parquet').load(INC_PATH + 'INCOME_BASED_DECILE_PD/') \
    .filter(F.col('image_date').isin([min_snapshot_str, max_snapshot_str])) \
    .select(*inc_cols)

# Merge both Agency and Banca customers' features
inc_df = inc_agency_raw.union(inc_pd_raw).dropDuplicates(['po_num','image_date'])

# Categorize features
inc_df = inc_df.withColumn('client_tenure_cat',
        F.when(F.col('client_tenure') < 1, F.lit("1 Less than 1yr"))
        .when(F.col('client_tenure') < 2, F.lit("2 1-2yr"))
        .when(F.col('client_tenure') < 3, F.lit("3 2-3yr"))
        .when(F.col('client_tenure') < 5, F.lit("4 3-5yr"))
        .when(F.col('client_tenure') < 7, F.lit("5 5-7yr"))
        .when(F.col('client_tenure') < 10, F.lit("6 7-10yr"))
        .otherwise("7 More than 10yr")) \
    .withColumn('inforce_pol_holding',
        F.when(F.col('inforce_pol') < 1, F.lit("0 No inforce policy"))
        .when(F.col('inforce_pol') < 2, F.lit("1 Only 1 policy"))
        .when(F.col('inforce_pol') < 3, F.lit("2 2 policies"))
        .when(F.col('inforce_pol') < 5, F.lit("3 3-4 policies"))
        .when(F.col('inforce_pol') <= 7, F.lit("4 5-7 policies"))
        .when(F.col('inforce_pol') <= 10, F.lit("5 8-10 policies"))
        .otherwise("6 More than 10 policies")) \
    .withColumn('rider_typ',
        F.when(F.col('rider_cnt') > 0, F.lit("2 Customer with Rider")).otherwise("1 Customer without Rider")) \
    .withColumn('rider_count_cat',
        F.when(F.col('rider_cnt') == 0, F.lit("1 No rider"))
        .when(F.col('rider_cnt') == 1, F.lit("2 1 rider"))
        .when(F.col('rider_cnt') <= 3, F.lit("3 2-3 riders"))       
        .when(F.col('rider_cnt') <= 5, F.lit("4 4-5 riders"))
        .otherwise("5. More than 5 riders")) \
    .withColumn('prod_type',
        F.when(
            (F.col('multi_prod') == 0) &
            (
                (F.col('investment_pol') > 0) |
                (F.col('term_pol') > 0) |
                (F.col('endow_pol') > 0) |
                (F.col('whole_pol') > 0) |
                (F.col('health_indem_pol') > 0)
            ),
            F.when(F.col('investment_pol') > 0, F.lit("1 Investment Only"))
            .when(F.col('term_pol') > 0, F.lit("2 Term Only"))
            .when(F.col('endow_pol') > 0, F.lit("3 Endowment Only"))
            .when(F.col('whole_pol') > 0, F.lit("4 Whole Life Only"))
            .when(F.col('health_indem_pol') > 0, F.lit("5 Health Only"))
        )
        .otherwise("6 Multiple")) \
    .withColumn('ape_holding',
        F.when(F.col('total_ape') < 500, F.lit("1 Below $500"))
        .when(F.col('total_ape') <= 1000, F.lit("2 $500-$1000"))
        .when(F.col('total_ape') <= 1500, F.lit("3 $1000-$1500"))
        .when(F.col('total_ape') <= 2000, F.lit("4 $1500-$2000"))
        .when(F.col('total_ape') <= 3000, F.lit("5 $2000-$3000"))
        .when(F.col('total_ape') <= 5000, F.lit("6 $3000-$5000"))
        .when(F.col('total_ape') <= 7000, F.lit("7 $5000-$7000"))
        .when(F.col('total_ape') <= 10000, F.lit("8 $7000-$10000"))
        .otherwise("9 Above $10k")) \
    .withColumn('mthly_incm_range',
        F.when(F.col('mthly_incm').isNull() | (F.col('mthly_incm') == 0), F.lit("0 Not declared"))
        .when(F.col('mthly_incm') < 500, F.lit("1 Below $500"))
        .when(F.col('mthly_incm') <= 1000, F.lit("2 $500-$1000"))
        .when(F.col('mthly_incm') <= 1500, F.lit("3 $1000-$1500"))
        .when(F.col('mthly_incm') <= 2000, F.lit("4 $1500-$2000"))
        .when(F.col('mthly_incm') <= 3000, F.lit("5 $2000-$3000"))
        .when(F.col('mthly_incm') <= 5000, F.lit("6 $3000-$5000"))
        .when(F.col('mthly_incm') <= 7000, F.lit("7 $5000-$7000"))
        .when(F.col('mthly_incm') <= 10000, F.lit("8 $7000-$10000"))
        .otherwise("9 Above $10k")) \

# Columns to drop
inc_drop_cols = ['client_tenure', 'mthly_incm', 'no_dpnd', 'pol_count', 'total_ape', 'rider_ape', 'rider_cnt', 'income_decile']

inc_final_df = inc_df.drop(*inc_drop_cols)

# Validation
#inc_df.groupBy('image_date').agg(F.count('po_num').alias('row_count'), F.countDistinct('po_num').alias('po_count')).sort('image_date').display()
#occp_df.limit(5).display()
#inc_final_df.filter(F.col('image_date') == max_snapshot_str).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Agents

# COMMAND ----------

par_cols = ['agt_cd', 'rank_code', 'br_code', 'tenure_mth', 'tier', 'last_9m_pol', 'image_date']
TPAR_PATH = '/mnt/lab/vn/project/cpm/datamarts/TPARDM_MTHEND/'

tpar_df = spark.read.format('parquet').load(TPAR_PATH).filter(
    F.col('image_date').isin([min_snapshot_str, max_snapshot_str])
).select(*par_cols)

tpar_df = tpar_df.withColumn('agt_type',
    F.when(F.col('last_9m_pol') == 0, 'SA').otherwise('Active')) \
    .withColumn('agent_tenure_cat',
    F.when(F.col('tenure_mth') < 12, F.lit("1 Less than 1yr"))
    .when(F.col('tenure_mth') < 24, F.lit("2 1-2yr"))
    .when(F.col('tenure_mth') < 36, F.lit("3 2-3yr"))
    .when(F.col('tenure_mth') < 60, F.lit("4 3-5yr"))
    .when(F.col('tenure_mth') < 84, F.lit("5 5-7yr"))
    .when(F.col('tenure_mth') < 120, F.lit("6 7-10yr"))
    .otherwise("7 More than 10yr")
    ) \
    .drop('tenure_mth', 'last_9m_pol')

#tpar_df.filter(F.col('image_date') == '2024-08-31').groupBy('image_date', 'agt_type', 'tier').agg(F.countDistinct('agt_cd').alias('agt_count')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consolidate all the imported data

# COMMAND ----------

# %sql

# with leads as (
#   select distinct
#         cmpgn_id, 
#         batch_start_date, batch_end_date, 
#         tgt_cust_id as old_po_num, 
#         --prev_pol_num as old_pol_num,
#         tgt_agt_code as old_agt_code,
#         tgt_agt_loc_code as old_loc_code,
#         max(case when ctc.contact_rslt = '01' then 'Y' else 'N' end) as contacted_ind,
#         last_day(add_months(batch_start_date, -1)) image_date
#   from  targetm_dm tgt 
#   left join tcontact_results ctc 
#         on tgt.tgt_cust_id = ctc.cli_num and 
#            (to_date(ctc.trxn_dt) between tgt.batch_start_date and tgt.batch_end_date)
#   group by
#         cmpgn_id, batch_start_date, batch_end_date, tgt_cust_id, --prev_pol_num, 
#         tgt_agt_code, tgt_agt_loc_code
# ), sales_dtl as (
#   select  distinct
#           tgt.cmpgn_id, 
#           tgt_cust_id as old_po_num, 
#           new_pol_cust_id as new_po_num, 
#           prev_pol_num as old_pol_num, 
#           new_pol_num,
#           case when new_own_prev_cli_type = 'Existing Customer' then 1 else 0 end as assign_lead_ind, 
#           last_day(to_date(new_pol_sbmt_dt)) as sbmt_dt, 
#           last_day(to_date(new_cvg_eff_dt)) as cvg_eff_dt,
#           new_pol_plan_cd, 
#           cast(new_pol_ape as int) as new_pol_ape, 
#           new_pol_cvg_typ, 
#           cast(new_pol_nbv as decimal(11,2)) as new_pol_nbv, 
#           new_pol_stat,
#           new_pol_ind, 
#           case when tgt_agt_code = new_pol_writing_agt then 1 else 0 end as assign_agent_ind, 
#           new_pol_writing_agt,
#           case when tgt_agt_loc_code = agt.loc_code then 1 else 0 end as assign_loc_code_ind,
#           agt.loc_code new_loc_code,
#           case when agt.chnl_cd = '01' then 1 else 0 end as assign_channel_ind,
#           case when new_pol_cvg_typ = 'B' then 1 else 0 end as new_base_ind,
#           case when new_pol_cvg_typ = 'R' then 1 else 0 end as new_rider_ind,
#           case when pln.prod_cat='U' then
#             case when trk.new_pol_plan_cd in ('UL004','UL005','UL035','UL036') then 'UL-Single-Premium'
#                  when trk.new_pol_plan_cd not in ('UL004','UL005','UL035','UL036') and trk.new_pol_plan_cd like 'UL%' then 'ULI'
#                  when trk.new_pol_plan_cd like 'RUV%' then 'UILP'
#                  else 'UL-Others'
#             end
#             when pln.prod_cat='T' then
#             case when trk.new_pol_plan_cd in ('PN001','PA007','PA008') then 'Single-Year'
#                  when trk.new_pol_plan_cd in ('FDB01','BIC01','BIC02','BIC03','BIC04') then 'Digital'
#                  when trk.new_pol_plan_cd in ('CA360','CX360','MI007') then 'Activator-Micro'
#                  when trk.new_pol_plan_cd like 'RP%' then 'TROP'
#                  else 'Traditional'
#             end
#             else 'Others'
#           end as new_pol_prod_cat,
#           case when pln.prod_typ='HC_RIDER' then 1 else 0 end as hcr_ind,
#           case when pln.prod_typ='MC_RIDER' then 1 else 0 end as mc_ind
#   from    targetm_dm tgt 
#   inner join trackingm trk 
#           on tgt.tgt_id = trk.tgt_id and tgt.cmpgn_id = trk.cmpgn_id 
#   left join vn_published_casm_ams_snapshot_db.tams_agents agt
#           on trk.new_pol_writing_agt = agt.agt_code and last_day(to_date(new_pol_sbmt_dt)) = agt.image_date
#   left join vn_published_cas_db.tplans pln 
#           on trk.new_pol_plan_cd = pln.plan_code
# ), sales_sum as (
#   select  cmpgn_id, old_po_num, new_po_num,     --new_own_prev_cli_type as rel_to_old_po,
#           --old_pol_num, new_pol_num, new_pol_prod_cat, new_pol_writing_agt as new_agt_code, new_loc_code,
#           max(assign_lead_ind) as assign_lead_ind,
#           max(sbmt_dt) as sbmt_dt,
#           max(cvg_eff_dt) as cvg_eff_dt,
#           max(assign_agent_ind) as assign_agent_ind,
#           max(assign_loc_code_ind) as assign_loc_code_ind,
#           max(assign_channel_ind) as assign_channel_ind,
#           max(new_base_ind) as new_base_ind,
#           count(distinct case when new_base_ind = 1 then new_pol_num end) as new_base_count,
#           count(distinct case when new_base_ind = 1 and new_pol_prod_cat = 'UILP' then new_pol_num end) new_ilp_count,
#           count(distinct case when new_base_ind = 1 and new_pol_prod_cat = 'ULI' then new_pol_num end) new_ul_count,
#           count(distinct case when new_base_ind = 1 and new_pol_prod_cat = 'TROP' then new_pol_num end) new_trop_count,
#           count(distinct case when new_base_ind = 1 and new_pol_prod_cat not in ('UILP', 'ULI', 'TROP') then new_pol_num end) new_other_count,
#           max(new_rider_ind) as new_rider_ind,
#           count(case when new_rider_ind = 1 then new_pol_num end) as new_rider_count,
#           sum(new_pol_ape) as new_ape_total,
#           sum(case when new_base_ind = 1 then new_pol_ape else 0 end) as new_ape_base,
#           sum(case when new_rider_ind = 1 then new_pol_ape else 0 end) as new_ape_rider,
#           sum(new_pol_nbv) as new_nbv_total,
#           sum(case when new_base_ind = 1 then new_pol_nbv else 0 end) as new_nbv_base,
#           sum(case when new_rider_ind = 1 then new_pol_nbv else 0 end) as new_nbv_rider,
#           max(hcr_ind) as hcr_ind,
#           max(mc_ind) as mc_ind
#   from    sales_dtl
#   group by
#           cmpgn_id, old_po_num, new_po_num      --, new_own_prev_cli_type, 
#           --old_pol_num, new_pol_num, new_pol_prod_cat, new_pol_writing_agt, new_loc_code
# ), cmpgn_sales as (
# select  ld.*, new_po_num, assign_lead_ind, sbmt_dt, cvg_eff_dt, --new_pol_num, new_pol_prod_cat, new_agt_code, new_loc_code,
#         assign_agent_ind, assign_loc_code_ind, assign_channel_ind, new_base_ind, new_base_count, new_ilp_count, new_ul_count, new_trop_count, new_other_count,
#         new_rider_ind, new_rider_count,
#         new_ape_total, new_ape_base, new_ape_rider, new_nbv_total, new_nbv_base, new_nbv_rider
# from    leads ld 
# left join sales_sum sls 
# on ld.cmpgn_id = sls.cmpgn_id and ld.old_po_num = sls.old_po_num
# )
# select  cmpgn_id, image_date, count(*) row_count, count(distinct old_po_num) po_count, count(distinct old_agt_code) agt_count, 
#         count(distinct case when new_po_num is not null then new_po_num end) response_count,
#         count(distinct case when assign_agent_ind = 1 then old_agt_code end) activated_agt_count
# from    cmpgn_sales
# group by cmpgn_id, image_date

# COMMAND ----------

target_df.createOrReplaceTempView("targetm_dm")
tracking_df.createOrReplaceTempView("trackingm")
tpolidm_df.createOrReplaceTempView("tpolidm_mthend")
tcustdm_df.createOrReplaceTempView('tcustdm_mthend')
ctct_df.createOrReplaceTempView("tcontact_results")
tpar_df.createOrReplaceTempView("tpardm_mthend")

# tracking_df.groupBy('cmpgn_id') \
#     .agg(F.countDistinct('new_pol_cust_id').alias('respond_count'),
#          F.countDistinct('new_pol_writing_agt').alias('agent_count'),
#          F.sum(F.when(F.col('new_pol_cvg_typ') == 'B', 1)).alias('new_pol_count'),
#          F.sum('new_pol_ape').alias('total_ape'),
#          F.sum('new_pol_nbv').cast('int').alias('total_nbv')
#          ) \
#     .display()

sql_string = '''
with cus_agt as (
  select image_date, sa_code as agt_code, count(po_num) as servicing_cus_count
  from  tpolidm_mthend
  where pol_stat_cd in ('1','3','5')
  group by image_date, sa_code
), leads as (
  select distinct
        group, campaign_name, cmpgn_id, last_day(add_months(batch_start_date, -1)) as image_date, batch_start_date, batch_end_date, 
        tgt_cust_id as old_po_num, 
        tgt_agt_code as old_agt_code,
        tgt_agt_loc_code as old_loc_code,
        tgt_agt_regional_head as rh_name,
        max(cus.servicing_cus_count) servicing_cus_count,
        max(case when ctc.contact_rslt = '01' then 'Y' else 'N' end) as contacted_ind
  from  targetm_dm tgt 
  left join tcontact_results ctc 
        on tgt.tgt_cust_id = ctc.cli_num and 
           (to_date(ctc.trxn_dt) between tgt.batch_start_date and tgt.batch_end_date)
  left join cus_agt cus 
        on tgt.tgt_agt_code = cus.agt_code and last_day(add_months(tgt.batch_start_date, -1)) = cus.image_date
  group by
        group, campaign_name, cmpgn_id, batch_start_date, batch_end_date, tgt_cust_id, tgt_agt_code, tgt_agt_loc_code, tgt_agt_regional_head
), sales_dtl as (
  select  distinct
          tgt.group,
          tgt.campaign_name,
          tgt.cmpgn_id, 
          tgt_cust_id as old_po_num, 
          new_pol_cust_id as new_po_num, 
          prev_pol_num as old_pol_num, 
          new_pol_num,
          case when new_own_prev_cli_type = 'Existing Customer' then 1 else 0 end as assign_lead_ind, 
          last_day(to_date(new_pol_sbmt_dt)) as sbmt_dt, 
          last_day(to_date(new_cvg_eff_dt)) as cvg_eff_dt,
          new_pol_plan_cd, 
          cast(new_pol_ape as int) as new_pol_ape, 
          new_pol_cvg_typ, 
          cast(new_pol_nbv as decimal(11,2)) as new_pol_nbv, 
          new_pol_stat,
          new_pol_ind, 
          case when tgt_agt_code = new_pol_writing_agt then 1 else 0 end as assign_agent_ind, 
          new_pol_writing_agt,
          case when tgt_agt_loc_code = agt.loc_code then 1 else 0 end as assign_loc_code_ind,
          agt.loc_code new_loc_code,
          case when agt.chnl_cd = '01' then 1 else 0 end as assign_channel_ind,
          case when new_pol_cvg_typ = 'B' then 1 else 0 end as new_base_ind,
          case when new_pol_cvg_typ in ('R','W') then 1 else 0 end as new_rider_ind,
          case when pln.prod_cat='U' then
            case when trk.new_pol_plan_cd in ('UL004','UL005','UL035','UL036') then 'UL-Single-Premium'
                 when trk.new_pol_plan_cd not in ('UL004','UL005','UL035','UL036') and trk.new_pol_plan_cd like 'UL%' then 'ULI'
                 when trk.new_pol_plan_cd like 'RUV%' then 'UILP'
                 else 'UL-Others'
            end
            when pln.prod_cat='T' then
            case when trk.new_pol_plan_cd in ('PN001','PA007','PA008') then 'Single-Year'
                 when trk.new_pol_plan_cd in ('FDB01','BIC01','BIC02','BIC03','BIC04') then 'Digital'
                 when trk.new_pol_plan_cd in ('CA360','CX360','MI007') then 'Activator-Micro'
                 when trk.new_pol_plan_cd like 'RP%' then 'TROP'
                 else 'Traditional'
            end
            else 'Others'
          end as new_pol_prod_cat,
          case when pln.prod_typ='HC_RIDER' then 1 else 0 end as hcr_ind,
          case when pln.prod_typ='MC_RIDER' then 1 else 0 end as mc_ind
  from    targetm_dm tgt 
  inner join trackingm trk 
          on tgt.tgt_id = trk.tgt_id and tgt.cmpgn_id = trk.cmpgn_id 
  left join vn_published_casm_ams_snapshot_db.tams_agents agt
          on trk.new_pol_writing_agt = agt.agt_code and last_day(to_date(new_pol_sbmt_dt)) = agt.image_date
  left join vn_published_cas_db.tplans pln 
          on trk.new_pol_plan_cd = pln.plan_code
), sales_sum as (
  select  group, campaign_name, cmpgn_id, old_po_num, new_po_num,
          max(assign_lead_ind) as assign_lead_ind,
          max(sbmt_dt) as sbmt_dt,
          max(cvg_eff_dt) as cvg_eff_dt,
          max(assign_agent_ind) as assign_agent_ind,
          max(assign_loc_code_ind) as assign_loc_code_ind,
          max(assign_channel_ind) as assign_channel_ind,
          max(new_base_ind) as new_base_ind,
          count(distinct case when new_base_ind = 1 then new_pol_num end) as new_base_count,
          count(distinct case when new_base_ind = 1 and new_pol_prod_cat = 'UILP' then new_pol_num end) new_ilp_count,
          count(distinct case when new_base_ind = 1 and new_pol_prod_cat = 'ULI' then new_pol_num end) new_ul_count,
          count(distinct case when new_base_ind = 1 and new_pol_prod_cat = 'TROP' then new_pol_num end) new_trop_count,
          count(distinct case when new_base_ind = 1 and new_pol_prod_cat not in ('UILP', 'ULI', 'TROP') then new_pol_num end) new_other_count,
          max(new_rider_ind) as new_rider_ind,
          count(case when new_rider_ind = 1 then new_pol_num end) as new_rider_count,
          sum(new_pol_ape)*1000/{ex_rate} as new_ape_total,
          sum(case when new_base_ind = 1 then new_pol_ape else 0 end)*1000/{ex_rate} as new_ape_base,
          sum(case when new_rider_ind = 1 then new_pol_ape else 0 end)*1000/{ex_rate} as new_ape_rider,
          sum(new_pol_nbv)*1000/{ex_rate} as new_nbv_total,
          sum(case when new_base_ind = 1 then new_pol_nbv else 0 end)*1000/{ex_rate} as new_nbv_base,
          sum(case when new_rider_ind = 1 then new_pol_nbv else 0 end)*1000/{ex_rate} as new_nbv_rider,
          max(hcr_ind) as hcr_ind,
          max(mc_ind) as mc_ind
  from    sales_dtl
  group by
          group, campaign_name, cmpgn_id, old_po_num, new_po_num
)
select  ld.*, new_po_num, assign_lead_ind, sbmt_dt, cvg_eff_dt, assign_agent_ind, assign_loc_code_ind, assign_channel_ind, new_base_ind, 
        new_base_count, new_ilp_count, new_ul_count, new_trop_count, new_other_count, new_rider_ind, new_rider_count,        
        new_ape_total, new_ape_base, new_ape_rider, new_nbv_total, new_nbv_base, new_nbv_rider, hcr_ind, mc_ind
from    leads ld 
left join sales_sum sls 
on ld.cmpgn_id = sls.cmpgn_id and ld.old_po_num = sls.old_po_num
'''
cmpgn_df = sql_to_df(sql_string, 1, spark)
#cmpgn_df = cmpgn_df.withColumn('campaign_name', F.when(F.col('cmpgn_id') == 'MIL-PRD-20240901', 'Pre-approved UW').otherwise('AWO'))
cmpgn_df = cmpgn_df.withColumnRenamed('old_po_num', 'po_num') \
        .withColumnRenamed('old_agt_code', 'agt_cd')

# cmpgn_df.groupBy('cmpgn_id') \
#     .agg(
#         F.count('*').alias('row_count'),
#         F.countDistinct('old_po_num').alias('po_count'),
#         F.sum(F.when(F.col('new_po_num').isNotNull(), 1).otherwise(0)).alias('response_count')
#     ).display()

# Merge campaigns' data with Customer and Agent features
master_df = cmpgn_df.join(
        inc_final_df, 
        on=['po_num', 'image_date'],
        how='left'
        ).join(
        tpar_df,
        on=['agt_cd', 'image_date'],
        how='left'
        ).join(
        tcustdm_df,
        on=['po_num', 'image_date'],
        how='left'
        ).drop(
        'cmpgn_id', 'occp_code') \
        .withColumn('respond_ind',
        F.when(F.col('new_po_num').isNotNull(), F.lit('1 Responder')).otherwise('2 Non-responder'))

int_columns = [field.name for field in master_df.schema.fields if isinstance(field.dataType, (IntegerType, LongType, DoubleType))]

# Fillna the identified integer columns
master_df_filled = master_df.fillna(0, subset=int_columns)

sample_df = master_df.limit(2)
sample_df.display()

# COMMAND ----------

master_df.write.mode('overwrite').parquet('/mnt/lab/vn/project/scratch/adhoc/adhoc_cpm_analysis/')

# COMMAND ----------

master_df.toPandas().to_csv(f'/dbfs/mnt/lab/vn/project/scratch/adhoc/adhoc_cpm_analysis.csv', index=False, header=True, encoding='utf-8-sig')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis

# COMMAND ----------

master_df = spark.read.format('parquet').load('/mnt/lab/vn/project/scratch/adhoc/adhoc_cpm_analysis/')

# COMMAND ----------

# List of responds
respond_df = master_df.filter(F.col('respond_ind') == '1 Responder')
respond_count = respond_df.groupBy('campaign_name').agg(
    F.countDistinct('po_num').alias('po_count'),
    F.countDistinct('agt_cd').alias('agt_count'))
respond_count.display()

# List of non-responds
nonrespond_df = master_df.filter(F.col('respond_ind') == '2 Non-responder')
nonrespond_count = nonrespond_df.groupBy('campaign_name').agg(
    F.countDistinct('po_num').alias('po_count'),
    F.countDistinct('agt_cd').alias('agt_count'))
nonrespond_count.display()
# print("No. response:", respond_count, ", no. non-response:", nonrespond_count)
# print("No. active:", agent_active_count, ", no. inactive:", agent_inactive_count)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong><i>Product holding

# COMMAND ----------

summary1 = master_df.groupBy('campaign_name', 'respond_ind', 'rider_typ', 'prod_type', 'inforce_pol_holding', 'ape_holding').agg(
    F.count('po_num').alias('po_count')
)
display(summary1)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong><i>Demographic

# COMMAND ----------

summary2 = master_df.groupBy('campaign_name', 'respond_ind', 'client_tenure_cat', 'industry', 'city', 'mthly_incm_range').agg(
     F.count('po_num').alias('po_count')
)

# summary2 = master_df.groupBy('campaign_name', 'respond_ind', 'industry', 'occupation').agg(
#     F.count('po_num').alias('po_count')
# )
display(summary2)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong><i>Agent

# COMMAND ----------

analysis3 = master_df.withColumn('servicing_cus_cat',
    F.when(F.col('servicing_cus_count') < 5, F.lit('1 Less than 5'))
    .when(F.col('servicing_cus_count') <= 10, F.lit('2 5-10'))
    .when(F.col('servicing_cus_count') <= 20, F.lit('3 11-20'))
    .when(F.col('servicing_cus_count') <= 30, F.lit('4 21-30'))
    .when(F.col('servicing_cus_count') <= 50, F.lit('5 31-50'))
    .when(F.col('servicing_cus_count') <= 70, F.lit('6 51-70'))
    .when(F.col('servicing_cus_count') <= 100, F.lit('7 71-100'))
    .otherwise('8 More than 100')
)
summary3 = analysis3.groupBy('campaign_name', 'respond_ind', 'agt_type', 'tier', 'rank_code', 'agent_tenure_cat', 'servicing_cus_cat').agg(
    F.count('po_num').alias('po_count'),
    F.countDistinct('agt_cd').alias('agt_count')
)
display(summary3)

# COMMAND ----------

display(respond_df.filter(F.col('campaign_name') == 'Pre-approved UW'))
