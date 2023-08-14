# Databricks notebook source
# MAGIC %md
# MAGIC ## Last modified: 2023.08.05
# MAGIC by San
# MAGIC
# MAGIC <font color='green'>(one run would take appx. 20 min)</font>
# MAGIC
# MAGIC -step 1: change the current_mth to the first day of the month you want to run the report(the cohort would be those who did thier first purchase 6 months before)  
# MAGIC -step 2: run everything
# MAGIC
# MAGIC <b>** if you want to run for more month after one run, you can skip the step <font color='blue'>0.Read tables</font>, rerun the parameter cell and start from <font color='blue'>1.Pre analysis</font>.</b>

# COMMAND ----------

#%run /Curation/Utilities/01_Initialize

# COMMAND ----------

import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import json
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import *

import datetime
import time


# COMMAND ----------

# MAGIC %md
# MAGIC # Set Parameters

# COMMAND ----------

# current month (all the behaviors before the 1st of current month would be considered)
current_mth = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-01')
#current_mth = '2023-08-01'
# the snap shot would be taken on the last day of the last month
snapshot_dt = str(pd.to_datetime(current_mth) -relativedelta(days=1))[:10]
# repurchase observing window (repurchase in n month after onboarding, 6 in default)
repurchase_obsv_window = 6
# persistency observing window (15 month in default)
persistency_obsv_window = 15
# only consider the repurchase of those who activated move n month after first policy (6 in defaul)
activate_obsv_window = 6
# taget customer would be those who onboarded this month
onboarding_mth = pd.to_datetime(current_mth)-relativedelta(months=repurchase_obsv_window+1)
# digital product plan code to exclude them in persistency calculation
digital_list = ['BIC01','BIC02','BIC03','BIC04','FDB01','MCI1B','PN001']
# when set to "Yes" will count digital products into 15 mth persistency, when set to 'No' will exclude digital products in persistency
cnt_digital = 'No'

# Re-check all parameters
print(f"current_mth: {current_mth}")
print(f"snapshot_dt: {snapshot_dt}")
print(f"onboarding_mth: {onboarding_mth}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Read tables

# COMMAND ----------

# DBTITLE 1,Temporarily run the query in HIVE and upload the result into Azure's container blob
#######0.1 Read Moveid manulifemember Table#######
##manulifemember is a customer who is related to an eligible Manulife policy for MOVE
#query ='''
# SELECT userid AS userid, MAX(keyid) AS keyid
# FROM  vn_published_move5_mongo_db.manulifemember   
# GROUP BY userid'''

moveid_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MANULIFEMEMBER_FLAT/'
moveid = spark.read.format("parquet").load(moveid_path)
# Repartition the dataframe to improve performance
df_moveid = moveid.select(
    'userid', 
    'keyid'
    )\
    .repartition('userid')
df_moveid = df_moveid.groupBy('userid').agg(max('keyid').alias('keyid'))
print("Number of records and columns in df_moveid:", df_moveid.count(),",", len(df_moveid.columns))
#df_moveid.display(10)

# COMMAND ----------

#######0.2 Read Movekey Table#######
#query ='''SELECT `_id` AS keyid, value AS key_value
#, TO_DATE(SUBSTR(activationdate, 1, 10)) AS activation_date
#, TO_DATE(SUBSTR(CREATED, 1, 10)) AS created_date
#FROM vn_published_move5_mongo_db.movekey'''
#df_movekey = pd.read_sql(query,conn)
#df_movekey['po_num'] = df_movekey.key_value.apply(lambda x:str(x)[1:])
movekey_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MOVEKEY_FLAT/'
movekey = spark.read.format("parquet").load(movekey_path)
# Repartition the dataframe to improve performance
from pyspark.sql.functions import col, date_format, substring

df_movekey = movekey.select(
    col('_id').alias('keyid'), 
    col('value').alias('key_value'),
    date_format('activationdate', 'yyyy-MM-dd').alias('activation_date'),
    date_format('CREATED', 'yyyy-MM-dd').alias('created_date')
).repartition('_id')

df_movekey = df_movekey.withColumn('po_num', expr("substring(key_value, 2)"))

# Set cap to MOVE creation date
df_movekey = df_movekey.filter(col('created_date') <= snapshot_dt)

df_move = df_moveid.join(df_movekey, on='keyid', how='inner').dropDuplicates()
print("Number of records and columns in df_move: ", df_move.count(),",", len(df_move.columns))
#df_move.display(10)

# COMMAND ----------

#######0.4 Read policy table in datamart#######
#query = f"""
#select
#    po_num
#    ,pol_num
#    ,tot_ape_usd
#    ,SBMT_DT as pol_sbmt_dt
#    ,pol_trmn_dt
#    ,pmt_mode
#    ,wa_code
#    ,sa_code
#    from vn_published_datamart_db.tpolidm_mthend 
#    where image_date = "{snapshot_dt}"
#          and pol_stat_cd not in ('A', 'L', 'X', 'R', 'N')
#"""
#df_policy_raw = pd.read_sql(query,conn)
policy_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/'
policy = spark.read.format("parquet").load(policy_path)
df_policies = policy.filter(
    ~col('pol_stat_cd').isin(['A','L','X','X','R','N']) &
    (col('image_date') == snapshot_dt)
    ).select(
        'po_num',
        'pol_num',
        #'plan_code',
        col('tot_ape_usd').cast("float"),
        col('sbmt_dt').alias('pol_sbmt_dt'),
        'pol_trmn_dt',
        'pmt_mode',
        'wa_code',
        'sa_code'
    ).repartition('pol_num').dropDuplicates()

print("Number of records and columns in df_policies: ", df_policies.count(),",", len(df_policies.columns))
# df_policies.display(10)

# COMMAND ----------

#query = f"""
#select pol_num
#        ,cvg_typ
#        ,vers_num
#        ,cvg_eff_dt
#        ,PLAN_CODE
#        ,CUSTOMER_NEEDS
#        ,CVG_PREM_USD
#        ,CVG_STAT_CD
#        ,CVG_STAT_DESC
#        ,concat(plan_code,"-", vers_num) as plan_key 
#from
#    vn_published_datamart_db.tporidm_mthend 
#where 
#     image_date = "{snapshot_dt}" and cvg_eff_dt >= '2021-01-01' 
#     and cvg_stat_cd not in ('A', 'L', 'X', 'R', 'N')
#"""
#df_coverage = pd.read_sql(query,conn)
coverage_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPORIDM_MTHEND/'
coverage = spark.read.format("parquet").load(coverage_path)
df_coverage = coverage.filter(
    ~col('cvg_stat_cd').isin(['A','L','X','X','R','N']) &
    (col('image_date') == snapshot_dt) &
    (col('cvg_eff_dt') >= '2021-01-01')
    ).select(
        'pol_num',
        'cvg_typ',
        'vers_num',
        'cvg_eff_dt',
        'plan_code',
        'customer_needs',
        col('cvg_prem_usd').cast("float"),
        'cvg_stat_cd',
        'cvg_stat_desc',
        concat_ws('-',col('plan_code'),col('vers_num')).alias('plan_key')
    ).repartition('pol_num', 'cvg_eff_dt')

print("Number of records and columns in df_coverage: ", df_coverage.count(),",", len(df_coverage.columns))
# df_coverage.display(10)

# COMMAND ----------

#query = """
#select pol_num
#        ,cvg_eff_dt
#        ,cvg_iss_dt
#        ,concat(plan_code,"-", vers_num) as plan_key 
#from
#    vn_published_cas_db.tcoverages
#where 
#     cvg_iss_dt >= '2021-01-01' 
#     and cvg_stat_cd not in ('A', 'L', 'X', 'R', 'N')
#"""
#df_coverage_iss = pd.read_sql(query,conn)
coverage_iss_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCOVERAGES/'
coverage_iss = spark.read.format("parquet").load(coverage_iss_path)
df_coverage_iss = coverage_iss.filter(
    ~col('cvg_stat_cd').isin(['A','L','X','X','R','N']) &
    (col('cvg_eff_dt') >= '2021-01-01')
    ).select(
        'pol_num',
        'cvg_eff_dt',
        'cvg_iss_dt',
        concat_ws('-',col('plan_code'),col('vers_num')).alias('plan_key')
    ).repartition('pol_num', 'cvg_eff_dt')

print("Number of records and columns in df_coverage_iss: ", df_coverage_iss.count(),",", len(df_coverage_iss.columns))
# df_coverage_iss.display(10)

# COMMAND ----------

#df_plan = pd.read_sql("""select 
#                            sngl_prem_ind
#                            ,concat(plan_code,"-", vers_num) as plan_key 
#                         from vn_published_cas_db.tplans
#                      """, conn)
plan_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPLANS/'
plan = spark.read.format("parquet").load(plan_path)
df_plan = plan.select(
    'sngl_prem_ind',
    concat_ws('-', col('plan_code'), col('vers_num')).alias('plan_key')
)

print("Number of records and columns in df_plan: ", df_plan.count(),",", len(df_plan.columns))

# COMMAND ----------

#query = """
#select
#agt_code as wa_code,
#chnl_cd,
#case when chnl_cd='' then ''
#         when chnl_cd='01' then 'Agency'
#         when chnl_cd='03' then 'Bancassurance'
#         when chnl_cd='09' then 'Micro Insurance'
#         when chnl_cd='34' then 'DMTM'
#         when chnl_cd='48' then 'Affinity'
#         when chnl_cd='08' then 'MGA'
#         when chnl_cd='04' then 'Collector'
#         end as channel
#from vn_published_ams_db.tams_agents"""
#df_agt_channel = pd.read_sql(query,conn)
agt_channel_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS/'
agt_channel = spark.read.format("parquet").load(agt_channel_path)
df_agt_channel = agt_channel.select(
    col('agt_code').alias('wa_code'),
    'chnl_cd',
    when(col('chnl_cd').isNull(), '').\
    when(col('chnl_cd') == '01', 'Agency').\
    when(col('chnl_cd') == '03', 'Bancassurance').\
    when(col('chnl_cd') == '04', 'Collector').\
    when(col('chnl_cd') == '08', 'MGA').\
    when(col('chnl_cd') == '09', 'Micro Insurance').\
    when(col('chnl_cd').isin(['34', '36']), 'DMTM').\
    when(col('chnl_cd') == '48', 'Affinity').\
    otherwise('Unknown').alias('channel')
)

#print("Number of records and columns in df_agt_channel: ", df_agt_channel.count(), ",", len(df_agt_channel.columns))

# COMMAND ----------

from pyspark.sql.window import Window

df_policies = df_policies.filter(col('po_num').isNotNull())
df_policies = df_policies.filter(col('pol_sbmt_dt').isNotNull())
df_policies = df_policies.withColumn('pol_sbmt_dt', to_date(col('pol_sbmt_dt')))
df_policies = df_policies.withColumn('pol_trmn_dt', to_date(col('pol_trmn_dt')))
df_coverage = df_coverage.withColumn('cvg_eff_dt', to_date(col('cvg_eff_dt')))
df_coverage_iss = df_coverage_iss.withColumn('cvg_eff_dt', to_date(col('cvg_eff_dt')))
df_coverage_iss = df_coverage_iss.withColumn('cvg_iss_dt', to_date(col('cvg_iss_dt')))

df_policy_raw = df_policies.join(df_coverage, on='pol_num', how='inner')
df_policy_raw = df_policy_raw.join(df_plan, on='plan_key', how='left')
df_policy_raw = df_policy_raw.join(df_coverage_iss, on=['pol_num','plan_key','cvg_eff_dt'], how='left')
df_policy_raw = df_policy_raw.join(df_agt_channel, on=['wa_code'], how='left')

df_policy_raw = df_policy_raw.dropDuplicates()

df_policy_raw = df_policy_raw.withColumn('cvg_prem_usd', when(col('cvg_prem_usd').isNull(), 0).otherwise(col('cvg_prem_usd')))
df_policy_raw = df_policy_raw.withColumn(
    'cvg_ape_usd',
    when(
        col('sngl_prem_ind') == 'Y',
        col('cvg_prem_usd') * 1.2 / col('pmt_mode')
    ).otherwise(
        col('cvg_prem_usd') * 12 / col('pmt_mode')
    )
)
df_policy_raw = df_policy_raw.withColumn(
    'cvg_iss_dt',
    when(
        col('cvg_iss_dt').isNull(),
        col('cvg_eff_dt')
    ).otherwise(
        col('cvg_iss_dt')
    )
)

sbmt_window = Window.partitionBy('po_num').orderBy('pol_sbmt_dt')
df_policy_raw = df_policy_raw.withColumn(
    'sbmt_dt_rank',
    rank().over(sbmt_window)
)

issdt_window = Window.partitionBy('po_num').orderBy('cvg_iss_dt')
df_policy_raw = df_policy_raw.withColumn(
    'cvg_issdt_rank',
    rank().over(issdt_window)
)

df_policy_raw = df_policy_raw.select(
    "po_num",
    "pol_num",
    "plan_code",
    "vers_num",
    "plan_key",
    "cvg_typ",
    "customer_needs",
    "tot_ape_usd",
    "pol_sbmt_dt",
    "cvg_iss_dt",
    "cvg_eff_dt",
    "pol_trmn_dt",
    "pmt_mode",
    "wa_code",
    "sa_code",
    "cvg_prem_usd",
    "cvg_ape_usd",
    "cvg_stat_cd",
    "cvg_stat_desc",
    "sngl_prem_ind",
    "chnl_cd",
    "channel",
    "sbmt_dt_rank",
    "cvg_issdt_rank"
).dropDuplicates()

#print("Number of records and columns in df_policy_raw: ", df_policy_raw.count(), ",", len(df_policy_raw.columns))
#df_policy_raw.display(10)

# COMMAND ----------

# find the first policy 
df_policy_onboarding_all = df_policy_raw.filter((col('sbmt_dt_rank') == 1) & (col('cvg_issdt_rank') == 1))
df_policy_onboarding_all = df_policy_onboarding_all.dropDuplicates(subset=['pol_num','plan_key','cvg_iss_dt','cvg_ape_usd'])

df_policy_onboarding_all = df_policy_onboarding_all.withColumnRenamed('pol_sbmt_dt', 'first_pol_sbmt_dt')
df_policy_onboarding_all = df_policy_onboarding_all.withColumnRenamed('cvg_iss_dt', 'first_cvg_iss_dt')
df_policy_onboarding_all = df_policy_onboarding_all.withColumn('year', year(col('first_pol_sbmt_dt')))
df_policy_onboarding_all = df_policy_onboarding_all.withColumn(
    'first_iss_month',
    year(col('first_cvg_iss_dt')) * 100 + month(col('first_cvg_iss_dt'))
)

#print("Number of records and columns in df_policy_onboarding_all: ", df_policy_onboarding_all.count(), ",", len(df_policy_onboarding_all.columns))
#df_policy_onboarding_all.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Pre Analysis

# COMMAND ----------

# keep corhort's policys only
df_policy_onboarding = df_policy_onboarding_all.filter(
    col('first_pol_sbmt_dt') >= add_months(to_date(lit(current_mth)), -(persistency_obsv_window + 1))
)
df_policy = df_policy_raw.join(
    df_policy_onboarding.select('po_num').distinct(),
    on='po_num',
    how='inner'
)

df = df_move.join(df_policy_onboarding, on='po_num', how='left').dropDuplicates()

# control group, DMTM & Agency channel only
df_control_onboarding = df_policy_onboarding.join(
    df.select('po_num').distinct(),
    on='po_num',
    how='left_anti'
).filter(col('channel').isin(['Agency', 'DMTM']))

#print("Number of records and columns in df_control_onboarding: ", df_control_onboarding.count(), ",", len(df_control_onboarding.columns))
#df_control_onboarding.display(10)

# COMMAND ----------

# set n month persistency index
df = df.withColumn(
    'pol_iss_dt_at15m',
    when(
        col('first_cvg_iss_dt').isNull(),
        to_date(lit('1900-01-01'))
    ).otherwise(
        add_months(col('first_cvg_iss_dt'), persistency_obsv_window)
    )
)
df = df.withColumn(
    'indicator_firpol_lapsed_within_15mths',
    when(
        col('pol_trmn_dt') < col('pol_iss_dt_at15m'),
        1
    ).otherwise(0)
)

df_control = df_control_onboarding
df_control = df_control.withColumn(
    'pol_iss_dt_at15m',
    add_months(col('first_cvg_iss_dt'), persistency_obsv_window)
)
df_control = df_control.withColumn(
    'indicator_firpol_lapsed_within_15mths',
    when(
        col('pol_trmn_dt') < col('pol_iss_dt_at15m'),
        1
    ).otherwise(0)
)

# digital product flag
df = df.withColumn(
    'is_digital',
    when(
        col('plan_code').isin(digital_list),
        1
    ).otherwise(0)
)

#print("Number of records and columns in df_control: ", df_control.count(), ",", len(df_control.columns))
#print("Number of records and columns in df: ", df.count(), ",", len(df.columns))
#df.display(10)

# COMMAND ----------

# convert to Pandas for computation
df = df.toPandas()
df_policy = df_policy.toPandas()
df_control = df_control.toPandas()

# find second policy
df_tmp = df.groupby(['po_num']).agg({'indicator_firpol_lapsed_within_15mths':'min'}).reset_index()
lapse_ls = df_tmp[df_tmp['indicator_firpol_lapsed_within_15mths']==1]['po_num'].unique()

df_second = df_policy[~((df_policy['sbmt_dt_rank']==1)&(df_policy['cvg_issdt_rank']==1))&~df_policy['po_num'].isin(lapse_ls)]
df_second = df_second.sort_values(by='cvg_iss_dt',ascending=True).drop_duplicates('po_num',keep = 'first')
df_second = df_second[df_second['po_num'].isin(df['po_num'])]
df_second.rename(columns={'cvg_iss_dt':'cvg_2nd_iss_dt',
                         'customer_needs':'2nd_customer_needs',
                          'cvg_ape_usd': '2nd_cvg_ape_usd'
                         }, inplace = True)
df = df.merge(df_second[['po_num','cvg_2nd_iss_dt', '2nd_customer_needs','2nd_cvg_ape_usd']], how='left', on='po_num')

#control group second policy
df_tmp = df_control.groupby(['po_num']).agg({'indicator_firpol_lapsed_within_15mths':'min'}).reset_index()
control_lapse_ls = df_tmp[df_tmp['indicator_firpol_lapsed_within_15mths']==1]['po_num'].unique()

df_second_control = df_policy[~((df_policy['sbmt_dt_rank']==1)&(df_policy['cvg_issdt_rank']==1))&~df_policy['po_num'].isin(control_lapse_ls)]
df_second_control = df_second_control.sort_values(by='cvg_iss_dt',ascending=True).drop_duplicates('po_num',keep = 'first')
df_second_control = df_second_control[df_second_control['po_num'].isin(df_control['po_num'])]
df_second_control.rename(columns={'cvg_iss_dt':'cvg_2nd_iss_dt',
                         'customer_needs':'2nd_customer_needs',
                         'cvg_ape_usd': '2nd_cvg_ape_usd'}, inplace = True)
df_control = df_control.merge(df_second_control[['po_num','cvg_2nd_iss_dt', '2nd_customer_needs','2nd_cvg_ape_usd']], how='left', on='po_num')

df_control.shape

# COMMAND ----------

# customers who buy 2nd policy n months after first MOVE policy issuance
df_control['mth_diff_2nd_1stiss'] = df_control['cvg_2nd_iss_dt'] - df_control['first_cvg_iss_dt']
df_control['mth_diff_2nd_1stiss_days'] = df_control['mth_diff_2nd_1stiss'].apply(lambda x: str(x).split(" ")[0]).replace('NaT','99999').astype(int)
df_control['mth_diff_2nd_1stiss'] = df_control['mth_diff_2nd_1stiss_days'].apply(lambda x: np.ceil(x/30) if x>=0 else np.floor(x/30))

df['mth_diff_2nd_1stiss'] = df['cvg_2nd_iss_dt'] - df['first_cvg_iss_dt']
df['mth_diff_2nd_1stiss_days'] = df['mth_diff_2nd_1stiss'].apply(lambda x: str(x).split(" ")[0]).replace('NaT','99999').astype(int)
df['mth_diff_2nd_1stiss'] = df['mth_diff_2nd_1stiss_days'].apply(lambda x: np.ceil(x/30) if x>=0 else np.floor(x/30))

# customers who activated n months after 1st MOVE policy issuance
df['activation_date'] = pd.to_datetime(df['activation_date'])
df['first_cvg_iss_dt'] = pd.to_datetime(df['first_cvg_iss_dt'])
df['mth_diff_activate_1stiss'] = df['activation_date'] - df['first_cvg_iss_dt']
df['mth_diff_activate_1stiss_days'] = df['mth_diff_activate_1stiss'].apply(lambda x: str(x).split(" ")[0]).replace('NaT','99999').astype(int)
df['mth_diff_activate_1stiss'] = df['mth_diff_activate_1stiss_days'].apply(lambda x: np.ceil(x/30) if x>=0 else np.floor(x/30))


# COMMAND ----------

# MAGIC %md
# MAGIC # Calculate KPI
# MAGIC ## Xsell

# COMMAND ----------

start_dt = str(onboarding_mth)[:10]
end_dt = str(pd.to_datetime(start_dt) + relativedelta(months=+1))[:10]
print("onboarding month:", start_dt[:7])

df_test_tmp = df[(pd.to_datetime(df.first_cvg_iss_dt) >= pd.to_datetime(start_dt)) &
                 (pd.to_datetime(df.first_cvg_iss_dt) < pd.to_datetime(end_dt))]
df_control_tmp = df_control[(pd.to_datetime(df_control.first_cvg_iss_dt) >= pd.to_datetime(start_dt)) &
                            (pd.to_datetime(df_control.first_cvg_iss_dt) < pd.to_datetime(end_dt))]
df_test_tmp.loc[:,'group'] = 'Move'
df_control_tmp.loc[:,'group'] = 'Control'

print(f"# of onboarding customer in {start_dt[:7]} in move group:", df_test_tmp['po_num'].nunique())
print(f"# of onboarding customer in {start_dt[:7]} in control group:", df_control_tmp['po_num'].nunique())

# COMMAND ----------

# only consider the repurchase of the user who activated move in a certain time period(6 mths)
df_tmp = pd.concat([df_test_tmp,df_control_tmp])

df_tmp = df_tmp[(df_tmp['mth_diff_activate_1stiss']<=activate_obsv_window)|(df_tmp['group']=='Control')]
df_tmp[f'cvg_iss_dt_at{repurchase_obsv_window}m'] = df_tmp['first_cvg_iss_dt'].apply(lambda x: x + relativedelta(months=+repurchase_obsv_window))

df_policy_start = df_tmp[~df_tmp['po_num'].isin(lapse_ls)&~df_tmp['po_num'].isin(control_lapse_ls)]
df_repurchase = df_policy_start[['po_num','first_cvg_iss_dt',f'cvg_iss_dt_at{repurchase_obsv_window}m', 'group', 'cvg_2nd_iss_dt',
                                 'mth_diff_activate_1stiss','mth_diff_2nd_1stiss']] \
                                .merge(df_policy, how='left', on='po_num').drop_duplicates()

df_policy_end = df_repurchase[(df_repurchase['cvg_iss_dt']<=df_repurchase[f'cvg_iss_dt_at{repurchase_obsv_window}m'])&
                                 (df_repurchase['cvg_iss_dt']>=df_repurchase['first_cvg_iss_dt'])]
df_policy_end = df_policy_end.sort_values(by='mth_diff_2nd_1stiss') \
                .drop_duplicates(subset=['po_num', 'pol_num', 'plan_key', 'cvg_iss_dt','cvg_ape_usd','group'],keep='first')

df_before = df_policy_start.groupby('po_num').agg({'cvg_ape_usd':'sum',
                                                      'pol_num': 'nunique',
                                                      'group': 'first'}).reset_index()
df_after = df_policy_end.groupby('po_num').agg({'cvg_ape_usd':'sum',
                                                   'pol_num': 'nunique',
                                                   'group': 'first'}).reset_index()

df_cmpr = df_before.merge(df_after, how='left', on=['po_num', 'group'])
df_cmpr['increased_ape'] = df_cmpr['cvg_ape_usd_y'] - df_cmpr['cvg_ape_usd_x']
df_cmpr['have_increased_ape'] = df_cmpr['increased_ape'].apply(lambda x: 1 if x>0 else 0)

df_tmp = df_cmpr.groupby(['group']).agg({'po_num': 'nunique',
                                'have_increased_ape': ['sum','mean'],
                                })

df_tmp_repurchased = df_cmpr[df_cmpr['have_increased_ape']==1].groupby(['group']).agg({'pol_num_y':'mean',
                                                                                       'cvg_ape_usd_x':'mean',
                                                                                    'increased_ape':'mean',
                                                                                    })
# df_tmp_repurchased['incrimental ape %'] = df_tmp_repurchased['increased_ape']/df_tmp_repurchased['cvg_ape_usd_x']

# df_tmp = df_tmp.merge(df_tmp_repurchased, how='left', on='group')

# df_tmp.columns = ["# of Cus", "# of Cus Repurchased in 6 Mth", "% of Repurchased in 6 Mth",
#                   "avg # of Pol hold of Repurchased Cus", "Start APE", "Increased APE in 6 month",
#                   "Incremental APE % (increased/start)"]
# df_tmp = df_tmp.T
# df_tmp['Move Incremental'] = df_tmp['Move'] - df_tmp['Control']
# df_tmp.loc['% of Repurchased in 6 Mth'] = df_tmp.loc['% of Repurchased in 6 Mth'].apply(lambda x: '{:.2f}%'.format(x*100))
# df_tmp.loc['Incremental APE % (increased/start)'] = df_tmp.loc['Incremental APE % (increased/start)'].apply(lambda x: '{:.2f}%'.format(x*100))
# df_tmp.loc['% of Repurchased in 6 Mth','Move Incremental'] = df_tmp.loc['% of Repurchased in 6 Mth','Move Incremental'].strip("%")+'ppt'
# df_tmp.loc['Incremental APE % (increased/start)','Move Incremental'] = df_tmp.loc['Incremental APE % (increased/start)','Move Incremental'].strip("%")+'ppt'
# df_tmp.loc['avg # of Pol hold of Repurchased Cus'] = df_tmp.loc['avg # of Pol hold of Repurchased Cus'].apply(lambda x: '{:.2f}'.format(x))
# df_tmp.loc['Start APE'] = df_tmp.loc['Start APE'].apply(lambda x: '{:.0f}'.format(x))
# df_tmp.loc['Increased APE in 6 month'] = df_tmp.loc['Increased APE in 6 month'].apply(lambda x: '{:.0f}'.format(x))
# df_tmp.loc['# of Cus','Move Incremental'] = '-'
# df_tmp.loc['# of Cus Repurchased in 6 Mth','Move Incremental'] = '-'

# df_tmp.to_csv(f'VN_MOVE_Xsell_{current_mth[:7]}.csv',index=False)
df_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental APE
# MAGIC <strong>Formula: # MOVE group customer * incremental ppt * MOVE group ticket size</strong>

# COMMAND ----------

#new incremental APE definition (# MOVE group customer * incremental ppt * MOVE group ticket size)

df_tmp = df_tmp.merge(df_tmp_repurchased, how='left', on='group')

df_tmp.columns = ["# of Cus", "# of Cus Repurchased in 6 Mth", "% of Repurchased in 6 Mth",
                  "avg # of Pol hold of Repurchased Cus", "Start APE", "Avg Increased APE in 6 month"]
df_tmp = df_tmp.T
df_tmp['Move Incremental'] = df_tmp['Move'] - df_tmp['Control']
df_tmp.loc['Avg Increased APE in 6 month','Move Incremental'] = df_tmp.loc['# of Cus','Move'] * df_tmp.loc['% of Repurchased in 6 Mth','Move Incremental'] * df_tmp.loc['Avg Increased APE in 6 month','Move']
df_tmp.loc['% of Repurchased in 6 Mth'] = df_tmp.loc['% of Repurchased in 6 Mth'].apply(lambda x: '{:.2f}%'.format(x*100))
df_tmp.loc['% of Repurchased in 6 Mth','Move Incremental'] = df_tmp.loc['% of Repurchased in 6 Mth','Move Incremental'].strip("%")+'ppt'
df_tmp.loc['avg # of Pol hold of Repurchased Cus'] = df_tmp.loc['avg # of Pol hold of Repurchased Cus'].apply(lambda x: '{:.2f}'.format(x))
df_tmp.loc['Start APE'] = df_tmp.loc['Start APE'].apply(lambda x: '{:.0f}'.format(x))
df_tmp.loc['Avg Increased APE in 6 month'] = df_tmp.loc['Avg Increased APE in 6 month'].apply(lambda x: '{:.0f}'.format(x))
df_tmp.loc['# of Cus','Move Incremental'] = '-'
df_tmp.loc['# of Cus Repurchased in 6 Mth','Move Incremental'] = '-'

df_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistency

# COMMAND ----------

start_dt = str(pd.to_datetime(current_mth)-relativedelta(months=persistency_obsv_window+1))[:10]
cohort_duration_mth = 1
end_dt = str(pd.to_datetime(start_dt)+relativedelta(months=+cohort_duration_mth))[:10]
print("onboarding month:", start_dt[:7])

df_test_tmp = df[(pd.to_datetime(df.first_cvg_iss_dt) >= pd.to_datetime(start_dt)) &
                 (pd.to_datetime(df.first_cvg_iss_dt) < pd.to_datetime(end_dt))]
df_control_tmp = df_control[(pd.to_datetime(df_control.first_cvg_iss_dt) >= pd.to_datetime(start_dt)) &
                            (pd.to_datetime(df_control.first_cvg_iss_dt) < pd.to_datetime(end_dt))]
df_test_tmp['group'] = 'Move'
df_control_tmp['group'] = 'Control'

print(f"# of onboarding customer in {start_dt[:7]} in move group:", df_test_tmp['po_num'].nunique())
print(f"# of onboarding customer in {start_dt[:7]} in control group:", df_control_tmp['po_num'].nunique())

# COMMAND ----------

# Persistency - "+15mo Persistency of those activated within 6 months"
df_tmp = pd.concat([df_test_tmp,df_control_tmp])

# count digital products as lapse or not
if cnt_digital == "No":
    df_tmp[df_tmp['is_digital']==1]['indicator_firpol_lapsed_within_15mths'] = 0
else: pass

df_tmp = df_tmp[(df_tmp['mth_diff_activate_1stiss']<=activate_obsv_window)|(df_tmp['group']=='Control')]
df_tmp = df_tmp.groupby(['group','pol_num']).agg({'indicator_firpol_lapsed_within_15mths':'max'}).reset_index()
df_tmp = df_tmp.groupby(['group']).agg({'pol_num':'nunique',
                                       'indicator_firpol_lapsed_within_15mths':['sum','mean']})
df_tmp['persistency'] = 1-df_tmp[('indicator_firpol_lapsed_within_15mths','mean')]
df_tmp.columns = ['# of Cus', '# of Lapsed', 'Lapse Rate', 'Persistency Rate']
df_tmp = df_tmp.T
df_tmp['Move Incremental'] = df_tmp['Move'] - df_tmp['Control']
df_tmp.loc['Lapse Rate'] = df_tmp.loc['Lapse Rate'].apply(lambda x: '{:.2f}%'.format(x*100))
df_tmp.loc['Persistency Rate'] = df_tmp.loc['Persistency Rate'].apply(lambda x: '{:.2f}%'.format(x*100))
df_tmp.loc['Lapse Rate','Move Incremental'] = df_tmp.loc['Lapse Rate','Move Incremental'].strip("%")+'ppt'
df_tmp.loc['Persistency Rate','Move Incremental'] = df_tmp.loc['Persistency Rate','Move Incremental'].strip("%")+'ppt'
df_tmp.loc['# of Cus','Move Incremental'] = '-'
df_tmp.loc['# of Lapsed','Move Incremental'] = '-'

df_spark = spark.createDataFrame(df_tmp)
df_spark.write.mode('overwrite').csv(f'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/digital_kpi/MOVE/VN_MOVE_Persistency_{current_mth[:7]}.csv', header=True)
df_tmp

# COMMAND ----------


