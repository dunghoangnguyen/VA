# Databricks notebook source
# MAGIC %md
# MAGIC # Analysis done on Claimnants by Occupation cohorts

# COMMAND ----------

# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Declare and load params

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
from datetime import datetime, timedelta
import calendar
# Get the last month-end from current system date
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declare and load paths and tables

# COMMAND ----------

dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
rpt_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
out_path = '/mnt/lab/vn/project/scratch/adhoc/'

tbl1 = 'TCUSTDM_MTHEND/'
tbl2 = 'TAGTDM_MTHEND/'
tbl3 = 'tpolidm_mthend/'
tbl4 = 'tclaims_conso_all/'
tbl5 = 'vn_plan_code_map/'
tbl6 = 'loc_to_sm_mapping/'

paths = [dm_path, rpt_path, cpm_path]
tables = [tbl1, tbl2, tbl3, tbl4, tbl5, tbl6]

df_list = load_parquet_files(paths, tables)
generate_temp_view(df_list)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Immediate and consolidate all features used for analysis

# COMMAND ----------

# # # # #
# Consider incorporating the following features for analysis:
# a.    Occupation code
# b.	Policy type
# c.	Location/ UM/ Branch
# d.	Agent (location, branch, region, rank, group)
# e.	APE
# f.	Claims amount
# g.	Claims amount / APE ratio
# h.    Onboarding year(s)
# i.    ...
# # # # #

df_tmp = spark.sql(f"""
with tclaims as (
    select distinct
             policy_number pol_num
            --,claim_type clm_code
            ,concat_ws('-',lpad(claim_type,2,'0'),claim_type_desc) clm_code_desc
            ,year(Claim_Approved_Date) clm_yr
            ,count(Claim_ID) no_clm
            ,min(to_date(Claim_Approved_Date)) frst_clm_apprv_dt
            ,sum(Claim_Approved_Amount) clm_apprv_amt
    from    tclaims_conso_all
    where   to_date(Claim_Approved_Date) >= '2019-01-01'    -- adjust to # of observing years
        and Claim_Status='A'                                -- select only Approved claims
    group by
             policy_number
            ,year(Claim_Approved_Date)
            ,concat_ws('-',lpad(claim_type,2,'0'),claim_type_desc)
)           
select   pol.po_num
        ,pol.pol_num
        ,to_date(pol.pol_iss_dt) pol_iss_dt
        ,year(pol.pol_iss_dt) iss_yr
        ,pol.plan_code
        ,pln.nbv_factor_group
        --,pol.plan_code_desc
        ,case when pol.pol_stat_cd in ('1','2','3','5','7','9') then '01-Active'
              else '99-Inactive'
         end po_status
        ,floor(datediff(pol.pol_trmn_dt,pol.pol_iss_dt)/365.25) as yr_lapsed_after
        ,case when pol.pol_trmn_dt is null then '99. Active'
              when floor(datediff(pol.pol_trmn_dt,pol.pol_iss_dt)/365.25) <= 2 then '1. Within 2 years'
              when floor(datediff(pol.pol_trmn_dt,pol.pol_iss_dt)/365.25) > 2 then '2. After 2 years'
         end as lapse_tenure
        ,substr(cus.occp_desc,0,charindex('-',cus.occp_desc)-1) occp_cat
        ,substr(cus.occp_desc,charindex('-',cus.occp_desc)+1,255) occ_sub
        ,cus.city
        ,agt.channel
        ,agt.rank_code
        ,case when agt.rank_code='FA' then '01-Agent'
              when agt.rank_code in ('UM','SUM','DM','SDM','PM','AM','BM') then '02-UM+'
              when agt.rank_code in ('PSM','SM','PSSM','SSM','DRD','RD','SRD','AVP','SAVP','VP') then '03-PSM+'
              else '99-Unknown'
         end rank_type
        ,agt.loc_cd
        ,agt.loc_desc
        ,agt.br_code
        ,loc.rh_name
        ,clm.clm_code_desc
        ,clm.clm_yr
        ,case   when clm_yr-year(pol_iss_dt) < 1 then '1. Winthin 1st year'
                when clm_yr-year(pol_iss_dt) >= 1 and clm_yr-year(pol_iss_dt) < 3 then '2. Within 2-3 years'
                else '3. After 3 years'
         end as clm_gap_yr
        ,clm.no_clm
        ,clm.frst_clm_apprv_dt
        ,cast(pol.tot_ape as float) tot_ape
        ,cast(clm.clm_apprv_amt as float) clm_apprv_amt
from    tpolidm_mthend  pol inner join
        tcustdm_mthend  cus on pol.po_num=cus.cli_num and pol.image_date=cus.image_date inner join
        tagtdm_mthend   agt on pol.sa_code=agt.agt_code and pol.image_date=agt.image_date inner join
        tclaims         clm on pol.pol_num=clm.pol_num left join
        loc_to_sm_mapping loc on agt.loc_cd=loc.loc_cd left join
        vn_plan_code_map pln on pol.plan_code=pln.plan_code
where   pol.image_date='{last_mthend}'
    and cus.occp_desc is not null
    and pol.pol_stat_cd in ('1','2','3','5','7','9','B','E','F','H')            -- all still inforce and/or lapsed/surrendered
    and to_date(pol.pol_iss_dt)>='2018-01-01'                                   -- issued after 2017
    and pol.plan_code not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')  -- exclude digital products (Discontinued)
order by
        pol.pol_num, clm.clm_yr
""")

# COMMAND ----------

# DBTITLE 1,Verify data
pd_tmp = df_tmp.toPandas()
pd_tmp.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start analysis

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>General analysis on occupation</strong>

# COMMAND ----------

pd_sum = pd_tmp.groupby(['occp_cat', 'occ_sub', 'po_status', 'lapse_tenure']).agg({
    'po_num': 'count',
    'no_clm': 'sum',
    'tot_ape': lambda x: x.sum() / 1000000,
    'clm_apprv_amt': lambda x: x.sum() / 1000000
}).reset_index()
pd_sum['percent'] = (pd_sum['clm_apprv_amt'] / pd_sum['tot_ape']).round(4)
pd_sum

# COMMAND ----------

# Normalizing the data to show percentages
pd_raw = pd_tmp.groupby(['occp_cat', 'occ_sub', 'rh_name', 'rank_type', 'clm_gap_yr']).agg({
    'no_clm': 'sum',
    'tot_ape': lambda x: x.sum() / 1000000,
    'clm_apprv_amt': lambda x: x.sum() / 1000000
    }).reset_index()
pd_raw['percent'] = (pd_raw['clm_apprv_amt'] / pd_raw['tot_ape']).round(4)
pd_raw

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Year-on-year claim, claims per account and lapse/surrender rate</strong>

# COMMAND ----------

pd_raw2 = pd_tmp.groupby(['clm_yr', 'po_status', 'occp_cat', 'occ_sub', 'nbv_factor_group']).agg({
    'po_num': 'nunique',
    'no_clm': 'sum',
    'tot_ape': lambda x: x.sum() / 1000000,
    'clm_apprv_amt': lambda x: x.sum()/ 1000000
    }).reset_index()
pd_raw2['clm_per_cus'] = (pd_raw2['no_clm'] / pd_raw2['po_num']).round(2)
#pd_raw2['percent'] = pd_raw2['clm_apprv_amt'] / pd_raw['tot_ape']
pd_raw2

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Save working data to csv</strong>

# COMMAND ----------

pd_sum.to_csv(f'/dbfs{out_path}claim_analysis_occp_sum.csv', index=False)

# COMMAND ----------

#pd.set_option('display.max_rows', pd_raw.shape[0]+1)
pd_raw.to_csv(f'/dbfs{out_path}claim_analysis_occp_cat.csv', index=False)

# COMMAND ----------

pd_raw2.to_csv(f'/dbfs{out_path}claim_analysis_occp_yoy.csv', index=False)

# COMMAND ----------


