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
# Import necessary modules

x = 0 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    # Get the first day of the month of interest.
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

# Get the last day of the month that's 'x' months ago from the current system date
# to calculate month-end for import date filtering
last_day_of_x_months_ago = current_month - timedelta(days=1)
# Format the date as string in Y-m-d format and store it as last_mthend variable. 
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
tbl2 = 'TAGTDM_MTHEND_backup/'
tbl3 = 'tpolidm_mthend/'
tbl4 = 'tclaims_conso_all/'
tbl5 = 'vn_plan_code_map/'
tbl6 = 'loc_to_sm_mapping_hist/'

paths = [dm_path, rpt_path, cpm_path]
tables = [tbl1, tbl2, tbl3, tbl4, tbl5, tbl6]

df_list = load_parquet_files(paths, tables)
generate_temp_view(df_list)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Immediate and consolidate all features used for analysis
# MAGIC
# MAGIC Consider incorporating the following features for analysis:<br>
# MAGIC a.  Occupation code<br>
# MAGIC b.	Policy type<br>
# MAGIC c.	Location/ UM/ Branch<br>
# MAGIC d.	Agent (location, branch, region, rank, group)<br>
# MAGIC e.	APE<br>
# MAGIC f.	Claims amount<br>
# MAGIC g.	Claims amount / APE ratio<br>
# MAGIC h.  Onboarding year(s)<br>
# MAGIC i.  Lapse<br>
# MAGIC

# COMMAND ----------

df_tmp = spark.sql(f"""
with tclaims as (
    select distinct
             policy_number pol_num
            ,claim_type clm_code
            ,concat_ws('-',lpad(claim_type,2,'0'),claim_type_desc) clm_code_desc
            ,year(Claim_Approved_Date) clm_yr
            ,count(Claim_ID) no_clm
            ,min(to_date(Claim_Approved_Date)) frst_clm_apprv_dt
            ,sum(Claim_Approved_Amount) clm_apprv_amt
    from    tclaims_conso_all
    where   to_date(Claim_Approved_Date) between '2019-01-01' 
                                             and '{last_mthend}'  -- adjust to # of observing years
        and Claim_Status='A'                                      -- select only Approved claims
    group by
             policy_number
            ,claim_type
            ,concat_ws('-',lpad(claim_type,2,'0'),claim_type_desc)
            ,year(Claim_Approved_Date)
)
select   pol.po_num
        ,pol.pol_num
        ,to_date(pol.pol_iss_dt) pol_iss_dt
        ,year(pol.pol_iss_dt) iss_yr
        ,pol.plan_code
        ,pln.nbv_factor_group
        ,case when pol.pol_stat_cd in ('1','2','3','5','7','9') then '01-Active'
              else '99-Inactive'
         end po_status
        ,pol.pol_trmn_dt
        ,floor(datediff(pol.pol_trmn_dt,pol.pol_iss_dt)/365.25) as yr_lapsed_after
        ,case when pol.pol_trmn_dt is null or pol.pol_stat_cd in ('1','2','3','5','7','9') then '99. Active'
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
        ,case when pol.pol_iss_dt > (select max(pol_trmn_dt) from tpolidm_mthend where po_num = pol.po_num) then
                  floor(datediff(pol.pol_trmn_dt,pol.pol_iss_dt)/30.44) else null 
         end as pur_mth_post_lapse
from    tpolidm_mthend  pol inner join
        tcustdm_mthend  cus on pol.po_num=cus.cli_num and pol.image_date=cus.image_date inner join
        tagtdm_mthend_backup   agt on pol.sa_code=agt.agt_code and pol.image_date=agt.image_date inner join
        tclaims         clm on pol.pol_num=clm.pol_num left join
        loc_to_sm_mapping_hist loc on agt.loc_cd=loc.loc_cd and agt.image_date=loc.image_date left join
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

pd_tmp.to_csv(f'/dbfs{out_path}claim_analysis_occp_details_2311.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start analysis

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>General analysis on occupation</strong>

# COMMAND ----------

# Group the data by occupation category, sub-category of occupation, policy status,
# and lapse tenure and calculate count of policies, sum of claims filed, sum of APE, and sum of claim approval amount for each group
pd_sum = pd_tmp.groupby(['occp_cat', 'occ_sub', 'po_status', 'lapse_tenure']).agg({
    'po_num': 'count',
    'no_clm': 'sum',
    'tot_ape': lambda x: x.sum() / 1000000,
    'clm_apprv_amt': lambda x: x.sum() / 1000000
}).reset_index()

# Compute the percentage of claim approval amount with respect to APE and round off to 4 decimal places
pd_sum['percent'] = (pd_sum['clm_apprv_amt'] / pd_sum['tot_ape']).round(4)

# Display the computed result
pd_sum


# COMMAND ----------

# Group the data by occupation category, sub-category of occupation, and other columns: rh_name, rank_type and clm_gap_yr.    
# It calculates the sum of claims filed, sum of APE, and sum of claim approval amount for each group.
pd_raw = pd_tmp.groupby(['occp_cat', 'occ_sub', 'rh_name', 'rank_type', 'clm_gap_yr']).agg({
    'no_clm': 'sum',
    'tot_ape': lambda x: x.sum() / 1000000,
    'clm_apprv_amt': lambda x: x.sum() / 1000000
    }).reset_index()

# Computes the percentage of claim approval amount with respect to APE and round off to 4 decimal places
pd_raw['percent'] = (pd_raw['clm_apprv_amt'] / pd_raw['tot_ape']).round(4)

# Displays the computed result
pd_raw

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Year-over-year, claims per account and lapse/surrender rate</strong>

# COMMAND ----------

# Group the data by claim year, policy status, occupation category, sub-category of occupation, and nbv_factor group.
# Compute the number of unique policies, sum of claims filed, sum of APE, and sum of claim approval amount for each group
pd_raw2 = pd_tmp.groupby(['clm_yr', 'po_status', 'occp_cat', 'occ_sub', 'nbv_factor_group']).agg({
    'po_num': 'nunique',
    'no_clm': 'sum',
    'tot_ape': lambda x: x.sum() / 1000000,
    'clm_apprv_amt': lambda x: x.sum() / 1000000
}).reset_index()

# Compute the ratio of number of claims to number of unique policies, rounded off to 2 decimal places
pd_raw2['clm_per_cus'] = (pd_raw2['no_clm'] / pd_raw2['po_num']).round(2)

# Display the computed result
pd_raw2


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Year-over-year, occupation and region, branches, ...

# COMMAND ----------

# Group the data by claim year, RH name, location description, occupation category, and sub-category of occupation.
# Compute the number of unique policies, sum of claims filed, sum of APE, and sum of claim approval amount for each group.
pd_raw3 = pd_tmp.groupby(['clm_yr', 'rh_name', 'loc_desc', 'occp_cat', 'occ_sub']).agg({
    'po_num': 'nunique',
    'no_clm': 'sum',
    'tot_ape': lambda x: x.sum() / 1000000,
    'clm_apprv_amt': lambda x: x.sum() / 1000000
}).reset_index()

# Compute the ratio of number of claims to number of unique policies, rounded off to 2 decimal places.
pd_raw3['clm_per_cus'] = (pd_raw3['no_clm'] / pd_raw3['po_num']).round(2)

# Displays the computed result.
pd_raw3

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Year-over-year, occupation and region, branches, months comeback after lapse...</strong>

# COMMAND ----------

# Group the data by claim year, months return after lapse, RH name, location description, occupation category, and sub-category of occupation.
# Compute the number of unique policies, sum of claims filed, sum of APE, and sum of claim approval amount for each group.
pd_raw4 = pd_tmp.groupby(['clm_yr', 'pur_mth_post_lapse', 'rh_name', 'loc_desc', 'occp_cat', 'occ_sub']).agg({
    'po_num': 'nunique',
    'no_clm': 'sum',
    'tot_ape': lambda x: x.sum() / 1000000,
    'clm_apprv_amt': lambda x: x.sum() / 1000000
}).reset_index()

# Compute the ratio of number of claims to number of unique policies, rounded off to 2 decimal places.
pd_raw4['clm_per_cus'] = (pd_raw4['no_clm'] / pd_raw4['po_num']).round(2)

# Displays the computed result.
pd_raw4

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

pd_raw3.to_csv(f'/dbfs{out_path}claim_analysis_occp_region.csv', index=False)

# COMMAND ----------

pd_raw4.to_csv(f'/dbfs{out_path}claim_analysis_occp_return.csv', index=False)
