# Databricks notebook source
# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import calendar

cpm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
dm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/'

src_tbl1 = 'vn_plan_code_map/'
src_tbl2 = 'tap_client_policy_links/'
src_tbl3 = 'tpolicys/'
src_tbl4 = 'tcoverages/'
src_tbl5 = 'tpolidm_mthend/'
src_tbl6 = 'tcustdm_mthend/'
src_tbl7 = 'tporidm_mthend/'
src_tbl8 = 'tclaim_details/'

x = 0 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
last_mthend = last_day_of_x_months_ago.strftime('%Y-%m-%d')

abfss_paths = [cas_path,dm_path]
parquet_files = [src_tbl1,src_tbl2,src_tbl3,src_tbl4,src_tbl5,src_tbl6,
                 src_tbl7, src_tbl8]

df_list = {}

df_daily = load_parquet_files(abfss_paths, parquet_files)

df_list.update(df_daily)

for df_name, df_temp in df_list.items():
    try:
        df_list[df_name] = df_list[df_name].filter(col('image_date')==last_mthend)
    except:
        continue

# COMMAND ----------

generate_temp_view(df_list)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Result from POSSTG tables</strong>

# COMMAND ----------

result_df = spark.sql("""
select  distinct 
        to_date(ben.birth_dt) as bnfy_birth_dt
        ,case when ben.age between 0 and 4 then '0-4'
             when ben.age between 5 and 10 then '5-10'
             when ben.age between 11 and 14 then '11-14'
             when ben.age between 15 and 18 then '15-18'
             when ben.age between 19 and 22 then '19-22'
             when ben.age >22 then '>22'
        end as bnfy_age_band
        ,ben.age as bnfy_age
        ,ben.cli_num as bnfy_cli_num
        ,ben.sex_code as bnfy_gender
        ,ben.bnfy_nm
        ,ben.bnfy_id
        ,app.pol_num
from    tap_beneficiary_details ben inner join
        tap_applications app on ben.app_num=app.app_num
where   true 
    and ben.bnfy_code='03'
""")

# COMMAND ----------

result_sum = result_df.groupBy('bnfy_age_band')\
    .agg(
        countDistinct('bnfy_cli_num').alias('no_insrd'),
        countDistinct('pol_num').alias('no_policies')
    )

result_sum.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Result from Datamart</strong>

# COMMAND ----------

dm_result = spark.sql("""
select  pol.pol_num,
        pol.tot_ape,
        pol.base_ape,
        pol.rid_ape,
        po_num,
        cvg.cli_num as insrd_num,
        floor(datediff(po.image_date, po.birth_dt)/365.25) as po_age,
        case when floor(datediff(po.image_date, po.birth_dt)/365.25) < 22 then '01. <22'
             when floor(datediff(po.image_date, po.birth_dt)/365.25) between 22 and 30 then '02. 22-30'
             when floor(datediff(po.image_date, po.birth_dt)/365.25) between 31 and 40 then '03. 31-40'
             when floor(datediff(po.image_date, po.birth_dt)/365.25) between 41 and 50 then '04. 41-50'
             when floor(datediff(po.image_date, po.birth_dt)/365.25) between 51 and 60 then '05. 51-60'
             when floor(datediff(po.image_date, po.birth_dt)/365.25) >60 then '06. >60'
        end as po_age_band,
        floor(datediff(ben.image_date, ben.birth_dt)/365.25) as bnfy_age,
        case when floor(datediff(ben.image_date, ben.birth_dt)/365.25) between 0 and 4 then '01. 0-4'
             when floor(datediff(ben.image_date, ben.birth_dt)/365.25) between 5 and 10 then '02. 5-10'
             when floor(datediff(ben.image_date, ben.birth_dt)/365.25) between 11 and 14 then '03. 11-14'
             when floor(datediff(ben.image_date, ben.birth_dt)/365.25) between 15 and 18 then '04. 15-18'
             when floor(datediff(ben.image_date, ben.birth_dt)/365.25) between 19 and 22 then '05. 19-22'
             when floor(datediff(ben.image_date, ben.birth_dt)/365.25) >22 then '06. >22'
        end as bnfy_age_band
from    tpolidm_mthend pol inner join
        tcoverages cvg on pol.pol_num=cvg.pol_num inner join
        tcustdm_mthend po on pol.po_num=po.cli_num inner join
        tcustdm_mthend ben on cvg.cli_num=ben.cli_num
where   cvg.rel_to_insrd='03'                           -- children
    and pol_stat_cd not in ('6','8','A','N','R','X')    -- exclude non-issued/nottaken policies
""")


# COMMAND ----------

dm_sum = dm_result.groupBy(['po_age_band','bnfy_age_band'])\
    .agg(
        countDistinct('po_num').alias('no_po'),
        countDistinct('insrd_num').alias('no_insrd'),
    )

#dm_sum.display()
