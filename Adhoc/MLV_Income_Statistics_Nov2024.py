# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import libraries
# MAGIC ### Declare all variables and paths

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
from pyspark.sql import Window

floor_dt='2019-01-01'
lst_mthend='2024-11-30'
mthend_sht=lst_mthend[:4]+lst_mthend[5:7]

agt_incm_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_AGENCY/image_date={lst_mthend}'
pd_incm_path  = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_PD/image_date={lst_mthend}'
cust_dm_path  = f'/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TCUSTDM_MTHEND/image_date={lst_mthend}'
cvg_dm_path   = f'/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPORIDM_MTHEND/image_date={lst_mthend}'
pol_dm_path   = f'/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/image_date={lst_mthend}'

output_path   = f'/mnt/lab/vn/project/scratch/adhoc/mlv_income_statistic/'

# Retrieving latest exchange rate (for VND to USD conversion)
exrt_string = f'''
with xrt as (
select  cast(XCHNG_RATE as int) ex_rate,
        row_number() over (partition by XCHNG_RATE_TYP order by FR_EFF_DT DESC) rn
from    vn_published_cas_db.texchange_rates
where   XCHNG_RATE_TYP='U'
    and FR_CRCY_CODE='78'
    and to_date(FR_EFF_DT) <= '{lst_mthend}'
qualify rn=1
) select ex_rate from xrt
'''
exrt_df = sql_to_df(exrt_string, 1, spark)
ex_rate = exrt_df.collect()[0][0]/100

# Define policy selection criteria
hcr2019 = ['RHC6I','RHC7O','RHC6O','RHC7D','RHC6D','RHC7I']
hcr2024 = ['RHC8M','RHC8D','RHC8I','RHC8O']
inf_sts = ['1','2','3','5','7','9']

print(f"Selected date: {lst_mthend}, snapshot: {mthend_sht}, ex. rate: {ex_rate}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load all working tables

# COMMAND ----------

incm_cols = ['po_num', 'min_prem_dur', 'first_pol_eff_dt', 'sex_code', 'client_tenure', 'pol_count', 'ins_typ_count', 'term_pol', 'endow_pol', 'health_indem_pol', 'whole_pol', 'investment_pol', 'inforce_pol', 'f_same_agent', 'f_owner_is_agent', 'agt_tenure_yrs', 'mdrt_flag', 'multi_prod', 'annual_flag', 'valid_email', 'valid_mobile', 'channel_final', 'unassigned_ind', 'no_dpnd', 'f_HCM', 'f_HN', 'f_DN', 'pol_cnt', '10yr_pol_cnt', 'f_vip_elite', 'f_vip_plat', 'f_vip_gold', 'f_vip_silver', 'existing_vip_seg', 'f_1st_term', 'f_1st_endow', 'f_1st_health_indem', 'f_1st_whole', 'f_1st_invest', 'mthly_incm']
cust_cols = ['cli_num', 'mthly_incm']

# Add additional income columns adjusted to inflation rate of 5%
cust_add_cols = {
    'mthly_incm': F.col('mthly_incm')*23.145,
    'mthly_incm_usd': F.col('mthly_incm')*23.145/ex_rate,
    'adj_mthly_incm': (F.col('mthly_incm')*23.145)*(1+F.col('client_tenure')*5/100),
    'adj_mthly_incm_usd': (F.col('mthly_incm_usd')*(1+F.col('client_tenure')*5/100))
}

# Load and merge customer income segmentation
agt_incm_df = spark.read.format('parquet').load(agt_incm_path)
pd_incm_df  = spark.read.format('parquet').load(pd_incm_path)

incm_df = agt_incm_df.unionAll(pd_incm_df).select(*incm_cols).dropDuplicates(['po_num'])
#print("Before:", incm_df.count())
# Keep only PO whose first issued/effective policy from Jan 1st 2019
incm_df = incm_df.filter((F.col('inforce_ind') == 1) &
                         (F.to_date(F.col('first_pol_eff_dt')) >= floor_dt))
#print("After:", incm_df.count())
# Load customer datamart
cust_df     = spark.read.format('parquet').load(cust_dm_path).select(*cust_cols) \
    .withColumnRenamed('cli_num', 'po_num')

# Merge these two and add the income values
#incm_df = incm_df.join(cust_df, 'po_num', 'inner')

master_df = incm_df.withColumns(cust_add_cols)
# Checking the result count
#po_count = master_df.count()
#po_distinct_count = master_df.select('po_num').distinct().count()
#diff_count = po_count - po_distinct_count
#print(f"Total POs: {po_count}, duplicated record: {diff_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Derive HCR2019 and HCR2024 riders up until Nov'24

# COMMAND ----------

# Load coverage datamart
cvg_df = spark.read.format('parquet').load(cvg_dm_path)
pol_df = spark.read.format('parquet').load(pol_dm_path)
cvg_df.createOrReplaceTempView('tporidm_mthend')
pol_df.createOrReplaceTempView('tpolidm_mthend')

hcr2019_str = ','.join(f"'{x}'" for x in hcr2019)
hcr2024_str = ','.join(f"'{x}'" for x in hcr2024)
inf_sts_str = ','.join(f"'{x}'" for x in inf_sts)

hcr_df = spark.sql(f'''
WITH hcr_2019 AS (
    SELECT DISTINCT pol_num, 'Y' AS hcr2019_ind
    FROM tporidm_mthend
    WHERE plan_code IN ({hcr2019_str})
      AND cvg_stat_cd IN ({inf_sts_str})
      AND to_date(cvg_eff_dt) >= '{floor_dt}'
), 
hcr_2024 AS (
    SELECT DISTINCT pol_num, 'Y' AS hcr2024_ind
    FROM tporidm_mthend
    WHERE plan_code IN ({hcr2024_str})
      AND cvg_stat_cd IN ({inf_sts_str})
      AND to_date(cvg_eff_dt) >= '{floor_dt}'
)
SELECT po_num,
       MAX(nvl(h19.hcr2019_ind, 'N')) AS hcr2019_ind,
       MAX(nvl(h24.hcr2024_ind, 'N')) AS hcr2024_ind
FROM    tpolidm_mthend pol
LEFT JOIN hcr_2019 h19 ON pol.pol_num = h19.pol_num
LEFT JOIN hcr_2024 h24 ON pol.pol_num = h24.pol_num
WHERE 1=1
  AND pol.pol_stat_cd IN ({inf_sts_str})
  AND (h19.pol_num IS NOT NULL OR h24.pol_num IS NOT NULL)
GROUP BY po_num
''').dropDuplicates()

#print(hcr_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge HCR data into master dataframe

# COMMAND ----------

master_df = master_df.join(hcr_df, 'po_num', 'left').fillna({'hcr2019_ind': 'N', 'hcr2024_ind': 'N'})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save result and reload into pd for analysis

# COMMAND ----------

master_df.write.mode('overwrite').parquet(f'{output_path}parquet/')

# COMMAND ----------

incm_pd = pd.read_parquet(f'/dbfs{output_path}parquet/').copy()

# COMMAND ----------

# MAGIC %md
# MAGIC # Break income into buckets
# MAGIC <strong><=10m<br>
# MAGIC <strong>10-20m<br>
# MAGIC <strong>20-30m<br>
# MAGIC <strong>30-50m<br>
# MAGIC <strong>50-70m<br>
# MAGIC <strong>70-100m<br>
# MAGIC <strong>100-150m<br>
# MAGIC <strong>150-200m<br>
# MAGIC <strong>>=200m

# COMMAND ----------

# Define bins and labels for each column to be binned
bins_labels = [
    # For 'adj_mthly_incm'
    ('adj_mthly_incm', [1, 10000, 20000, 30000, 50000, 70000, 100000, 150000, 200000, float('inf')],
     ['<=10m', '10-20m', '20-30m', '30-50m', '50-70m', '70-100m', '100-150m', '150-200m', '>=200m', 'NaN']),
    ]

# Apply the function to each feature
for column, bins, labels in bins_labels:
    create_categorical(incm_pd, column, bins, labels)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store final output to CSV

# COMMAND ----------

incm_pd.to_csv(f'/dbfs{output_path}/mlv_incm_statistics_{mthend_sht}.csv', index=False, header=True, encoding='utf-8-sig')
print(incm_pd.shape[0])
incm_pd[incm_pd['adj_mthly_incm'].notnull()].head(2)

# COMMAND ----------

# Find the minimum value of 'first_pol_eff_dt' where 'adj_mthly_incm' is not null
#min_first_pol_eff_dt = incm_pd[incm_pd['adj_mthly_incm'].isnull()]['first_pol_eff_dt'].min()

#print(f"The minimum value of 'first_pol_eff_dt' where 'adj_mthly_incm' is not null is: {min_first_pol_eff_dt}")

# COMMAND ----------

group_a = incm_pd.groupby('adj_mthly_incm_cat')['po_num'].count().reset_index()
group_a.columns = ['adj_mthly_incm_cat', 'po_num_count']
display(group_a)

# COMMAND ----------

# Group by 'adj_mthly_incm_cat' for those with 'hcr2019_ind' as 'Y'
group_b_2019 = incm_pd[incm_pd['hcr2019_ind'] == 'Y'].groupby('adj_mthly_incm_cat')['po_num'].count().reset_index()
group_b_2019.columns = ['adj_mthly_incm_cat', 'po_num_count_2019']

# Group by 'adj_mthly_incm_cat' for those with 'hcr2024_ind' as 'Y'
group_b_2024 = incm_pd[incm_pd['hcr2024_ind'] == 'Y'].groupby('adj_mthly_incm_cat')['po_num'].count().reset_index()
group_b_2024.columns = ['adj_mthly_incm_cat', 'po_num_count_2024']

# Group by 'adj_mthly_incm_cat' for those with both 'hcr2019_ind' and 'hcr2024_ind' as 'Y'
group_b_both = incm_pd[(incm_pd['hcr2019_ind'] == 'Y') & (incm_pd['hcr2024_ind'] == 'Y')].groupby('adj_mthly_incm_cat')['po_num'].count().reset_index()
group_b_both.columns = ['adj_mthly_incm_cat', 'po_num_count_both']

display(group_b_2019)
display(group_b_2024)
display(group_b_both)
