# Databricks notebook source
# MAGIC %md
# MAGIC # APE Distribution Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1. Consolidate datasets used for analysis

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libl, params and paths</strong>

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import *
from functools import reduce
import pandas as pd
from datetime import date, datetime, timedelta

pro_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
dm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/'

output_dest = 'adhoc/'
poli_source = 'TPOLIDM_MTHEND/'

start_mth = date(2021, 8, 1).strftime('%Y-%m-%d')
last_mthend = date(2023, 7, 31).strftime('%Y-%m-%d')
print('Start from:', start_mth)
print('Monthend data:', last_mthend)

exclude_status = ['6','8','A','N','R','X']
exclude_product = ['FDB01','BIC01','BIC02','BIC03','BIC04','PN001']

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

poliDF = spark.read.parquet(f'{dm_path}{poli_source}')


# COMMAND ----------

# MAGIC %md
# MAGIC <Strong>Intermediate tables</strong>

# COMMAND ----------

# Get the latest month data
poliDF = poliDF.filter(col('image_date') == last_mthend)

# Exclude products
poli_dmDF = poliDF.filter(~(col('plan_code').isin(exclude_product)) &
                          (col('tot_ape') > 0))

# Exclude pending/nottaken/cancelled/rejected status from original purchases
org_dmDF = poliDF.filter(~(col('pol_stat_cd').isin(exclude_status)))

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Consolidate datasets </strong>

# COMMAND ----------

poli_dmDF.createOrReplaceTempView('tpolidm')
org_dmDF.createOrReplaceTempView('original_dm')

masterDF = spark.sql(f"""
select  pol.pol_num,
        pol.tot_ape as ape,
        cast(pol.tot_ape/23.145 as decimal(14,2)) as ape_usd,
        substr(to_date(pol.pol_iss_dt),1,7) as pol_iss_mth,
        case when org.po_num is null then 'Y' else 'N' end new_ind
from    tpolidm pol
left join
		(select	po_num, min(to_date(frst_iss_dt)) frst_iss_dt
		 from	original_dm
		 --where	pol_stat_cd not in ('6','8','A','N','R','X')
		 group by po_num
		) org
	 on	pol.po_num=org.po_num AND pol.pol_iss_dt>org.frst_iss_dt
where   to_date(pol.pol_iss_dt) >= '{start_mth}'
    --and pol.tot_ape > 0 -- exclude lapsed policies
    --and pol.plan_code not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
""")

print('Number of records:', masterDF.count())

# COMMAND ----------

# Define the window specification sorted by APE in descending order
windowSpec = Window.orderBy(desc('ape'))

# Calculate the percentile rank for each policy
masterDF = masterDF.withColumn('percentile', percent_rank().over(windowSpec))

# Define the decile boundaries (max 20 deciles, incremental by 5% ea.)
decile_bounds = [x * 0.05 for x in range(20)]

# Create a new column to assign each record to a decile
decile_expr = reduce(
    lambda acc, x: acc + when(col('percentile') >= x, 1).otherwise(0),
    decile_bounds,
    0
)
masterDF = masterDF.withColumn('decile', decile_expr)

# COMMAND ----------

master_sumDF = masterDF.groupBy('decile')\
    .agg(
        count('pol_num').alias('count'),
        min('ape_usd').alias('min_ape'),
        max('ape_usd').alias('max_ape'),
        mean('ape_usd').alias('mean_ape'),
        stddev('ape_usd').alias('sttdev_ape')
    )

master_sumDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Write back result to parquet and reload</st>

# COMMAND ----------

masterDF.write.mode('overwrite').parquet(f'{lab_path}{output_dest}ape_decile_analysis/')

# Reload dataset for faster computation
masterDF = spark.read.parquet(f'{lab_path}{output_dest}ape_decile_analysis/')
masterDF_pd = masterDF.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2. Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Decile break-down by mean APE</strong>

# COMMAND ----------

summaryDF = pd.pivot_table(
    masterDF_pd,
    index='decile',
    columns='pol_iss_mth',
    values='ape_usd',
    aggfunc='mean'
)

summaryDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Total New Policies</strong>

# COMMAND ----------

summaryDF = pd.pivot_table(
    masterDF_pd,
    index='new_ind',
    columns='pol_iss_mth',
    values='pol_num',
    aggfunc='count'
)

summaryDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Total APE</strong>

# COMMAND ----------

summaryDF = pd.pivot_table(
    masterDF_pd,
    index='new_ind',
    columns='pol_iss_mth',
    values='ape_usd',
    aggfunc='sum'
)

summaryDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Avg. APE</strong>

# COMMAND ----------

summaryDF = pd.pivot_table(
    masterDF_pd,
    index='new_ind',
    columns='pol_iss_mth',
    values='ape_usd',
    aggfunc='mean'
)

summaryDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>New customer Distribution</strong>

# COMMAND ----------

# Filter the data to select only rows where "new_ind" is 'Y'
filteredDF = masterDF_pd[masterDF_pd['new_ind'] == 'Y']

# Pivot the filtered data to create a summary table with 20 rows of "decile" and columns being "pol_iss_mth"
summaryDF = pd.pivot_table(
    filteredDF,
    index='decile',
    columns='pol_iss_mth',
    values='pol_num',
    aggfunc='count'
)

# Display the summary table
summaryDF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Existing Customer Distribution</strong>

# COMMAND ----------

# Filter the data to select only rows where "new_ind" is 'Y'
filteredDF = masterDF_pd[masterDF_pd['new_ind'] == 'N']

# Pivot the filtered data to create a summary table with 20 rows of "decile" and columns being "pol_iss_mth"
summaryDF = pd.pivot_table(
    filteredDF,
    index='decile',
    columns='pol_iss_mth',
    values='pol_num',
    aggfunc='count'
)

# Display the summary table
summaryDF.display()

# COMMAND ----------


