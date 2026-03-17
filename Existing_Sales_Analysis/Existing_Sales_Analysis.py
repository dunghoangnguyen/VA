# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Derive last month-end by going back 1 day from the 1st day of the current month
last_mthend = datetime.now().replace(day=1) - timedelta(days=1)

# Format last_mthend to Date type
last_mthend_date = last_mthend.date()

# Create a string representation of the date
last_mthend_str = last_mthend_date.strftime('%Y-%m-%d')

# Print the results
print("Last month-end as Date type:", last_mthend_date)
print("Last month-end as string:", last_mthend_str)

dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cseg_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'

# COMMAND ----------

tpol_df = spark.read.parquet(
    f"{dm_path}TPOLIDM_MTHEND/"
).filter(
    F.col("image_date") == last_mthend_str

)

tcov_df = spark.read.parquet(
    f"{dm_path}TPORIDM_MTHEND/"
).filter(
    F.col("image_date") == last_mthend_str
)

tagt_df = spark.read.parquet(
    f"{dm_path}TAGTDM_MTHEND/"
).filter(
    F.col("image_date") == last_mthend_str
)
#print(tagt_df.count())

# COMMAND ----------

tpol_df.createOrReplaceTempView("TPOLIDM_MTHEND")
tcov_df.createOrReplaceTempView("TPORIDM_MTHEND")
tagt_df.createOrReplaceTempView("TAGTDM_MTHEND")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Identify Agency customers 

# COMMAND ----------

# DBTITLE 1,a. ALLCUS_ALLSTST
allcus_allsts = spark.sql(f"""
SELECT
    a.po_num,
    a.pol_num,
    a.wa_code,
    a.sa_code,
    a.tot_ape,
    a.pol_iss_dt,
    a.pol_stat_cd,
    a.dist_chnl_cd,
    CASE WHEN b.comp_prvd_num = '01' AND b.channel = 'Agency' THEN 'Y' ELSE 'N' END AS act_agt_ind,
    b.loc_cd AS loc_cd,
    a.image_date
FROM
    TPOLIDM_MTHEND a
INNER JOIN
    TAGTDM_MTHEND b
ON
    a.sa_code = b.agt_code AND a.image_date = b.image_date
WHERE
    1 = 1
AND a.image_date = '{last_mthend_str}'
AND a.pol_stat_cd NOT IN ('8', 'A', 'N', 'R', 'X') 
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  *
# MAGIC from    allcus_allsts
# MAGIC limit   10
