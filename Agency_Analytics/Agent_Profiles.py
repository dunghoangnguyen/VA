# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Set the number of months going backward
x = 1  # Replace 0 with the desired number of months (0 being the last month-end)
y = 11 # Number of months to be captured (0 means only 1 month)

# Calculate the last month-end
current_date = pd.Timestamp.now()
last_monthend = current_date - pd.DateOffset(days=current_date.day)
last_monthend = last_monthend - pd.DateOffset(months=x)
last_monthend = last_monthend + pd.offsets.MonthEnd(0)
max_date_str = last_monthend.strftime('%Y-%m-%d')

# Calculate the last 12 months month-end
last_y_monthend = last_monthend - pd.DateOffset(months=y)
last_y_monthend = last_y_monthend + pd.offsets.MonthEnd(0)
min_date_str = last_y_monthend.strftime('%Y-%m-%d')


snapshot = '202407' #int(max_date_str[:4] + max_date_str[5:7])
print(f"From: {min_date_str} to: {max_date_str}, snapshot: {snapshot}")

agtn_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TAGTDM_MTHEND/'
agtr_path = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_AMS_SNAPSHOT_DB/TAMS_AGENTS/'
agtb_path = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_AMS_SNAPSHOT_DB/TAMS_AGENTS_HISTORY/'
can_path = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_AMS_SNAPSHOT_DB/TAMS_CANDIDATES/'
fld_path = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TFIELD_VALUES/'
poldm_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/'
polcas_path = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TPOLICYS/'
aftm_path = f'/mnt/lab/vn/project/agency_analytics/agent_features/'

# List of excluded products
excluded_products = ["FDB01","BIC01","BIC02","BIC03","BIC04","PN001"]

# COMMAND ----------

# MAGIC %md
# MAGIC #Analysis on Agency agent profile (joined from Sep'22 - Aug'23)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Key metrics:
# MAGIC <br>
# MAGIC <strong>Demographic:</strong><br>
# MAGIC gender<br>
# MAGIC education<br>
# MAGIC age<br>
# MAGIC marital status<br>
# MAGIC work experience (any type)<br>
# MAGIC work experience (insurance)<br>
# MAGIC <br>
# MAGIC <strong>Performance:</strong><br>
# MAGIC policies & APE sold<br>
# MAGIC 12m retention (Y/N)<br>
# MAGIC moved to UM+ w/i 1st 12m<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ###Load all data tables

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Agent's data tables

# COMMAND ----------

# Load all agents
agt_cols = ["agt_code","can_num","agt_join_dt","rank_cd","mdrt_ind","mdrt_desc","image_date"]
agtr_df = spark.read.format("parquet").load(agtr_path).filter(    
    (F.col("image_date") >= min_date_str) &
    (F.col("image_date") <= max_date_str) &
    (F.col("comp_prvd_num").isin(["01","98"])) &
    (F.col("trmn_dt").isNull()))
agtb_df = spark.read.format("parquet").load(agtb_path).filter(    
    (F.col("image_date") >= min_date_str) &
    (F.col("image_date") <= max_date_str) &
    (F.col("comp_prvd_num").isin(["01","98"])) &
    (F.col("trmn_dt").isNull()))
agt_df = agtr_df.select(*agt_cols).union(agtb_df.select(*agt_cols))

can_df = spark.read.format("parquet").load(can_path).filter(
    (F.col("image_date") >= min_date_str) &
    (F.col("image_date") <= max_date_str)
)

# Merge the raw tams_agents with tams_candidates
agtf_df = agt_df.join(can_df, on=["can_num", "image_date"])

# Add additional demographic fields
agtf_df = agtf_df.withColumn("gender", 
                                  F.when(F.col("sex_code").isin("M","m"), F.lit("male"))
                                  .when(F.col("sex_code").isin("F","f"), F.lit("female"))
                                  .when(F.col("sex_code").isNull(), F.lit("null"))
                                  .otherwise("other")) \
                .withColumn("education", 
                                  F.when(F.col("edu_lvl") == "ABO", F.lit("Postgraduate"))
                                  .when(F.col("edu_lvl") == "UNI", F.lit("University"))
                                  .when(F.col("edu_lvl") == "HIG", F.lit("Highschool"))
                                  .when(F.col("edu_lvl").isNull(), F.lit("null"))
                                  .otherwise("other")) \
                .withColumn("cur_age", F.floor(F.months_between(F.lit(last_monthend), F.col("can_dob"))/12)) \
                .withColumn("age", 
                            F.when(F.col("cur_age") < 25, F.lit("<25"))
                            .when((F.col("cur_age") >= 25) & (F.col("cur_age") < 30), F.lit("[25,30)"))
                            .when((F.col("cur_age") >= 30) & (F.col("cur_age") < 35), F.lit("[30,35)"))
                            .when((F.col("cur_age") >= 35) & (F.col("cur_age") < 40), F.lit("[35,40)"))
                            .when((F.col("cur_age") >= 40) & (F.col("cur_age") < 45), F.lit("[40,45)"))
                            .when((F.col("cur_age") >= 45) & (F.col("cur_age") < 50), F.lit("[45,50)"))
                            .when((F.col("cur_age") >= 50) & (F.col("cur_age") < 55), F.lit("[50,55)"))
                            .otherwise(">=55")) \
                .withColumn("marital_status", 
                               F.when(F.col("mar_stat_cd") == "S", F.lit("single"))
                               .when(F.col("mar_stat_cd") == "M", F.lit("married"))
                               .when(F.col("mar_stat_cd").isNull(), F.lit("null"))
                               .otherwise("others")) \
                .withColumn("year_exp", F.floor(F.months_between(F.lit(last_monthend), F.col("agt_join_dt"))/12)) \
                .withColumn("work_exp", 
                            F.when(F.col("year_exp") < 1, F.lit("0"))
                            .when((F.col("year_exp") >= 1) & (F.col("year_exp") < 3), F.lit("[0, 3)"))
                            .when((F.col("year_exp") >= 3) & (F.col("year_exp") < 5), F.lit("[3, 5)"))
                            .when((F.col("year_exp") >= 5) & (F.col("year_exp") < 10), F.lit("[5, 10)"))
                            .when((F.col("year_exp") >= 10) & (F.col("year_exp") < 15), F.lit("[10, 15)"))
                            .when((F.col("year_exp") >= 15) & (F.col("year_exp") < 20), F.lit("[15, 20)"))
                            .otherwise(">=20")) \
                .withColumn("rank",
                            F.when(F.col("rank_cd") == F.lit("FA"), 1)
                            .when(F.col("rank_cd") == F.lit("UM"), 2)
                            .when(F.col("rank_cd") == F.lit("SUM"), 3)
                            .when(F.col("rank_cd") == F.lit("DM"), 4)
                            .when(F.col("rank_cd") == F.lit("SDM"), 5)
                            .when(F.col("rank_cd") == F.lit("BM"), 6)
                            .when(F.col("rank_cd") == F.lit("AM"), 7)
                            .when(F.col("rank_cd") == F.lit("PSM"), 8)
                            .when(F.col("rank_cd") == F.lit("SM"), 9)
                            .when(F.col("rank_cd") == F.lit("PSSM"), 10)
                            .when(F.col("rank_cd") == F.lit("SSM"), 11)
                            .when(F.col("rank_cd") == F.lit("DRD"), 12)
                            .when(F.col("rank_cd") == F.lit("RD"), 13)
                            .when(F.col("rank_cd") == F.lit("SRD"), 14)
                            .when(F.col("rank_cd") == F.lit("AVP"), 15)
                            .when(F.col("rank_cd") == F.lit("SAVP"), 16)
                            .when(F.col("rank_cd") == F.lit("VP"), 17)
                            .when(F.col("rank_cd") == F.lit("SVP"), 18)
                            .otherwise(0)
                            )

# Calculate the 12-month period from the agent's join date
agtf_df = agtf_df.withColumn("join_plus_12m", F.add_months(F.col("agt_join_dt"), 12))

# Filter to find agents whose rank moved from 1 to 2 or above within 12 months
rank_change_df = agtf_df.filter(
    (F.col("rank") >= 2) &
    (F.col("image_date") <= F.col("join_plus_12m"))
).select("agt_code").distinct()

# Add the new column 'fa_to_um_12m' to the original dataframe
agtf_df = agtf_df.join(rank_change_df.withColumn("fa_to_um_12m", F.lit("Y")), on="agt_code", how="left")

# Fill null values with 'N' for agents who did not meet the criteria
agtf_df = agtf_df.withColumn("fa_to_um_12m", F.when(F.col("fa_to_um_12m").isNull(), "N").otherwise(F.col("fa_to_um_12m")))

# Drop the temporary column
agtf_df = agtf_df.drop("join_plus_12m")

# Convert all column headers to lower-case
agtf_df = agtf_df.select(*[col.lower() for col in agtf_df.columns])

# Rename 'agt_code' to 'agt_cd'
agtf_df = agtf_df.withColumnRenamed("agt_code", "agt_cd")
del agtr_df
del agtb_df
del agt_df
del can_df
del rank_change_df
agtf_df.createOrReplaceTempView("tams_agents_candidates")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  agt_code, to_date(agt_join_dt) agt_join_dt, rank_cd, image_date
# MAGIC from    vn_published_casm_ams_snapshot_db.tams_agents
# MAGIC where   image_date between '2022-09-30' and '2024-07-31'
# MAGIC   and   agt_join_dt between '2022-09-01' and '2023-08-31'
# MAGIC   and   last_day(agt_join_dt) <= image_date
# MAGIC   and   agt_code='KT8YU'
# MAGIC order by image_date desc
# MAGIC
# MAGIC select    --can.OFFER_RANK_CD, agt.RANK_CD, count(agt.AGT_CODE) no_agents
# MAGIC           agt_code, gender, education, age, marital_status, work_exp, to_date(AGT_JOIN_DT) AGT_JOIN_DT,  RANK_CD latest_rank, fa_to_um_12m,
# MAGIC           floor(months_between(image_date, AGT_JOIN_DT)/12) YEARS_OF_EXP
# MAGIC from      tams_agents_candidates 
# MAGIC where     image_date='2024-07-31'
# MAGIC   and     agt_join_dt between '2022-09-01' and '2023-08-31'

# COMMAND ----------

# Verify metrics
# Gender
gender_count = agtf_df.groupBy("gender").agg(F.count("agt_code").alias("agent_count"))
# Education
edu_count = agtf_df.groupBy("education").agg(F.count("agt_code").alias("agent_count"))
# Age
age_count = agtf_df.groupBy("age").agg(F.count("agt_code").alias("agent_count"))
# Martital status
mar_count = agtf_df.groupBy("marital_status").agg(F.count("agt_code").alias("agent_count"))
# Work experience
wrk_count = agtf_df.groupBy("work_exp").agg(F.count("agt_code").alias("agent_count"))
display(gender_count)
display(edu_count)
display(age_count)
display(mar_count)
display(wrk_count)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Policy's data

# COMMAND ----------

agt_ftm = spark.read.parquet(aftm_path)
agt_ftm = agt_ftm.filter(F.col("snapshot") == snapshot)
agtf_df = agtf_df.filter(F.col("image_date") == max_date_str)
print("agt_ftm: ", agt_ftm.count(), ", agtf_df: ", agtf_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge all metrics required

# COMMAND ----------

final_cols = ["agt_cd", "agt_join_dt", "gender", "education", "age", "marital_status", "work_exp", "fa_to_um_12m",
              "1st12m_pol", "1st12m_ape", "12m_per"
              ]
final = agtf_df.join(agt_ftm, on="agt_cd", how="inner").select(*final_cols)

# Keep only agents who joined from Sep'22 - Aug'23
final = final.filter((F.col("agt_join_dt") >= F.lit("2022-09-01")) &
                     (F.col("agt_join_dt") <= F.lit("2023-08-31"))
                     )
final = final.drop("agt_join_dt")
final.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis starts here

# COMMAND ----------

# Calculate the percentiles and assign groups
windowSpec = Window.orderBy(F.col("1st12m_ape").desc())
final = final.withColumn("percentile", F.ntile(100).over(windowSpec))

final = final.withColumn("group",
                   F.when(F.col("percentile") <= 20, "Top_agents")
                   .when((F.col("percentile") > 20) & (F.col("percentile") <= 50), "Mid_agents")
                   .otherwise("Low_agents"))

# Group by the specified columns and aggregate
result = final.groupBy("group", "gender", "education", "age", "marital_status", "work_exp") \
           .agg(F.countDistinct("agt_cd").alias("unique_agents"),
                F.sum("1st12m_pol").alias("total_1st12m_pol"),
                (F.sum("1st12m_ape")/25).alias("total_1st12m_ape"),
                F.sum(F.when(F.col("12m_per") == "Y", 1).otherwise(0)).alias("count_12m_per_Y"),
                F.sum(F.when(F.col("fa_to_um_12m") == "Y", 1).otherwise(0)).alias("count_fa_to_um_12m_Y"))
           
# Display the result
display(result)
