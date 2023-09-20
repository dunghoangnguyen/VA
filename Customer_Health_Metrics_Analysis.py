# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Health Analysis - BMI
# MAGIC ### Purpose is to identify group of high risk to Obesity customers

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load all params, paths and libs</strong>

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import calendar
from functools import reduce
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

pro_Path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/'
cur_Path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/' 
lab_Path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/adhoc/'

posstg_db = 'VN_PUBLISHED_POSSTG_DB/'
cas_db = 'VN_PUBLISHED_CAS_DB/'
datamart_db = 'VN_CURATED_DATAMART_DB/'
reports_db = 'VN_CURATED_REPORTS_DB/'
snapshot_db = 'VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'

# Add conditions for claims diagnosis
conditions = ["diabetes", "obese", "obesity", "adiposity"]

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load and filter tables</strong>

# COMMAND ----------

#tposs_client = spark.read.parquet(f"{pro_Path}{posstg_db}TPOSS_CLIENT/")
tap_client_details = spark.read.parquet(f"{pro_Path}{posstg_db}TAP_CLIENT_DETAILS/")
tap_client_add_info = spark.read.parquet(f"{pro_Path}{posstg_db}TAP_CLIENT_ADD_INFO")
tclaim_details = spark.read.parquet(f"{pro_Path}{snapshot_db}TCLAIM_DETAILS/")
tclaim_diag = spark.read.parquet(f"{pro_Path}{cas_db}TCLM_DIAGNOSIS/")
tporidm = spark.read.parquet(f"{cur_Path}{datamart_db}TPORIDM_MTHEND/")
tcoverages = spark.read.parquet(f"{pro_Path}{snapshot_db}TCOVERAGES/")
tpolidm = spark.read.parquet(f"{cur_Path}{datamart_db}TPOLIDM_MTHEND/")
tagtdm = spark.read.parquet(f"{cur_Path}{datamart_db}TAGTDM_MTHEND/")
tloc = spark.read.parquet(f"{cur_Path}{reports_db}LOC_TO_SM_MAPPING_HIST/")

#tposs_client = tposs_client.toDF(*[col.lower() for col in tposs_client.columns])
tap_client_details = tap_client_details.toDF(*[col.lower() for col in tap_client_details.columns])
tap_client_add_info = tap_client_add_info.toDF(*[col.lower() for col in tap_client_add_info.columns])
tclaim_details = tclaim_details.toDF(*[col.lower() for col in tclaim_details.columns])
tclaim_diag = tclaim_diag.toDF(*[col.lower() for col in tclaim_diag.columns])
tporidm = tporidm.toDF(*[col.lower() for col in tporidm.columns])
tpolidm = tpolidm.toDF(*[col.lower() for col in tpolidm.columns])
tagtdm = tagtdm.toDF(*[col.lower() for col in tagtdm.columns])
tcoverages = tcoverages.toDF(*[col.lower() for col in tcoverages.columns])

tclaim_details = tclaim_details.filter(col("image_date") == last_mthend)
tporidm = tporidm.filter(col("image_date") == last_mthend)
tpolidm = tpolidm.filter(col("image_date") == last_mthend)
tagtdm = tagtdm.filter(col("image_date") == last_mthend)
tcoverages = tcoverages.filter(col("image_date") == last_mthend)
tloc = tloc.filter(col("image_date") == last_mthend)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Itermediate tables

# COMMAND ----------

# MAGIC %md
# MAGIC <strong> Clients' health metrics (height, weight, conditions...)</strong>

# COMMAND ----------

# Select only columns needed
#tposs_df = tposs_client.select(
#    "cli_num",
#    "client_type", 
#    "age"
#    "height_unit", 
#    "height", 
#    "weight_unit", 
#    "weight",
#    to_date(col("process_date")).alias("process_date")
#).where(col("weight").isNotNull()).dropDuplicates()

# Define the window spec for sorting
#tposs_spec = Window.partitionBy("cli_num").orderBy("process_date")

# Add first and last columns
#tposs_df = tposs_df.withColumn("first_height", first("height").over(tposs_spec)) \
#                                     .withColumn("first_weight", first("weight").over(tposs_spec)) \
#                                     .withColumn("last_height", last("height").over(tposs_spec)) \
#                                     .withColumn("last_weight", last("weight").over(tposs_spec)) \
#                                     .withColumn("first_bmi", first("weight").over(tposs_spec) / (first("height").over(tposs_spec)/100)**2) \
#                                     .withColumn("last_bmi", last("weight").over(tposs_spec) / (last("height").over(tposs_spec)/100)**2)

#tposs_final_df = tposs_df.select(
#    "cli_num",
#    "first_height",
#    "first_weight",
#    "first_bmi",
#    "last_height",
#    "last_weight",
#    "last_bmi"
#).dropDuplicates()

# Convert the wrongly capture height/weight
tap_client_add_info = tap_client_add_info.withColumn("new_height", 
                                                when(
                                                    (col("height") < 100) &
                                                    (col("weight") > 100), col("weight")
                                                ) \
                                                .otherwise(
                                                        when(
                                                            (col("weight") < 100) &
                                                            (col("height") < col("weight")), col("height") + 100
                                                            ) \
                                                        .otherwise(col("height"))
                                                )
                                        ) \
                                        .withColumn("new_weight",
                                                when(
                                                    (col("height") < 100) &
                                                    (col("weight") > 100), col("height")
                                                ) \
                                                .otherwise(
                                                    when(
                                                        col("weight") > col("height"), col("height")
                                                    ) \
                                                    .otherwise(col("weight"))
                                                )
                                        )
#tap_client_add_info.display()

tap_client_df = tap_client_details.alias("a").join(tap_client_add_info.select(
    "app_num",
    "cli_num",
    "height_unit",
    "new_height",
    "weight_unit",
    "new_weight"
).alias("b"), on=["app_num","cli_num"], how="inner") \
    .select(
        "a.app_num",
        "a.cli_num",
        "a.sex_code",
        "a.age",
        "b.height_unit",
        col("b.new_height").alias("height"),
        "b.weight_unit",
        col("b.new_weight").alias("weight")
    ) \
.where((col("weight").isNotNull()) & 
       (col("weight") != 0)
       )
tap_client_df = tap_client_df.dropDuplicates(["app_num", "cli_num"])

# Define the window spec for sorting
tap_client_spec = Window.partitionBy("cli_num").orderBy("app_num")

# Add first and last columns
tap_client_df = tap_client_df.withColumn("first_age", first("age").over(tap_client_spec)) \
                                    .withColumn("last_age", last("age").over(tap_client_spec)) \
                                    .withColumn("first_height", first("height").over(tap_client_spec).cast(DecimalType(6, 2))) \
                                    .withColumn("first_weight", first("weight").over(tap_client_spec).cast(DecimalType(6, 2))) \
                                    .withColumn("last_height", last("height").over(tap_client_spec).cast(DecimalType(6, 2))) \
                                    .withColumn("last_weight", last("weight").over(tap_client_spec).cast(DecimalType(6, 2))) \
                                    .withColumn("first_bmi", (first("weight").over(tap_client_spec) / (first("height").over(tap_client_spec)/100)**2).cast(DecimalType(6, 2))) \
                                    .withColumn("last_bmi", (last("weight").over(tap_client_spec) / (last("height").over(tap_client_spec)/100)**2).cast(DecimalType(6, 2)))

tap_client_final_df = tap_client_df.select(
    "cli_num",
    "sex_code",
    "first_age",
    "first_height",
    "first_weight",
    "first_bmi",
    "last_age",
    "last_height",
    "last_weight",
    "last_bmi"
).dropDuplicates(["cli_num"])

print("#'s tap_client_final_df:", tap_client_final_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Customers' claims and status</strong>

# COMMAND ----------

tclaim_df = tclaim_details.filter(
    (col("clm_stat_code").isin(["A","D"])) &
    (col("clm_code").isin(["3","7","8","11","27","28","29"]))).alias("tclm")\
        .join(
            tclaim_diag.alias("tdg"),
            on=col("tclm.clm_diagnosis") == col("tdg.diag_code"),
            how="left"
        )\
        .select(
            "cli_num",
            "clm_id",
            lower(col("diag_nm_eng")).alias("condition")
        )\
        .where(col("clm_recv_dt") >= '2020-01-01')

# Identify obesity
tclaim_df = tclaim_df.withColumn(
    "obesity_ind",
    when(tclaim_df["condition"].isin(conditions), 1).otherwise(0)
)

# Select only obese customers
tclaim_obese_df = tclaim_df.where(
    reduce(lambda a, b: a | b, [lower(col("diag_nm_eng")).contains(c) for c in conditions])
    )

# Get list of customers who've submitted claims since 2020
tclaim_sum_df = tclaim_df.groupBy(col("cli_num"))\
    .agg(
        countDistinct("clm_id").alias("number_of_claims"),
        max("obesity_ind").alias("obesity_ind")
        )
#tclaim_sum_df_filtered = tclaim_df_filtered.groupBy(col("cli_num")).agg(countDistinct("clm_id").alias("number_of_claims"))

# Get list of Active customers whose policies issued since 2020
tcov_sum_df = tcoverages.withColumn("status", when(col("cvg_stat_cd").isin(["1","2","3","5"]), "Active").otherwise("Inactive")) \
                        .groupBy(col("cli_num")) \
                        .agg(min("cvg_eff_dt").alias("first_eff_dt"), 
                             min("status").alias("status")) \
                        .where(col("first_eff_dt") >= "2020-01-01") \
                        .dropDuplicates()

# Get list of Smokers
tsmkr_df = tcoverages.filter(col("smkr_code") == "S") \
                .select("cli_num",
                       "smkr_code") \
                .dropDuplicates()

print("#'s tclaim_sum_df:", tclaim_sum_df.count())
print("#'s obese_ind: ", tclaim_sum_df.filter(col("obesity_ind") != 0).count())
print("#'s tcov_sum_df:", tcov_sum_df.count())
print("#'s tsmkr_df:", tsmkr_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Find latest serving agents for each customer</strong>

# COMMAND ----------

tpolidm.createOrReplaceTempView("tpolidm")
tagtdm.createOrReplaceTempView("tagtdm")
tloc.createOrReplaceTempView("tloc")

tcus_loc = spark.sql("""           
with lst_agt as (
    select  po_num, sa_code as agt_code,
            row_number() over (partition by po_num order by pol_iss_dt desc, pol_num desc) rn
    from    tpolidm
    where   pol_stat_cd in ('1','2','3','5')
)
select  lst_agt.po_num,
        lst_agt.agt_code,
        agt.loc_cd,
        tloc.rh_code,
        tloc.rh_name
from    lst_agt inner join
        tagtdm agt on lst_agt.agt_code = agt.agt_code left join
        tloc on agt.loc_cd = tloc.loc_cd
where   lst_agt.rn = 1             
""")

tcus_loc = tcus_loc.dropDuplicates(["po_num"])
print("#'s tcus_loc:", tcus_loc.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Finalize tables</strong>

# COMMAND ----------

tap_client_final_df.createOrReplaceTempView("tap_client_final")
tclaim_sum_df.createOrReplaceTempView("tclaim_sum")
tcov_sum_df.createOrReplaceTempView("tcov_sum")
tsmkr_df.createOrReplaceTempView("tsmkr")
tcus_loc.createOrReplaceTempView("tcus_loc")

final_df = spark.sql("""
    select  tap.cli_num,
            case when tap.sex_code="F" then "Female"
                 when tap.sex_code="M" then "Male"
            end as gender,
            tap.first_age,
            tap.first_height,
            tap.first_weight,
            tap.first_bmi, 
            case when tap.first_bmi < 18.5 then "Underweight"
                 when tap.first_bmi between 18.5 and 22.9 then "Normal"
                 when tap.first_bmi >= 23 and tap.first_bmi < 25 then "Overweight"
                 when tap.first_bmi >= 25 and tap.first_bmi < 30 then "Obesity Level 1"
                 else "Obesity Level 2"
            end as first_bmi_cat,
            tap.last_age,
            case when tap.last_age < 20 then "01. <20yo"
                 when tap.last_age between 20 and 25 then "02. 20-25yo"
                 when tap.last_age between 26 and 30 then "03. 26-30yo"
                 when tap.last_age between 31 and 35 then "04. 31-35yo"
                 when tap.last_age between 36 and 40 then "05. 36-40yo"
                 when tap.last_age between 41 and 45 then "06. 41-45yo"
                 when tap.last_age between 46 and 50 then "07. 46-50yo"
                 when tap.last_age between 51 and 55 then "08. 51-55yo"
                 when tap.last_age between 56 and 60 then "09. 56-60yo"
                 when tap.last_age > 60 then "10. >60yo"
            end as last_age_cat,
            tap.last_height,
            tap.last_weight,
            tap.last_bmi,
            case when tap.last_bmi < 18.5 then "Under-weight"
                 when tap.last_bmi between 18.5 and 22.9 then "Normal"
                 when tap.last_bmi >= 23 and tap.last_bmi < 25 then "Overweight"
                 when tap.last_bmi >= 25 and tap.last_bmi < 30 then "Obesity Level 1"
                 else "Obesity Level 2"
            end as last_bmi_cat,
            nvl(tclm.number_of_claims, 0) as number_of_claims,
            nvl(tclm.obesity_ind, 0) as obesity_ind,
            tcov.status,
            case when tsmkr.cli_num is not null then 'Smoker' else 'Non-smoker' end as smkr_cat,
            tcus_loc.rh_code,
            tcus_loc.rh_name
    from    tap_client_final tap inner join
            tcov_sum tcov on tap.cli_num=tcov.cli_num left join
            tclaim_sum tclm on tap.cli_num=tclm.cli_num left join
            tsmkr on tap.cli_num=tsmkr.cli_num left join
            tcus_loc on tap.cli_num=tcus_loc.po_num        
""")

print("#'s Universe records:", final_df.count())
print("#'s Active records:", final_df.where(col("status") == "Active").count())

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{lab_Path}CUS_METRICS")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Reload result for analysis</strong>

# COMMAND ----------

import seaborn as sns
import matplotlib.pyplot as plt

cus_metrics = spark.read.parquet(f"{lab_Path}CUS_METRICS").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Apply filteres/exclusion rules</strong>

# COMMAND ----------

# Add exclusion rules here
cus_metrics_filtered = cus_metrics[(cus_metrics["last_age_cat"] != "01. <20yo") & 
                                   (cus_metrics["status"] == "Active") &
                                   (~cus_metrics["last_bmi_cat"].isin(["Under-weight", "Normal"]))]

display(cus_metrics_filtered)
# Plotting the distribution of (status, last_bmi_cat)
#plt.figure(figsize=(12,6))
#sns.countplot(x='status', hue='last_bmi_cat', data=cus_metrics_filtered)
#plt.title('Distribution of Customer status and BMI (excl. age < 20)')
#plt.show()

# Plotting the distribution of (gender, last_bmi_cat)
#plt.figure(figsize=(12,6))
#sns.countplot(x='gender', hue='last_bmi_cat', data=cus_metrics_filtered)
#plt.title('Distribution of Gender and BMI (excl. age < 20)')
#plt.show()

# Plotting the distribution of (last_age_cat, last_bmi_cat)
#plt.figure(figsize=(12,6))
#sns.countplot(x='last_age_cat', hue='last_bmi_cat', data=cus_metrics_filtered)
#plt.title('Distribution of Age and BMI (excl. age < 20)')
#plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Deepdive analysis</strong>

# COMMAND ----------

# Normalizing the data to show percentages
all_bmi_filtered = cus_metrics_filtered.groupby(['rh_name', 'last_age_cat', 'gender', 'smkr_cat', 'last_bmi_cat']).size().reset_index(name='counts')
all_bmi_filtered['percent'] = all_bmi_filtered['counts'] / all_bmi_filtered['counts'].sum()
## Calculate the % of customers submitting obesity related claims
#all_bmi_filtered['obese_clm_percent'] = all_bmi_filtered[all_bmi_filtered['obesity_ind'] == 1]['counts'] / all_bmi_filtered['counts'].sum()

gender_bmi_unfiltered = cus_metrics.groupby(['status', 'gender', 'rh_name', 'last_bmi_cat']).size().reset_index(name='counts')
gender_bmi_unfiltered['percent'] = gender_bmi_unfiltered['counts'] / gender_bmi_unfiltered['counts'].sum()

gender_bmi_filtered = cus_metrics_filtered.groupby(['status', 'gender', 'rh_name', 'last_bmi_cat']).size().reset_index(name='counts')
gender_bmi_filtered['percent'] = gender_bmi_filtered['counts'] / gender_bmi_filtered['counts'].sum()

age_bmi_unfiltered = cus_metrics.groupby(['status', 'last_age_cat', 'rh_name', 'last_bmi_cat']).size().reset_index(name='counts')
age_bmi_unfiltered['percent'] = age_bmi_unfiltered['counts'] / age_bmi_unfiltered['counts'].sum()

age_bmi_filtered = cus_metrics_filtered.groupby(['status', 'last_age_cat', 'rh_name', 'last_bmi_cat']).size().reset_index(name='counts')
age_bmi_filtered['percent'] = age_bmi_filtered['counts'] / age_bmi_filtered['counts'].sum()

# Plotting the distribution of (gender, last_bmi_cat) in terms of percentages
#plt.figure(figsize=(12,6))
#sns.barplot(x='gender', y='percent', hue='last_bmi_cat', data=gender_bmi_filtered)
#plt.title('Distribution of Gender and BMI (in percentages excl. age < 20)')
#plt.show()

# Plotting the distribution of (last_age_cat, last_bmi_cat) in terms of percentages
#plt.figure(figsize=(12,6))
#sns.barplot(x='last_age_cat', y='percent', hue='last_bmi_cat', data=age_bmi_filtered)
#plt.title('Distribution of Age and BMI (in percentages excl. age < 20)')
#plt.show()

# Combine the two graphs as one
#all_bmi_filtered['age_gender'] = all_bmi_filtered['last_age_cat'] + ' - ' + all_bmi_filtered['gender']

# Re-plotting the distribution by Age-Gender and BMI in terms of percentages
#plt.figure(figsize=(12,6))
#sns.countplot(y='age_gender', hue='last_bmi_cat', data=all_bmi_filtered)
#plt.title('High risk distribution of BMI by Age and Gender (in % excl. age < 20)')
#plt.show()

# COMMAND ----------

display(all_bmi_filtered)

# COMMAND ----------

#display(age_bmi_unfiltered)
