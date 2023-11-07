# Databricks notebook source
# MAGIC %md
# MAGIC # Checking the following
# MAGIC ### Step 1. One-off load table from vn_processing_datamart_temp_db
# MAGIC ### Step 2. Refresh table weekly or half-weekly
# MAGIC ### Step 3. Store and automate the process

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>1. Load existing data</strong>

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

from pyspark.sql.functions import *
orc_path = '/asia/vn/lab/project/scratch/hive/'

# COMMAND ----------

orc_filename = 'mat_plan_code/'

# COMMAND ----------

wandisco_container = f"abfss://wandisco@{storage}.dfs.core.windows.net/"
path = get_dir_content(wandisco_container+orc_path+orc_filename)
df = spark.read.format("orc").option("recursiveFileLookup","True").load(path)
#print(path)
rdd = df.rdd.map(lambda x: x[-1])
schema_df = rdd.toDF(sampleRatio=0.1)
#df.display()

# COMMAND ----------

convert_orc_acid_to_parquet(f'{orc_path}{orc_filename}')

# COMMAND ----------

in_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/ORC_ACID_to_Parquet_Conversion/apps/hive/warehouse/vn_processing_datamart_temp_db.db/'
out_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/dashboard/'
#df = spark.read.parquet(in_path+orc_filename)
#df = df.withColumnRenamed('needs_typ', 'need_typ')
#spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#df.write.mode('overwrite').partitionBy('reporting_date').parquet(out_path+orc_filename)
