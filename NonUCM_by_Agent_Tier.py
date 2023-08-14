# Databricks notebook source
mnth_end = '2023-07-31'
campaign_launch = '2023-07-31'

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.types import NullType
from pyspark.sql.functions import *

tcoverages = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCOVERAGES/')
tpolicys = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPOLICYS/')
tclient_policy_links  = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLIENT_POLICY_LINKS/')
tams_agents = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS/')
tams_candidates = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_CANDIDATES/')
loc_code_mapping = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/LOC_CODE_MAPPING/')
loc_to_sm_mapping = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/LOC_TO_SM_MAPPING/')
agent_scorecard = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/AGENT_SCORECARD/')
customer_needs_model_path = "abfss://wandisco@abcmfcadovnedl01psea.dfs.core.windows.net/asia/vn/lab/project/campaign_module/hive/vn_existing_customer_needs_model/"

tcoverages.createOrReplaceTempView('tcoverages')
tpolicys.createOrReplaceTempView('tpolicys')
tclient_policy_links.createOrReplaceTempView('tclient_policy_links')
tams_agents.createOrReplaceTempView('tams_agents')
tams_candidates.createOrReplaceTempView('tams_candidates')
loc_to_sm_mapping.createOrReplaceTempView('loc_to_sm_mapping')
agent_scorecard.createOrReplaceTempView('agent_scorecard')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Get list of all agents and their last score</strong>

# COMMAND ----------

agent_scorecard_last = spark.sql("""
                                 
select  agt_code, agent_tier, monthend_dt
from    agent_scorecard
order by agt_code, monthend_dt desc
""")

agent_scorecard_last = agent_scorecard_last.dropDuplicates(['agt_code'])
print("Number of agents with score:", agent_scorecard_last.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Get list of non-UCM policies</strong>

# COMMAND ----------

agent_scorecard_last.createOrReplaceTempView('agent_scorecard')
nonucm_pols = spark.sql("""
                        
SELECT DISTINCT
    po_lnk.cli_num                                      AS po_num
   ,pol.pol_num                                         AS pol_num
   ,pol.plan_code_base                                  AS plan_code
   ,cvg.cvg_prem*12/pol.pmt_mode                        AS inforce_APE
   ,cvg.xpry_dt
   ,pol.agt_code                                        AS sagt_cd
   ,sa.comp_prvd_num                                    AS sagt_comp_prvd_num
   ,CASE 
        WHEN sa.trmn_dt IS NOT NULL 
            AND sa.comp_prvd_num IN ('01','04', '97', '98') THEN 'Orphaned'
        WHEN sa.comp_prvd_num = '01'  
            AND (
                pol.agt_code = pol.wa_cd_1 
                OR pol.agt_code = pol.wa_cd_2
                )                                           THEN 'Original Agent'
        WHEN sa.comp_prvd_num = '01'                        THEN 'Reassigned Agent'
        WHEN sa.comp_prvd_num = '04'                        THEN 'Orphaned-Collector'
        WHEN sa.comp_prvd_num IN ('97', '98')               THEN 'Orphaned-Agent is SM'
        ELSE 'Non-Agency' 
    END                                                 AS cus_agt_rltnshp
    ,nvl(sc.agent_tier,'NaN')                           AS agent_tier
FROM tcoverages cvg 
INNER JOIN tpolicys pol
    ON cvg.pol_num = pol.pol_num
-- owner
INNER JOIN tclient_policy_links po_lnk 
    ON pol.pol_num = po_lnk.pol_num
        AND po_lnk.link_typ = 'O'
-- agent
INNER JOIN tams_agents sa 
    ON pol.agt_code = sa.agt_code
-- agents score
LEFT JOIN agent_scorecard sc
    ON sa.agt_code = sc.agt_code
WHERE 
    cvg.cvg_typ = 'B'
    AND cvg.cvg_reasn = 'O'
    -- still paying premium
    AND cvg.cvg_stat_cd IN ('1','3')
    -- agency sizing
    AND sa.comp_prvd_num in ('01','04','97','98')
""")

nonucm_pols = nonucm_pols.filter(col('cus_agt_rltnshp').isin(['Original Agent','Reassigned Agent'])).dropDuplicates(['pol_num'])
print("Number of policies served by non-UCM agents:", nonucm_pols.count())

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Break-down policies by agent tier</strong>

# COMMAND ----------

nonucm_summary = nonucm_pols.groupBy(['agent_tier'])\
    .agg(
        count('pol_num').alias('no_policies'),
        countDistinct('po_num').alias('no_customers')
    )

nonucm_summary = nonucm_summary.toPandas()
nonucm_summary

# COMMAND ----------

# Create a Spark session
spark = SparkSession.builder.appName("List Delta Parts").getOrCreate()

# Create a DBUtils object
dbutils = DBUtils(spark)

# List the directories under the delta/partitioned table path
directories_new = dbutils.fs.ls(customer_needs_model_path)

# Store the directory names in a list
customer_needs_model_files = [customer_needs_model_path+directory.name for directory in directories_new]
#print(customer_needs_model_files)
customer_needs_model = None
for file in customer_needs_model_files:
    temp_df = spark.read.format("orc").option("recursiveFileLookup","True").load(file)
    non_null_columns = [c for c in temp_df.columns if temp_df.schema[c].dataType != NullType()]
    temp_df = temp_df.select(*non_null_columns)
    if len(temp_df.columns) > 15: #add a check to filter out partition with less than 16 columns
        if customer_needs_model is None:
            customer_needs_model = temp_df
        else:
            customer_needs_model = customer_needs_model.union(temp_df)

# Compute the unique count of po_num by month
result_new_df = customer_needs_model.groupBy("monthend_dt").agg(countDistinct("cli_num").alias("unique_po_num_count"))

# Display the result
result_new_df.show()

# COMMAND ----------


