# Databricks notebook source
# MAGIC %md
# MAGIC # Inbound Call Analysis

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import date, timedelta

spark.conf.set('partitionOverwriteMode', 'dynamic')

sfdc_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/'
cics_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CICS_DB/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'

case_source = 'CASE/'
user_source = 'USER/'
tcisc_source = 'TCISC_SERVICE_DETAILS/'
tfield_source = 'TFIELD_VALUES/'

daily_paths = [sfdc_path,cics_path,cas_path]
daily_files = [case_source,user_source,tcisc_source,tfield_source]

st_mth = date(2023, 1, 1).strftime('%Y-%m-%d')
end_mth = date(2023, 9, 30).strftime('%Y-%m-%d')
print('Start and end date:', st_mth, end_mth)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

list_df = {}

daily_df = load_parquet_files(daily_paths, daily_files)

list_df.update(daily_df)

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

resultDF = spark.sql(f"""
SELECT DISTINCT
        cse.CaseNumber as case_num,
        to_date(cse.createddate) as create_date,
        substr(cse.createddate,1,7) as contact_month,
        to_date(cse.ClosedDate) as close_date,
        policy_number__c as pol_num,
        origin,
        type,
        sub_type__c,
        subject,
        description,
        --cse.Case_Handling_Time__c as handling_time,
        cse.Requester_Role__c as requester_role,
        cse.Type__c as case_type,
        status,
        division
FROM    `case` cse inner join 
        `user` usr on cse.ownerid = usr.id
WHERE   1=1
    and TO_DATE(cse.createddate) between '{st_mth}' and '{end_mth}'
    and policy_number__c is not null
    --and origin = 'Inbound call'
    and status = 'Closed'
    --and division in ('CCHN','CCHCM')              
""")

resultDF.count()

# COMMAND ----------

result_sum = resultDF.groupBy(['contact_month','origin'])\
    .agg(
        count('case_num').alias('no_cntct'),
        #countDistinct('pol_num').alias('no_pols'),
        #mean('handling_time').alias('avg_handling_time')
    )\
    .where(col('origin')=='CWS')\
    .orderBy('contact_month')

# COMMAND ----------

result_sum.display()

# COMMAND ----------


