# Databricks notebook source
# MAGIC %md
# MAGIC ### Make sure the Data Persistency.csv is updated on the 6th (contact LE QUANG PHU or LE THI NGOC THU)

# COMMAND ----------

from pyspark.sql import functions as F

rpt_period = '202602' # reporting month

# reading in data tables
tpolicys = spark.table('vn_prod_catalog.vn_published_cas_db.tpolicys') \
                .select('pol_num', 'pol_eff_dt')
tucm     = spark.table('vn_prod_catalog.vn_published_ams_db.tams_ucm_assign_log') \
            .select('pol_num', 'term_agt', 'new_serv_agt', 'trxn_dt', 'propensity_typ', 'reassign_stat')

Data_Raw = spark.table("vn_aabuild_catalog.vn_lab_project_agency_l1b_report_db.persistency")
#Data_Raw = spark.read.option("header",True).csv("/mnt/lab/vn/project/analytics_datamart/Phu_Le_Quang_Data/mapping files/Data Persistency.csv")
Data_Raw_filtered = Data_Raw.filter(F.col('reporting_period_m13') >= rpt_period)

# find out the latest re-assignment of agent
tucm0 = tucm.join(tpolicys, on='pol_num', how='left')\
    .filter(F.col('trxn_dt')<F.add_months(F.col('pol_eff_dt'), 15))\
    .sort(F.col('trxn_dt').desc())\
    .dropDuplicates(['pol_num'])\
    .select('pol_num', 'new_serv_agt', 'trxn_dt')

# count if any reassignment within 15 months
tucm2 = tucm.join(tpolicys.select('pol_num', 'pol_eff_dt'), on='pol_num', how='left')\
    .filter(F.col('trxn_dt')<F.add_months(F.col('pol_eff_dt'), 15))\
    .groupby('pol_num').agg(F.count('pol_num').alias('ucm_cnt'))\
    .join(tucm0, on='pol_num', how='left')

Data_Raw_filtered.filter(F.col('channel_group_1')=='Agency')\
     .groupby('reporting_period_m13').agg(F.sum('exposure_ape_m13').alias('total_exposure'), F.sum('exposure_lapse_ape_m13').alias('total_lapse'), (1-F.sum('exposure_lapse_ape_m13')/F.sum('exposure_ape_m13')).alias('13m_lapse') )\
     .sort(F.col('reporting_period_m13').desc()).display()

temp = Data_Raw_filtered.filter(F.col('channel_group_1')=='Agency')\
    .join(tucm2, on=[tucm2['pol_num']==Data_Raw_filtered['Policy_number']], how='left')\
    .fillna({'ucm_cnt':0})\
    .withColumn('ucm_tag', F.when((F.col('ucm_cnt')>=1), 'ucm_13m_per').otherwise('original_13m_per'))\
    .groupby('reporting_period_m13').pivot('ucm_tag').agg((1-F.sum('exposure_lapse_ape_m13')/F.sum('exposure_ape_m13')).alias('13m_per') )\
    .sort(F.col('reporting_period_m13').desc())

temp2 = Data_Raw_filtered.filter(F.col('channel_group_1')=='Agency')\
    .join(tucm2, on=[tucm2['pol_num']==Data_Raw_filtered['Policy_number']], how='left')\
    .fillna({'ucm_cnt':0})\
    .withColumn('ucm_tag', F.when((F.col('ucm_cnt')>=1), 'ucm_exposure').otherwise('original_exposure'))\
    .groupby('reporting_period_m13').pivot('ucm_tag').agg(F.sum('exposure_ape_m13') )\
    .sort(F.col('reporting_period_m13').desc())

temp3 = temp.join(temp2, on='reporting_period_m13', how='inner') \
            .select(F.col('reporting_period_m13').alias('reporting_month'),
                    'original_exposure',
                    'ucm_exposure',
                    'original_13m_per',
                    'ucm_13m_per'
                    ) \
            .orderBy(F.desc('reporting_period_m13'))

temp3.display()
