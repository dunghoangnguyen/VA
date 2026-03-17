# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start of Lapse Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC select  fld_valu, fld_valu_desc desc_vnese, fld_valu_desc_eng desc_eng
# MAGIC from    vn_published_cas_db.tfield_values
# MAGIC where   fld_nm = 'REASN_CODE'
# MAGIC   --and   fld_valu like '%301%'
# MAGIC order by fld_valu

# COMMAND ----------

sql_string = '''
with pmt1 as (
select  pol.pol_num, to_date(pol_iss_dt) pol_iss_dt, to_date(last_pd_to_dt) last_pd_to_dt, cast(pmt_mode as int) pmt_mode, 
        cast(mode_prem as int) as mode_prem, cast(renw_yr as int) renw_yr, to_date(eff_dt) eff_dt, cast(trxn_amt as int) trxn_amt, reasn_code,
        row_number() over (partition by txn.pol_num order by eff_dt desc) cycle
from    vn_published_cas_db.tpolicys pol inner join
        vn_published_cas_db.ttrxn_histories txn on pol.pol_num=txn.pol_num
where 1=1
  --and pol.pol_num='2840591190'
  and pol_stat_cd in ('1','3')
  and renw_yr >= 1
  and txn.trxn_cd = 'APPREM'
    and (txn.trxn_desc LIKE '%Premium Pay%'
        OR txn.trxn_desc LIKE 'Premium applied as shown%'
        OR txn.trxn_desc LIKE 'Arrears premium applied%')
    and txn.pd_to_dt IS NOT NULL
    and txn.trxn_amt IS NOT NULL
    and txn.trxn_amt > 0
    and txn.undo_trxn_id IS NULL
    and txn.reasn_code in ('301','304','306','308','310')
order by eff_dt),
pmt2 as (
select  pol_num, pol_iss_dt, pmt_mode, renw_yr, trxn_amt, cycle, last_pd_to_dt, add_months(last_pd_to_dt, -1*cycle*pmt_mode) cycle_pd_to_dt, eff_dt
from    pmt1)
select  *, datediff(eff_dt, cycle_pd_to_dt) day_diff
from    pmt2
'''

#df = sql_to_df(sql_string, 0, spark)

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType, DateType, LongType
from datetime import datetime, timedelta, date
import calendar

image_date='2024-03-31'

src_path1 = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
src_path5 = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
src_path6 = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'

tpolidm_df = spark.read.parquet(f'{src_path1}TPOLIDM_MTHEND/image_date={image_date}/')
tcustdm_df = spark.read.parquet(f'{src_path1}TCUSTDM_MTHEND/image_date={image_date}/')
tpolicys_df = spark.read.parquet(f'{src_path6}TPOLICYS/image_date={image_date}/')
ttrxn_his_df = spark.read.parquet(f'{src_path5}TTRXN_HISTORIES/')
tfield_df = spark.read.parquet(f'{src_path6}TFIELD_VALUES/*')

df_list = [tpolidm_df, tcustdm_df, tpolicys_df, ttrxn_his_df, tfield_df]

# Loop through each DataFrame and convert column names to lowercase
for i, df in enumerate(df_list):
    df_list[i] = df.toDF(*[col.lower() for col in df.columns])
    #print(f'{i}:', df_list[i].count())

# COMMAND ----------

# Unpack the modified DataFrames back to their original names
tpolidm_df, tcustdm_df, tpolicys_df, ttrxn_his_df, tfield_df = df_list

tpol_cols = ['pol_num', 'plan_code_base', 'pol_iss_dt', 'pol_stat_cd', 'dist_chnl_cd', 'pmt_mode', 'mode_prem', 'last_pd_to_dt',
             'renw_yr', 'ins_typ_base']
trxn_cols = ['pol_num', 'pd_to_dt', 'trxn_amt', 'eff_dt', 'trxn_cd', 'trxn_desc', 'trxn_id', 'undo_trxn_id', 'reasn_code']
tpo_cols = ['pol_num', 'po_num']
tcli_cols = ['cli_num', 'sms_ind', 'email_addr']
tfield_cols = ['fld_nm', 'fld_valu', 'fld_valu_desc']

tpol_df = tpolicys_df.select(*tpol_cols).dropDuplicates()
trxn_df = ttrxn_his_df.filter(F.col('eff_dt')<=image_date).select(*trxn_cols)
tpo_df  = tpolidm_df.select(*tpo_cols).dropDuplicates()
tcli_df = tcustdm_df.select(*tcli_cols).dropDuplicates()
tfield_df = tfield_df.filter(F.col("fld_nm")=="INS_TYP").select(*tfield_cols).dropDuplicates()

# Filter tpol_df and trxn_df based on the given conditions
# (F.col('pol_stat_cd').isin('1', '3'))
# (~F.col('pol_stat_cd').isin('8','A','N','R','X'))
filtered_tpol_df = tpol_df.filter((~F.col('pol_stat_cd').isin('8','A','N','R','X')) &
                                  (F.col('mode_prem')>0)) \
                        .join(tfield_df, tpol_df['ins_typ_base'] == tfield_df['fld_valu'], how='left') \
                        .withColumnRenamed('fld_valu_desc', 'product_type')

filtered_trxn_df = trxn_df.filter((F.year(F.col('eff_dt')).isin([2021,2022,2023,2024])) &
                                  (F.col('trxn_cd') == 'APPREM') &
                                  (F.col('trxn_desc').like('%Apply Premium%') |
                                   F.col('trxn_desc').like('Premium applied as shown%') |
                                   F.col('trxn_desc').like('Arrears premium applied%')) &
                                  F.col('pd_to_dt').isNotNull() &
                                  F.col('trxn_amt').isNotNull() &
                                  (F.col('trxn_amt') > 0) &
                                  # Exclude reversal transactions
                                  F.col('undo_trxn_id').isNull() &
                                  # Only select Renewal premium collection
                                  (F.col('reasn_code') == '301'))\
                          .groupBy('pol_num', 'pd_to_dt') \
                          .agg(
                              (F.sum(F.col('trxn_amt'))/F.count(F.col('pol_num'))).alias('trxn_amt'),
                              F.min('eff_dt').alias('eff_dt')
                          )

filtered_trxn_df.agg(
    F.countDistinct('pol_num').alias('total_policies'),
    F.count('pol_num').alias('total_txns')
).display()

# COMMAND ----------

# DBTITLE 1,Adhoc Sizing for Premium collection notices
# Join the filtered DataFrames with table aliases and perform the required aggregations
pmt_rslt_df = filtered_tpol_df.alias('pol').join(filtered_trxn_df.alias('txn'),
                                                F.col('pol.pol_num') == F.col('txn.pol_num'),
                                                'inner') \
                                            .join(tpo_df.alias('po'),
                                                  F.col('pol.pol_num') == F.col('po.pol_num'),
                                                  'inner') \
                                            .join(tcli_df.alias('cli'),
                                                  F.col('po.po_num') == F.col('cli.cli_num'),
                                                  'inner'
                                                  ) \
    .select('po_num', 'pol.pol_num', 'product_type', 'plan_code_base', 'pol_iss_dt', 'pol_stat_cd', 'dist_chnl_cd', 'pmt_mode', 'mode_prem', 'trxn_amt', 'last_pd_to_dt', 'renw_yr', 'eff_dt', 'sms_ind', 'email_addr') \
    .withColumn('iss_yr', F.year(F.col('pol_iss_dt'))) \
    .withColumn('pmt_mode', F.col('pmt_mode').cast('int')) \
    .withColumn('ape_usd', (F.col('pmt_mode') / 12 * F.col('mode_prem') / 23.145).cast('float')) \
    .withColumn('prem_amt_paid', F.round((F.col('trxn_amt') / 23.145).cast('float'), 2)) \
    .withColumn('last_pd_to_dt', F.col('last_pd_to_dt').cast('date')) \
    .withColumn('eff_dt', F.col('eff_dt').cast('date')) \
    .withColumn('renw_yr', F.col('renw_yr').cast('int')) \
    .withColumn('contact_channel', 
                F.when((F.col('sms_ind')=='Y') & (F.col('email_addr').isNotNull()), '1. Both Email & SMS')
                 .when((F.col('sms_ind')=='Y') & (F.col('email_addr').isNull()), '2. SMS Only')
                 .when((F.col('sms_ind')=='N') & (F.col('email_addr').isNotNull()), '3. Email Only')
                 .when((F.col('sms_ind')=='N') & (F.col('email_addr').isNull()), '4. None')
                ) \
    .withColumn('status', F.when(F.col('pol_stat_cd').isin('1','2','3','5','7','9'), '1. Inforce')
                           .when(F.col('pol_stat_cd') == 'B', '2. Lapsed')
                           .otherwise('3. Expired/Matured')
                ) \
    .withColumn('renw_yr_cat', F.when(F.col('renw_yr')==1, "1")
                .when(F.col('renw_yr')==2, "2")
                .when(F.col('renw_yr')==3, "3")
                .otherwise("4+")
                )

# Add new year columns to pmt_rslt_df
pmt_rslt_df = pmt_rslt_df.withColumn('pmt_yr', F.year('eff_dt')) \
                         .withColumn('no_year', F.col('pmt_yr') - F.col('iss_yr'))

# Add cycle numbers using row_number() function
windowSpec = Window.partitionBy('pol_num').orderBy(F.desc(F.col('eff_dt')))
pmt_rslt_df = pmt_rslt_df.withColumn('cycle', F.row_number().over(windowSpec))

# Calculate cycle_pd_to_dt using the below formula
# cycle_pd_to_dt =  last_pd_to_dt - (#'s of months of) cycle * pmt_mode
pmt_rslt_df = pmt_rslt_df.withColumn('cycle_pd_to_dt', F.add_months(F.col('last_pd_to_dt'), -1 * (F.col('pmt_mode') * F.col('cycle'))))

# Calculate day_diff
pmt_rslt_df = pmt_rslt_df.withColumn('day_diff', F.datediff(F.col('eff_dt'), F.col('cycle_pd_to_dt')))

pmt_rslt_df = pmt_rslt_df.withColumn('day_category', 
                                     F.when(F.col('day_diff') < -14, '01. Before T-14')
                                     .when((F.col('day_diff') >= -14) & (F.col('day_diff') < -7), '02. T-14 to T-7')
                                     .when((F.col('day_diff') >= -7) & (F.col('day_diff') < 0), '03. T-7 to T')
                                     .when((F.col('day_diff') >= 0) & (F.col('day_diff') < 7), '04. T to T+7')
                                     .when((F.col('day_diff') >= 7) & (F.col('day_diff') < 14), '05. T+7 to T+14')
                                     .when((F.col('day_diff') >= 14) & (F.col('day_diff') < 25), '06. T+14 to T+25')
                                     .when((F.col('day_diff') >= 25) & (F.col('day_diff') < 45), '07. T+25 to T+45')
                                     .when((F.col('day_diff') >= 45) & (F.col('day_diff') < 55), '08. T+45 to T+55')
                                     .when((F.col('day_diff') >= 55) & (F.col('day_diff') < 60), '09. T+55 to T+60')
                                     .otherwise('10. After T+60'))

pmt_rslt_df = pmt_rslt_df.withColumn('channel_category', 
                                     F.when(F.col('dist_chnl_cd').isin('01','02','08','50','*'), '1. Agency')
                                     .when(F.col('dist_chnl_cd').isin('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53','54','55'), '2. Banca')
                                     .otherwise('3. Others'))

pmt_rslt_df = pmt_rslt_df.withColumn('pmt_mode_category', 
                                     F.when(F.col('pmt_mode') == 12, '1. Yearly')
                                     .when(F.col('pmt_mode') == 6, '2. Half-yearly')
                                     .when(F.col('pmt_mode') == 3, '3. Quarterly')
                                     .when(F.col('pmt_mode') == 1, '4. Monthly'))

# Finalize the DataFrame
f_cols = ['product_type', 'po_num', 'pol_num', 'pmt_mode', 'ape_usd', 'prem_amt_paid', 
          'iss_yr', 'pmt_yr', 'renw_yr', 'last_pd_to_dt', 'cycle', 'pmt_mode_category', 
          'cycle_pd_to_dt', 'eff_dt', 'day_diff', 'day_category', 'status', 'renw_yr_cat',
          'channel_category', 'contact_channel']

result_df = pmt_rslt_df.select(*f_cols).dropDuplicates()

# Print out the size (count of records) of the DataFrame
#print("The size (count of records) of result_df is:", result_df.count())

# Show the resulting DataFrame
#result_df.groupBy('product_type') \
#      .agg(
#            F.countDistinct(F.col('po_num')).alias('total_clients'),
#            F.countDistinct(F.col('pol_num')).alias('total_policies')
#      ).display()
#

# COMMAND ----------

result_df.write.mode("overwrite").parquet('/mnt/lab/vn/project/lapse/lapse_profile/lapse_analysis/')

# COMMAND ----------

# Reload the df and filter out the unwanted records
pmt_rslt_df = spark.read.parquet('/mnt/lab/vn/project/lapse/lapse_profile/lapse_analysis/')

result_filtered_df = result_df.filter(F.col('pmt_mode_category')=="1. Yearly")

# COMMAND ----------

# MAGIC %md
# MAGIC # Full Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Break-down by all KPIs:<br>
# MAGIC <strong>
# MAGIC 1. Channel<br>
# MAGIC 2. Product type<br>
# MAGIC 3. Payment year<br>
# MAGIC 4. Day category</strong>

# COMMAND ----------

result_filtered_sum = result_filtered_df.filter((F.col("status") == "1. Inforce")) \
                            .groupBy("channel_category", "product_type" , "renw_yr_cat", "day_category"
                                     ) \
                            .agg(F.count("pol_num").alias("pol_count"))
result_filtered_sum.display()
# Convert the Dataframe to Pandas
result_sum_pd = result_filtered_sum.toPandas()

# Pivot the data to get the desired format
result_pivot_pd = result_sum_pd.pivot_table(index='product_type', 
                                            columns='channel_category', 
                                            values='pol_count', 
                                            fill_value=0).reset_index() \
                                        .sort_values(by='product_type', ascending=True)
# Show the result
display(result_pivot_pd)

# COMMAND ----------

result_filtered_sum = result_filtered_df.filter((F.col("status") == "1. Inforce")) \
                            .groupBy("channel_category", "product_type" #, "renw_yr_cat", "day_category"
                                     ) \
                            .agg(F.countDistinct("pol_num").alias("pol_count"))
#result_filtered_sum.display()
# Convert the Dataframe to Pandas
result_sum_pd = result_filtered_sum.toPandas()

# Pivot the data to get the desired format
result_pivot_pd = result_sum_pd.pivot_table(index='product_type', 
                                            columns='channel_category', 
                                            values='pol_count', 
                                            fill_value=0).reset_index() \
                                        .sort_values(by='product_type', ascending=True)
# Show the result
display(result_pivot_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### No. of Unique Customers & Policies
# MAGIC <strong>Get # unique customers and in-force policies</strong>

# COMMAND ----------

# Group by pmt_mode_category, pmt_yr, and channel_category, and count the pol_num
po_count = result_filtered_df.filter(F.col("status")=="1. Inforce") \
                    .groupBy("channel_category").agg(F.countDistinct("po_num").alias("po_count"),
                                                     F.countDistinct("pol_num").alias("pol_count"))

# Convert the Dataframe to Pandas
po_count = po_count.toPandas()

# Melt the DataFrame to make it long-form
melted_df = pd.melt(po_count, id_vars=["channel_category"], var_name="count_type", value_name="count")

# Pivot to get the desired structure
po_pivot = melted_df.pivot(index="count_type", columns="channel_category", values="count")

display(po_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>No. of Lapse and APE loss (US$)

# COMMAND ----------

# Group by pmt_mode_category, pmt_yr, and channel_category, and count the pol_num
po_count = result_filtered_df.filter(F.col("status") == "2. Lapsed") \
                    .groupBy("channel_category") \
                    .agg(F.countDistinct("po_num").alias("1.po_count"),
                         F.countDistinct("pol_num").alias("2.pol_count"),
                         (F.sum(F.col("ape_usd"))/1000).alias("3.mAPE_loss")
                         )
                    
#po_count.show()
# Convert the Dataframe to Pandas
lapse_cus = po_count.toPandas()

# Melt the DataFrame to make it long-form
melted_df = pd.melt(lapse_cus, id_vars=["channel_category"], var_name="count_type", value_name="count")

# Pivot to get the desired structure
po_pivot = melted_df.pivot(index="count_type", columns="channel_category", values="count").reset_index()

display(po_pivot)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Premium Due Notices
# MAGIC <strong>Count by pmt_mode_category on the vertical axis, and channel_category on the horizontal axis:</strong>

# COMMAND ----------

# Group by pmt_mode_category, pmt_yr, and channel_category, and count the pol_num
# (F.col('pmt_yr').isin([2023,2024])) & 
# (F.col('renw_yr').isin([1,2,3])) &
mode_year_count = result_df.filter((F.col("status") == "1. Inforce") &
                                   (F.col('pmt_mode_category')=="1. Yearly")) \
                            .groupBy("renw_yr_cat", "pmt_mode_category", "channel_category") \
                            .agg(F.count("pol_num").alias("count"))

# Convert the Dataframe to Pandas
mode_year_count_pd = mode_year_count.toPandas()

# Pivot the data to get the desired format
mode_year_pivot_pd = mode_year_count_pd.pivot_table(index=['renw_yr_cat', 'pmt_mode_category'], 
                                                    columns='channel_category', 
                                                    values='count', 
                                                    fill_value=0).reset_index() \
                                        .sort_values(by=['renw_yr_cat', 'pmt_mode_category'], ascending=True)
# Show the result
display(mode_year_pivot_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Count by pmt_mode_category and pmt_yr on the vertical axis, and channel_category on the horizontal axis:</strong>

# COMMAND ----------

# Group by pmt_mode_category, pmt_yr, and channel_category, and count the pol_num
mode_year_count = result_df.filter((F.col('pmt_yr').isin([2023,2024])) & 
                                   (F.col('renw_yr').isin([1,2,3])) &
                                   (F.col('pmt_mode_category')=="1. Yearly")) \
                            .groupBy("renw_yr", "pmt_mode_category", "pmt_yr", "channel_category") \
                            .agg(F.count("pol_num").alias("count"))

# Convert the Dataframe to Pandas
mode_year_count_pd = mode_year_count.toPandas()

# Pivot the data to get the desired format
mode_year_pivot_pd = mode_year_count_pd.pivot_table(index=['renw_yr', 'pmt_mode_category', 'pmt_yr'
                                                           ], 
                                                    columns='channel_category', 
                                                    values='count', 
                                                    fill_value=0).reset_index() \
                                        .sort_values(by='pmt_yr', ascending=False) \
                                        .sort_values(by=['renw_yr', 'pmt_mode_category'], ascending=True)
# Show the result
display(mode_year_pivot_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC </strong>Count by day_category_min on the vertical axis, and channel_category on the horizontal axis:</strong>

# COMMAND ----------

# Filter out unecessary payment modes
result_filtered_df = result_df.filter((F.col("status") == "1. Inforce") &
                                      (F.col('pmt_mode_category')=="1. Yearly"))

# Group by day_category and channel_category, and count the pol_num
day_count = result_filtered_df.groupBy("day_category", "channel_category").agg(F.count("pol_num").alias("count"))

# Pivot the data to get the desired format
day_pivot = day_count.groupBy("day_category").pivot("channel_category").sum("count").orderBy("day_category")

# Replace null values with 0
day_pivot = day_pivot.na.fill(0)

# Show the result
day_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Policy count by Email and Mobile

# COMMAND ----------

# Group by contact_channel and channel_category, and count the pol_num
contact_count = result_filtered_df.filter(F.col("status") == "1. Inforce") \
    .groupBy("contact_channel", "channel_category").agg(F.countDistinct("po_num").alias("count"))

# Pivot the data to get the desired format
contact_pivot = contact_count.groupBy("contact_channel").pivot("channel_category").sum("count").orderBy("contact_channel")

# Replace null values with 0
contact_pivot = contact_pivot.na.fill(0)

# Show the result
contact_pivot.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data extraction for Sep'24 to Mar'25 satisfying all these conditions:
# MAGIC 1. Policy status '1'
# MAGIC 2. Exclude monthly payment
# MAGIC 3. Exclude 1 year term products
# MAGIC 4. PY1-PY3 (pol_eff_dt >= Sep'21)
# MAGIC 5. Agecny, VTB and TCB

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

run_date = pd.Timestamp.now().strftime('%Y%m%d')

pol_string = '''
with base_sa as (
    select  pol_num, sum(FACE_AMT) base_sa
    from    vn_published_cas_db.tcoverages
    where   cvg_typ='B'
    group by pol_num
)
select  pol.pol_num,
        to_date(pol_eff_dt) effective_date,
        cast(base_sa as int) base_sa,
        plan_code_base basic_product,
        to_date(pd_to_dt) PTD,
        to_date(bill_to_dt) BTD,
        pol_stat_cd status,
        cast(DSCNT_PREM as int) regular_premium,
        substr(to_date(bill_to_dt),1,7) NEXT_BILLING,
        pmt_mode, case PMT_MODE
          when '12' then 'Yearly'
          when '06' then 'Half-yearly'
          when '03' then 'Quarterly'
          when '01' then 'Monthly'
        end payment_mode,
        cpl.cli_num po_num,
        dist_chnl_cd,
        case when dist_chnl_cd in ('*','01','02','08','50') then 'Agency'
             when dist_chnl_cd in ('10','24','49') then 'TCB'
             when dist_chnl_cd in ('52','53') then 'VTB'
             else 'Others'
        end Channel,
        datediff(current_date,to_date(pd_to_dt)) days_overdue,
        floor(months_between(current_date,to_date(pol_eff_dt)) / 12) + 1 year_since_effective,
        floor(months_between(bill_to_dt,pol_eff_dt) / 12) + 1 year_to_next_pd,
        floor(months_between(to_date(pd_to_dt), to_date(pol_eff_dt)) / 12) + 1 as no_payment_year,
        cli.mobl_phon_num po_mobile,
        oth.email_addr po_email
from    vn_published_cas_db.tpolicys pol inner join
        vn_published_cas_db.tclient_policy_links cpl on pol.pol_num=cpl.pol_num and cpl.link_typ='O' and cpl.rec_status='A' inner join
        vn_published_cas_db.tclient_details cli on cpl.cli_num=cli.cli_num inner join
        vn_published_cas_db.tclient_other_details oth on cli.cli_num=oth.cli_num inner join
        base_sa on pol.pol_num=base_sa.pol_num
where   1=1
  --and pol.PMT_MODE <> '01'    -- Exclude monthly payment
  and   pol_stat_cd = '1'       -- Only Premium paying
  and   substr(PLAN_CODE_BASE,1,3) not in ('FDB','BIC','PN0','PA0','CA3','CX3') -- Exclude single term
  and   PLAN_CODE_BASE not in ('UL001','UL004','UL005','UL007','UL035','UL036')         -- Exclude single premium payment
  and   DIST_CHNL_CD in ('*','01','02','08','50','10','24','49','52','53')      -- Only Agency, TCB and VTB
  --and   to_date(BILL_TO_DT) between '2024-06-30' and '2024-08-31'
  and   to_date(BILL_TO_DT) between '2024-09-01' and '2025-03-31'                 -- Due from Sep'24 to Mar'25
'''
# Execute the SQL query and get the DataFrame
pol_df = spark.sql(pol_string)

# Apply the filtering conditions
result_df = pol_df.filter(
    (F.col("no_payment_year") <= 3) #&                               # Capture only policies from PY1 to PY3
    #((F.col("days_overdue") == 1) | (F.col("days_overdue") == 55))  # Overdue 1 or 55 days
)

# Cache result_df as it is used multiple times
result_df.cache()

# Aggregation to get the count and distinct count of pol_num
agg_result = result_df.agg(
    F.count("pol_num").alias("rows"), 
    F.countDistinct("pol_num").alias("pols")
).collect()[0]

# Extract the counts from the aggregation result
total_count = agg_result['rows']
distinct_count = agg_result['pols']

# Check if the count of pol_num is equal to the distinct count of pol_num
if total_count == distinct_count:
    print("OK:", total_count)
else:
    print("Difference found:", total_count-distinct_count, "re-check!")


# COMMAND ----------

rider_string = '''
with rider_typ as (
select  pol_num
        , case when pln.prod_typ='TP_RIDER' then 'Y' else 'N' end as all_tp_rider
        , case when pln.prod_typ='CI_RIDER' then 'Y' else 'N' end as all_ci_rider
        , case when pln.prod_typ='MC_RIDER' then 'Y' else 'N' end as all_mc_rider
        , case when pln.prod_typ='ADD_RIDER' then 'Y' else 'N' end as all_add_rider
        , case when pln.prod_typ='HC_RIDER' then 'Y' else 'N' end as all_hc_rider
        , case when pln.prod_typ='TPD_RIDER' then 'Y' else 'N' end as all_tpd_rider
        , case when pln.prod_typ not in ('TP_RIDER','CI_RIDER','MC_RIDER','ADD_RIDER','HC_RIDER','TPD_RIDER') then 'Y' else 'N' end as all_oth_rider
from    vn_published_cas_db.tcoverages cvg inner join
        vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code and cvg.vers_num=pln.vers_num
where   cvg.cvg_typ<>'B'
)
select  cvg.pol_num
        ,coalesce(max(all_tp_rider),'N') as all_tp_rider
        ,coalesce(max(all_ci_rider),'N') as all_ci_rider
        ,coalesce(max(all_mc_rider),'N') as all_mc_rider
        ,coalesce(max(all_add_rider),'N') as all_add_rider
        ,coalesce(max(all_hc_rider),'N') as all_hc_rider
        ,coalesce(max(all_tpd_rider),'N') as all_tpd_rider
        ,coalesce(max(all_oth_rider),'N') as all_oth_rider
from    vn_published_cas_db.tcoverages cvg left join
        rider_typ on cvg.pol_num=rider_typ.pol_num
where   cvg.cvg_typ='B'
group by cvg.pol_num
'''
rider_df = sql_to_df(rider_string, 1, spark)

result_df = result_df.join(rider_df, on="pol_num", how="left")

#print(result_df.count())


# COMMAND ----------

result_df.toPandas().to_csv(f"/dbfs/mnt/lab/vn/project/scratch/adhoc/premium_reminder_list_{run_date}.csv", index=False, header=True, encoding='utf-8-sig')
#print(result_df.count())

# COMMAND ----------

# MAGIC %sql
# MAGIC with base_sa as (
# MAGIC     select  pol_num, sum(FACE_AMT) base_sa
# MAGIC     from    vn_published_cas_db.tcoverages
# MAGIC     where   cvg_typ='B'
# MAGIC     group by pol_num
# MAGIC ), lst_prem as (
# MAGIC select pol_num, max(to_date(prem_dt)) prem_pd_dt, count(pol_num) no_payment_made
# MAGIC from  vn_published_cas_db.tpremium_histories
# MAGIC where prem_typ in ('301','304','306','310')
# MAGIC group by pol_num
# MAGIC )
# MAGIC select  pol.pol_num, plan_code_base, to_date(POL_EFF_DT) eff_dt, to_date(PD_TO_DT) pol_pd_to_dt, to_date(BILL_TO_DT) bill_to_date, PMT_MODE, DIST_CHNL_CD
# MAGIC         , datediff(greatest(last_pd_to_dt,pol_eff_dt),to_date(pol_eff_dt))/365.25 no_pmt_year, no_payment_made
# MAGIC from    vn_published_cas_db.tpolicys pol left join
# MAGIC         lst_prem on pol.pol_num=lst_prem.pol_num
# MAGIC where   1=1
# MAGIC   --and   to_date(pol_eff_dt) between '2022-06-01' and '2022-09-31'
# MAGIC   --and   pmt_mode='03'
# MAGIC   --and   pol_stat_cd = '1'
# MAGIC   --and   to_date(pd_to_dt) between '2024-06-01' and '2024-12-31'
# MAGIC   --and pol.pol_num in ('2903357332','2805191663','2855950513')
# MAGIC   and pol.pol_num in ('28955976781','2901516970','2892120294','2806059398') -- Quarter
# MAGIC   --and pol.pol_num in ('3830525117','2891638593','3821485966','2803941747','3832260432') -- Half-yearly
# MAGIC   --and pol.pol_num in ('3833115759','2806537641')
# MAGIC
