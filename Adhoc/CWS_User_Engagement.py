# Databricks notebook source
# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
#pd.set_option('display.max_rows', None)

lab_path = '/mnt/lab/vn/project/cpm/datamarts/'
dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'

tblSrc1 = 'TCUSTDM_MTHEND/'
tblSrc2 = 'TPOLIDM_MTHEND/'
tblSrc3 = 'TAGTDM_MTHEND/'

tcustdm_mthend = spark.read.parquet(f'{lab_path}{tblSrc1}')
tpolidm_mthend = spark.read.parquet(f'{dm_path}{tblSrc2}')
tagtdm_mthend = spark.read.parquet(f'{dm_path}{tblSrc3}')

# Set x to the number of months going backward
x = 0 
begin_mth = datetime.today().replace(day=1)
last_mth = begin_mth - relativedelta(months=x)
last_mthend = (last_mth - relativedelta(days=1)).strftime('%Y-%m-%d')
print("last_mthend:", last_mthend)

#tcustdm_mthend.filter(col("image_date") == last_mthend).groupBy("image_date") \
#    .agg(count(col("cli_num")).alias("no_customers"),
#         min(col("cws_lst_login_dt")).cast("date").alias("first_login_dt"),
#         max(col("cws_lst_login_dt")).cast("date").alias("last_login_dt"),
#         count(when(last_day(col("cws_lst_login_dt")) == lit("2024-06-30"), col("cli_num"))).alias("no_customer_login_Jun"),
#         count(when(last_day(col("cws_lst_login_dt")) == lit("2024-07-31"), col("cli_num"))).alias("no_customer_login_Jul"),
#         count(when(last_day(col("cws_lst_login_dt")) == lit("2024-08-31"), col("cli_num"))).alias("no_customer_login_Aug"),
#         count(when(last_day(col("cws_lst_login_dt")) == lit("2024-09-30"), col("cli_num"))).alias("no_customer_login_Sep"),
#         count(when(last_day(col("cws_lst_login_dt")) == last_mthend, col("cli_num"))).alias("no_customer_login_Oct")) \
#    .display()

# COMMAND ----------

tcustdm_mthend = tcustdm_mthend.filter(col('image_date') == last_mthend)
tpolidm_mthend = tpolidm_mthend.filter(col('image_date') == last_mthend)
tagtdm_mthend = tagtdm_mthend.filter(col('image_date') == last_mthend)

# Create temp views for these tables
tcustdm_mthend.createOrReplaceTempView('tcustdm_mthend')
tpolidm_mthend.createOrReplaceTempView('tpolidm_mthend')
tagtdm_mthend.createOrReplaceTempView('tagtdm_mthend')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Item 1

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Identify customers whose IDs match with those of agents</strong>

# COMMAND ----------

# Check and exclude customers with ID identical to those of agents
cus_agt_all = spark.sql("""                         
    select  cus.cli_num,
            cus.cli_nm,
            greatest(cus.cws_lst_login_dt, cus.cws_lst_login_dt_legacy) as cws_lst_login_dt,
            cus.cws_ind,
            cus.cws_joint_dt,
            agt.agt_code,
            agt.agt_nm,
            agt.loc_cd,
            agt.trmn_dt,
            agt.comp_prvd_num,
            cus.email_addr as cus_email,
            agt.email_addr as agt_email,
            cus.mobl_phon_num as cus_mobl_phon,
            agt.mobl_phon_num as agt_mobl_phon,
            case when agt.id_num is not null then 'Y' else 'N' end as agt_ind,
            case when cus.email_addr is not null then 'Y' else 'N' end as email_ind
    from    tcustdm_mthend cus left join
            tagtdm_mthend agt on trim(cus.id_num) = trim(agt.id_num)
    --where   agt.id_num is null 
""")
# Apply filter to select POs = Agents only
cus_excl_agt = cus_agt_all.filter(col('agt_ind') == 'Y')

cus_agt_all.createOrReplaceTempView('cus_agt_all')
cus_excl_agt.createOrReplaceTempView('cus_excl_agt')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Identify status and communication type (CWS/Email/Both or None)</strong>

# COMMAND ----------

# Just select PO who are also agents without CWS and emails
po_agt = spark.sql("""
                   
    select  distinct
            cli_num,
            email_ind,
            --nvl(cws_lst_login_dt, cws_joint_dt) as lst_login_dt,
            cws_ind,
            --cws_joint_dt as join_dt,
            case when pol.pol_stat_cd in ('1','2','3','5') then 'Active'
                 else 'Inactive'
            end as cus_status,
            -- Add agent status
            case
                when trmn_dt is not null
                    and comp_prvd_num in ('01','04', '97', '98') then 'Orphaned'
                when comp_prvd_num = '01'
                    and pol.sa_code = pol.wa_code                then 'Active'
                when comp_prvd_num = '01'                        then 'Active'
                when comp_prvd_num = '08'                        then 'Orphaned-GA'
                when comp_prvd_num = '04'                        then 'Orphaned'
                when comp_prvd_num in ('97', '98')               then 'Orphaned-SM'
                else 'Unknown'
            end as agt_status,
            case when cws_ind='N' and email_ind='N' then '3. No CWS or Email'
                 when cws_ind='N' and email_ind='Y' then '1. No CWS'
                 when cws_ind='Y' and email_ind='N' then '2. No Email'
                 else '0. CWS and/or Email'
            end as comm_type,
            pol_num,
            cli_nm,
            agt_code,
            agt_nm,
            loc_cd,
            cus_email,
            agt_email,
            cus_mobl_phon,
            agt_mobl_phon
    from    cus_excl_agt cus inner join
            tpolidm_mthend pol on cus.cli_num=pol.po_num
    where   pol.pol_stat_cd not in ('6','8','A','N','R','X')
        --and cws_ind='N'
        --and email_ind='N'
""")

po_agt_noncws = po_agt.filter(col('comm_type') != '0. CWS and/or Email')\
    .select(
        'comm_type',
        'cus_status',
        'agt_status',
        'pol_num',
        'cli_num',
        'cli_nm',
        'agt_nm',
        'agt_code',
        'loc_cd',
        'cus_email',
        'agt_email',
        'cus_mobl_phon',
        'agt_mobl_phon'
    )

po_agt_noncws_sum = po_agt_noncws.groupBy('cus_status', 'agt_status', 'comm_type')\
    .agg(
        countDistinct('cli_num').alias('no_of_clients'),
        count('pol_num').alias('no_of_policies')
    ).orderBy('cus_status', 'agt_status', 'comm_type')

po_agt_noncws_sum_pd = po_agt_noncws_sum.toPandas()

#print("Number of POs agents without both CWS and email:", po_agt_noncws.count())
display(po_agt_noncws_sum_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Print out details of active POs who are also active agents</strong> 

# COMMAND ----------

po_agt_noncws_act = po_agt_noncws.filter((col('cus_status') == 'Active') & 
                                         (col('agt_status') == 'Active'))
#po_agt_noncws_act.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Item 2

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Identify active customer, agent status and CWS status</strong>

# COMMAND ----------

# Just select PO of active/inforce policies and registered to CWS
cws_po = spark.sql("""
                   
    select  distinct
            cli_num,
            cus.cli_nm,
            pol_num,
            to_date(pol_iss_dt) as pol_iss_dt,
            cus.email_addr as cus_email,
            agt_nm,
            agt_code,
            loc_cd,
            case
                when trmn_dt is not null
                    and comp_prvd_num in ('01','04', '97', '98') then 'Orphaned'
                when comp_prvd_num = '01'
                    and pol.sa_code = pol.wa_code                then 'Active'
                when comp_prvd_num = '01'                        then 'Active'
                when comp_prvd_num = '04'                        then 'Orphaned'
                when comp_prvd_num = '08'                        then 'Orphaned-GA'
                when comp_prvd_num in ('97', '98')               then 'Orphaned-SM'
                else 'Unknown'
            end as agt_status,
            agt.email_addr as agt_email,
            agt.mobl_phon_num as agt_mobl_phon,
            nvl(cws_lst_login_dt, cws_joint_dt) as lst_login_dt,
            cws_ind,
            cws_joint_dt as join_dt,
            case when pol.pol_stat_cd in ('1','2','3','5') then 'Active'
                 else 'Inactive'
            end as status,
            cus.image_date
    from    tcustdm_mthend cus inner join
            tpolidm_mthend pol on cus.cli_num=pol.po_num inner join
            tagtdm_mthend agt on pol.sa_code=agt.agt_code
    where   1=1
        --and pol.pol_stat_cd not in ('6','8','A','N','R','X')
        --and pol.plan_code not in ('FDB01','PN001')
        --and cws_ind='Y'
""")
#print("Number of customers with CWS:", cws_po.count())

# Refilter the dataset and convert to Pandas
# Remove those who's never logged
cws_po = cws_po.filter(
    col('join_dt').isNotNull()
).orderBy('cli_num')

#print("Number of customers logged-on to CWS:", cws_po.count())

# COMMAND ----------

#cws_po.createOrReplaceTempView('cws_po')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Identify the CWS login period</strong>

# COMMAND ----------

# Calculate time gap between last month-end and latest login date
#def activity_period(row):
#    last_mthend_dt = datetime.strptime(last_mthend, '%Y-%m-%d')
#    lst_login_dt = pd.to_datetime(row['lst_login_dt'])
#    diff = relativedelta(last_mthend_dt, lst_login_dt)
#    months = diff.years * 12 + diff.months
#    if 1<= months < 3:
#        return "Last 3 months"
#    elif 3 <= months < 6:
#        return "Last 3 to 6 months"
#    elif 6 <= months < 12:
#        return "Last 6 to 12 months"
#    else:
#        return "More than 12 months"

#cws_po['activity_period'] = cws_po.apply(activity_period, axis=1)

#result_pd = cws_po.groupby(['status', 'activity_period'])\
#    .agg(
#        no_of_users=('cli_num', 'count')
#    ).reset_index().sort_values(['status', 'activity_period'])

#display(cws_po)
cws_po = cws_po.withColumn(
    'activity_period',
    when(
        months_between(lit(last_mthend), cws_po['lst_login_dt']) < 1,
        "Last 1 month"
    ).when(
        (months_between(lit(last_mthend), cws_po['lst_login_dt']) >= 1) &
        (months_between(lit(last_mthend), cws_po['lst_login_dt']) < 3),
        "Last 3 months"
    ).when(
        (months_between(lit(last_mthend), cws_po['lst_login_dt']) >= 3) &
        (months_between(lit(last_mthend), cws_po['lst_login_dt']) < 6),
        "Last 3 to 6 months"
    ).when(
        (months_between(lit(last_mthend), cws_po['lst_login_dt']) >= 6) &
        (months_between(lit(last_mthend), cws_po['lst_login_dt']) < 12),
        "Last 6 to 12 months"
    ).otherwise("More than 12 months")
)

#display(cws_po)

# COMMAND ----------

# Check for 'lst_login_dt'
#cws_po.groupBy("image_date").agg(
#    max(to_date(col("lst_login_dt"))).alias("last_login_date")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Result in summary</strong>

# COMMAND ----------

# Perform groupby and aggregation on the Spark DataFrame
cws_po_sum = cws_po.groupBy('status', 'activity_period') \
    .agg(
        countDistinct('cli_num').alias('no_of_users')
    ) \
    .orderBy('status', 'activity_period')

# Convert the summarized Spark DataFrame to Pandas
result_pd = cws_po_sum.toPandas()

# Calculate the sum of 'no_of_users' for each 'status' group
status_total = result_pd.groupby('status')['no_of_users'].transform('sum')

# Calculate the 'active%' column
result_pd['active%'] = result_pd['no_of_users'] / status_total
result_pd['active%'] = result_pd['active%'].map(lambda x: f'{x:.2%}')

# Calculate the sum of 'no_of_users' across all rows
total_users = result_pd['no_of_users'].sum()

# Calculate the 'active_all%' column
result_pd['active_all%'] = result_pd['no_of_users'] / total_users
result_pd['active_all%'] = result_pd['active_all%'].map(lambda x: f'{x:.2%}')

# If user wishes to remove Inactive customers
# result_pd = result_pd[result_pd['status'] != 'Inactive']

display(result_pd[result_pd['activity_period'] != 'More than 12 months'])

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Save details of Active customers with more than 12 months activity</strong>

# COMMAND ----------

#cws_po_filtered = cws_po.filter((col('status') != 'Inactive') & 
#                                (col('activity_period') == 'More than 12 months'))

#cws_po_filtered.display()
