# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType, DateType, LongType
from datetime import datetime, timedelta, date
import calendar

image_date='2024-09-30'

src_path1 = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
src_path5 = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
src_path6 = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'

spark.read.parquet(f'{src_path1}TPOLIDM_MTHEND/image_date={image_date}/').createOrReplaceTempView('tpolidm_mthend')
spark.read.parquet(f'{src_path6}TPOLICYS/image_date={image_date}/').createOrReplaceTempView('tpolicys')
spark.read.parquet(f'{src_path1}TCUSTDM_MTHEND/image_date={image_date}/').createOrReplaceTempView('tcustdm_mthend')
spark.read.parquet(f'{src_path5}TTRXN_HISTORIES/*').createOrReplaceTempView('ttrxn_histories')
spark.read.parquet(f'{src_path6}TFIELD_VALUES/*').createOrReplaceTempView('tfield_values')
spark.read.parquet(f'{src_path6}TWRK_CLIENT_APE/image_date={image_date}/').createOrReplaceTempView('twrk_client_ape')

#df_list = [tpolidm_df, tpolicys_df, ttrxn_his_df, tfield_df, tvip_df]

# Loop through each DataFrame and convert column names to lowercase
#for i, df in enumerate(df_list):
#    df_list[i] = df.toDF(*[col.lower() for col in df.columns])


# COMMAND ----------

sql_pmt_string = '''
with pmt1 as (
select  pol.po_num, pol.pol_num, cast(pol.base_ape+pol.rid_ape as int) tot_ape, pol.pd_to_dt PTD,
        case when year(pol.pd_to_dt) - year(pol.pol_eff_dt) <  5 then 'Year 1-5'
             when year(pol.pd_to_dt) - year(pol.pol_eff_dt) < 10 then 'Year 6-10'
             else 'Year 10+'
        end as policy_tenure, cast(trxn_amt as int) trxn_amt
from    tpolidm_mthend pol inner join
        vn_published_cas_db.ttrxn_histories txn on pol.pol_num = txn.pol_num
where   1=1
    and pol.pol_stat_cd in ('1','3')
    and year(txn.trxn_dt) = 2023
    and year(pol.pd_to_dt) = 2024
    and txn.trxn_cd = 'APPREM'
    and txn.reasn_code in ('301','304','306','310')
    and txn.trxn_amt > 0
    and txn.undo_trxn_id IS NULL),
pmt2 as (
select  vip.vip_typ_desc vip_type, vip.cli_num, cast(vip.ape as int) vip_ape, 
        pol_num, policy_tenure, tot_ape,
        sum(trxn_amt) total_ape_paid
from    twrk_client_ape vip inner join
        pmt1 on vip.cli_num = pmt1.po_num
group by vip_type, cli_num, vip_ape, pol_num, policy_tenure, tot_ape)
select  *, (total_ape_paid / tot_ape) paid_policy_ratio, 
        case when total_ape_paid>=tot_ape then 1 else 0 end paid_full_policy,
        (total_ape_paid / vip_ape) paid_vip_ratio,
        case when total_ape_paid>=vip_ape then 1 else 0 end paid_full_vip
from    pmt2
'''

paid_vip_df = sql_to_df(sql_pmt_string, 0, spark)

paid_vip_df.groupBy('vip_type').agg(
    F.countDistinct('cli_num').alias('number_vip_customers'),
    F.count('pol_num').alias('number_paid_policies'),
    F.sum('tot_ape').alias('total_APE'),
    F.sum('total_ape_paid').alias('total_APE_paid'),
    F.mean('paid_full_policy').alias('paid_policy_ratio'),
    F.mean('paid_vip_ratio').alias('paid_full_vip_ratio')
).display()

#display(paid_vip_df)
paid_vip_df.toPandas().to_csv('/dbfs/mnt/lab/vn/project/cpm/Adhoc/VIP_Customer_Care/VIP_Customers_Premium_Paid.csv', index=False, header=True)

# COMMAND ----------

sql_insrd_string = f'''
with profile as (
select  vip_typ_desc vip_type,
        pol.po_num, cli.cur_age po_age,
        pol.insrd_num, ins.cur_age insrd_age,
        pol.REL_TO_INSRD relationship
from    twrk_client_ape vip inner join
        tpolidm_mthend pol on vip.cli_num = pol.po_num and pol.po_num <> pol.insrd_num inner join
        tcustdm_mthend cli on pol.po_num = cli.cli_num inner join
        tcustdm_mthend ins on pol.insrd_num = ins.cli_num
where   1=1
  and   pol.pol_stat_cd in ('1','2','3','5','7','9')
  and   ins.cur_age < 14
)
select  vip_type,
        --relationship,
        count(distinct po_num) number_vip_customers,
        count(distinct insrd_num) number_vip_with_insrd_below_14,
        min(po_age) min_po_age, max(po_age) max_po_age,
        min(insrd_age) min_insrd_age, max(insrd_age) max_insrd_age
from    profile
group by vip_type--, relationship
'''

vip_insrd_df = sql_to_df(sql_insrd_string, 1, spark)
display(vip_insrd_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  vip_typ_desc vip_type,
# MAGIC         pol.po_num, cli.cur_age po_age,
# MAGIC         pol.insrd_num, to_date(ins.birth_dt) birth_dt, ins.cur_age insrd_age,
# MAGIC         pol.REL_TO_INSRD relationship
# MAGIC from    twrk_client_ape vip inner join
# MAGIC         tpolidm_mthend pol on vip.cli_num = pol.po_num inner join
# MAGIC         tcustdm_mthend cli on pol.po_num = cli.cli_num inner join
# MAGIC         tcustdm_mthend ins on pol.insrd_num = ins.cli_num
# MAGIC where   1=1
# MAGIC   and   pol.pol_stat_cd in ('1','2','3','5','7')
# MAGIC   and   ins.cur_age < 14
# MAGIC order by insrd_age desc
# MAGIC limit 10
