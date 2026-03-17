# Databricks notebook source
# MAGIC %md
# MAGIC ### Source file: MOVE subsequent_sales - Mmm YY.xlsx (provided by NGUYEN DINH HOANG HA)
# MAGIC <strong> Extract data from the 3 columns (G: New CLient ID, and H: New MOVE Key, D: MOVE activation) and store as 'client_id', 'movekey' and 'activation_dt' in <i>"\2023\MOVE\Rawdata\move_active_users.csv"</i></strong>
# MAGIC <br><strong> Upload "move_active_users.csv" to <i>"/mnt/lab/vn/project/scratch/"</i></strong>
# MAGIC
# MAGIC <strong>Key note: </strong><i>Make sure date format as "yyyy-mm-dd"</i>

# COMMAND ----------

# Import datetime so that datetime and timedelta can be used
import datetime

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType


# COMMAND ----------

rpt_mth = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)
rpt_mth = rpt_mth.strftime('%y%m')
#rpt_mth = '2311'
print(rpt_mth)

lab_path = '/mnt/lab/vn/project/'
filename = 'scratch/move_active_users.csv'

file_schema = StructType([
    StructField('client_id', StringType(), True),
    StructField('movekey', StringType(), True),
    StructField('activation_dt', DateType(), True),
])

move_active_users = spark.read.option('header', True).csv(f"{lab_path}{filename}", schema=file_schema)
#move_active_users = move_active_users.withColumn('activation_dt', to_date(move_active_users['activation_dt'], 'dd/MM/yyyy'))
#move_active_users.limit(10).display()


# COMMAND ----------

datamart_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
tpolidm_path = 'TPOLIDM_DAILY/'

tpolidm = spark.read.parquet(f"{datamart_path}{tpolidm_path}")

# COMMAND ----------

move_active_users.createOrReplaceTempView('move_active_users')
tpolidm.createOrReplaceTempView('tpolidm_daily')

result = spark.sql("""
with sub_sales as (
  select po_num, 1 as sub_sales	-- each policy met criteria counted as 1
  from   move_active_users mov inner join
  		 tpolidm_daily pol on mov.client_id=pol.po_num
  where	 activation_dt < to_date(frst_iss_dt) -- check if policy issued after activation date
	and	 pol_stat_cd not in ('8','A','N','R','X') -- exclude Nottaken and Rejects
),
lst_sales as (
  select po_num, to_date(max(pol_iss_dt)) last_iss_dt -- retrieve date of last issued policy
  from   tpolidm_daily
  where  pol_stat_cd not in ('8','A','N','R','X') -- exclude Nottaken and Rejects
  group by po_num
),
actv_sts as (
  select  po_num, 'Active' as status
  from    tpolidm_daily
  where   pol_stat_cd in ('1','2','3','5','7','9')
  group by
          po_num
)
select  client_id,
        movekey,
        min(nvl(actv_sts.status, 'Inactive')) as status,
		    sum(nvl(sub_sales,0)) as subsequent_sales,
		    max(last_iss_dt) last_iss_dt
from	move_active_users mov left join
		sub_sales on mov.client_id=sub_sales.po_num left join
		lst_sales on mov.client_id=lst_sales.po_num left join
    actv_sts  on mov.client_id=actv_sts.po_num
group by client_id,
		movekey                
""")

result.toPandas().to_csv(f'/dbfs{lab_path}digital_kpi/MOVE/MOVE subsequent sales result_{rpt_mth}.csv', index=False)
