# Databricks notebook source
# MAGIC %md
# MAGIC # UCM Program - NEW
# MAGIC ### Data Initialization

# COMMAND ----------

pip install Unidecode

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load defined functions</strong>

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls, params and paths</strong>

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from unidecode import unidecode

# List of ABFSS paths
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/'
dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
rpt_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
lab_path = '/mnt/lab/vn/project/scratch/cseg_cltv/'
out_path = '/mnt/lab/vn/project/cpm/'

# List of tables
tbl_src1 = 'tagtdm_daily/'
tbl_src2 = 'loc_to_sm_mapping/'
tbl_src3 = 'orphpol/'
tbl_src4 = 'orphcus/'
tbl_src5 = 'tpolidm_daily/'
tbl_src6 = 'tcustdm_daily/'
tbl_src7 = 'loc_code_mapping/'
tbl_src8 = 'temp3/'

#snapshot_paths = [lab_path,casm_path,asm_path]
daily_paths = [ams_path,cas_path,dm_path,rpt_path,cpm_path,lab_path]
daily_files = [tbl_src1,tbl_src2,tbl_src3,tbl_src4,tbl_src5,tbl_src6,
               tbl_src7,tbl_src8,]

#last_mthend = datetime.strftime(datetime.now().replace(day=1) - timedelta(days=1), '%Y-%m-%d')

x = 0 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
last_mthend = last_day_of_x_months_ago.strftime('%Y-%m-%d')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load working tables</strong>

# COMMAND ----------

list_df = load_parquet_files(daily_paths,daily_files)

# COMMAND ----------

list_df['TCSEG_MTHEND'] = list_df.pop('temp3')
list_df['TCSEG_MTHEND'] = list_df['TCSEG_MTHEND'][list_df['TCSEG_MTHEND']['image_date']==last_mthend]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate temp views and result

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate temp view</strong>

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Reformat provinces</strong>

# COMMAND ----------

altenative_city_df = spark.sql("""
select distinct
        b.cli_num cli_num,
        c.prov_nm prov_nm
from    hive_metastore.vn_published_cas_db.tclient_addresses b left join
        hive_metastore.vn_published_cas_db.tprovinces c ON substr(b.zip_code,1,2) = c.prov_id
where   cli_num in (000)
    and c.prov_nm not in ('nan','city','Xem Đơn YCBH','City')                         
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong> Identify the date policy becoming unassigned</strong>

# COMMAND ----------

ucm_dt_df = spark.sql("""
select  pol_num
        ,max(to_date(trxn_dt)) ucm_dt     
from    hive_metastore.vn_published_cas_db.ttrxn_histories
where   (trxn_cd='POLCHG' and reasn_code='211' and trxn_desc like '%AGT_CODE is changed%') or
        (trxn_cd='ORPAGN' and reasn_code='406' and trxn_desc like '%Servicing agent changed%')
group by pol_num
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Identify the policies with claims last 6 months</strong>

# COMMAND ----------

claims_df = spark.sql(f"""
select distinct 
  tclaim.clm_id
, to_date(tclaim.clm_recv_dt) clm_sbmt_dt
, to_date(tclaim.clm_aprov_dt) clm_aprv_dt
, tclaim.clm_stat_code
, tclaim.clm_reasn_cd
, tclaim.plan_code
, tclaim.clm_code
, tclaim.clm_aprov_amt
, case 
    when tclaim.clm_stat_code = 'A' then tclaim.clm_aprov_amt 
    else 0 
  end as adj_aprov_amt 
, tclaim.clm_prvd_amt
, tclaim.pol_num
, tclaim.cli_num
from hive_metastore.vn_published_cas_db.tclaim_details tclaim      
where tclaim.clm_stat_code in ('A','D')
  and months_between('{last_mthend}', tclaim.clm_recv_dt) <= 6
""")

claims_df = claims_df.groupBy('pol_num') \
                    .agg(
                        F.sum(F.col('adj_aprov_amt')).cast('float').alias('clm_aprov_amt'),
                        F.count(F.col('clm_id')).alias('clm_cnt'),
                        F.max(F.col('clm_sbmt_dt')).alias('lst_clm_sbmt_dt'),
                        F.max(F.col('clm_aprv_dt')).alias('lst_clm_aprv_dt')
                    ) \
                    .select('pol_num','clm_aprov_amt','clm_cnt','lst_clm_sbmt_dt','lst_clm_aprv_dt')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Get all unassigned policies by SM's locations</strong>

# COMMAND ----------

altenative_city_df.createOrReplaceTempView('alternative_city')
ucm_dt_df.createOrReplaceTempView('ucm_dt')
claims_df.createOrReplaceTempView('claims')

ucm_sm_df = spark.sql(f"""
select 	distinct
		pol.po_num,
		ucm.pol_num,
		--cus.city cus_city,
		case when cus.city in ('nan','city','Xem Đơn YCBH','City') then alt.prov_nm 
			 when lower(cus.city) = 'hcm' then 'Hồ Chí Minh'
			 when lower(cus.city) = 'hn' then 'Hà Nội'
      		 else cus.city 	
     	end as cus_city,
		ucm.agt_code old_serv_code,
		ucm.loc_cd old_loc_cd,
		ucm.new_serv_code,
		ucm.cus_agt_rltnshp,
		pol.sa_code,
		nvl(loc.city, '') agt_city,
		agt.stat_cd,
		agt.loc_cd new_loc_cd,
		nvl(sm.manager_code_0,
  			nvl(sm.manager_code_1,
     			nvl(sm.manager_code_2,
        			nvl(sm.manager_code_3,
           				nvl(sm.manager_code_4,
               				nvl(sm.manager_code_5,
                   				nvl(sm.manager_code_6, 'Open'))))))) sm_code,
		sm.rh_name,
		agt.channel,
		ucm_dt.ucm_dt lst_ucm_dt,
		claims.clm_cnt,
  		claims.clm_aprov_amt,
		claims.lst_clm_sbmt_dt,
		claims.lst_clm_aprv_dt
from 	orphpol ucm inner join
		tpolidm_daily pol on ucm.pol_num=pol.pol_num inner join		
		claims on ucm.pol_num=claims.pol_num inner join
		tcustdm_daily cus on pol.po_num=cus.cli_num inner join
		tagtdm_daily agt on pol.sa_code=agt.agt_code left join
		loc_to_sm_mapping sm on agt.loc_cd=sm.loc_cd left join
		loc_code_mapping loc on sm.loc_cd=loc.loc_code left join
		alternative_city alt on cus.cli_num=alt.cli_num left join
		ucm_dt on ucm.pol_num=ucm_dt.pol_num
where	ucm.cus_agt_rltnshp like 'ORPHAN%'		-- Select only unassigned policies
	and	pol.pol_stat_cd in ('1','3','5')		-- Select only Premium-paying policies
	and nvl(cus.city,alt.prov_nm) is not null
""")

print('No of UCM with Claims records:', ucm_sm_df.count())
ucm_sm_df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store UCM data for later use</strong>

# COMMAND ----------

ucm_sm_df.write.mode('overwrite').parquet(f'{out_path}UCM/New/ucm_claims_data/')
ucm_sm_df = ucm_sm_df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Intermediate city data to align between customers' and agents'</strong>

# COMMAND ----------

ucm_sm_df['cus_city_new'] = ucm_sm_df['cus_city'].apply(lambda x: str(x).lower()) # Convert customers' city names to lower case
ucm_sm_df['agt_city_new'] = ucm_sm_df['agt_city'].apply(lambda x: str(x).lower()) # Convert customers' city names to lower case
ucm_sm_df['cus_city_new'] = ucm_sm_df['cus_city_new'].apply(lambda x: unidecode(x) if x is not None else x) # Strip off VNese accents

# COMMAND ----------

cus_city_list = ucm_sm_df.groupby('cus_city_new').po_num.nunique('no_clients')

cus_city_list

# COMMAND ----------

#cus_city_list.display()

# COMMAND ----------

agt_city_list = ucm_sm_df.groupby('agt_city_new').old_serv_code.nunique('no_agents')

agt_city_list

# COMMAND ----------

#agt_city_list.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Get Manupro agents and their capacity</strong>

# COMMAND ----------

#ucm_sm_df.createOrReplaceTempView('ucm_sm')
ucm_sm_df

#manupro_df = spark.sql(f"""      
#""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate result</strong>

# COMMAND ----------

#ucm_final_df = spark.sql(f"""    """)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Store result to parquet</strong>

# COMMAND ----------

#spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

#ucm_final_df.write.mode('overwrite').parquet(f'{out_path}UCM/New/')
