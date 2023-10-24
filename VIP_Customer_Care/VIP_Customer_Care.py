# Databricks notebook source
# MAGIC %md
# MAGIC # Analysis on VIP segments

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load all libls</strong>

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import calendar

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load params and paths</strong>

# COMMAND ----------

dm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'
alt_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'
pos_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/'

tbl_src1 = 'TCUSTDM_daily/'
tbl_src2 = 'TPOLIDM_MTHEND/'
tbl_src3 = 'TAGTDM_MTHEND/'
tbl_src4 = 'TCLIENT_ADDRESSES/'
tbl_src5 = 'tprovinces/'
tbl_src6 = 'CUS_RFM/'
tbl_src7 = 'tpos_collection/'
tbl_src8 = 'tcoverages/'
tbl_src9 = 'TPORIDM_MTHEND/'
tbl_src10 = 'tclaim_details/'
#tbl_src11 = 'customers_table_mthly/'

abfss_paths = [dm_path,cas_path,alt_path,pos_path]
parquet_files = [tbl_src1,tbl_src2,tbl_src3,tbl_src4,tbl_src5,
                 tbl_src6,tbl_src7,tbl_src8,tbl_src9,tbl_src10]

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
start_mthend = datetime(2022, 6, 30).strftime('%Y-%m-%d')
print(start_mthend, last_mthend)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load files into list of dfs</strong>

# COMMAND ----------

list_df = load_parquet_files(abfss_paths, parquet_files)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Add filters and conditions to dfs</strong>

# COMMAND ----------

list_df['TPOLIDM_MTHEND'] = list_df['TPOLIDM_MTHEND']\
            .withColumn('prev_due', F.when(F.col('pmt_mode') =='12', F.add_months(F.col('pd_to_dt'),-12))
                                     .when(F.col('pmt_mode') =='01',F.add_months(F.col('pd_to_dt'),-1))
                                     .when(F.col('pmt_mode') =='03',F.add_months(F.col('pd_to_dt'),-3))
                                     .when(F.col('pmt_mode') =='06',F.add_months(F.col('pd_to_dt'),-6)))\
            .withColumn('prev_due_lag2', F.when(F.col('pmt_mode') =='12', F.add_months(F.col('pd_to_dt'),-24))
                                     .when(F.col('pmt_mode') =='01',F.add_months(F.col('pd_to_dt'),-2))
                                     .when(F.col('pmt_mode') =='03',F.add_months(F.col('pd_to_dt'),-6))
                                     .when(F.col('pmt_mode') =='06',F.add_months(F.col('pd_to_dt'),-12)))

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate temp views</strong>

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Immediate and clean data</strong>

# COMMAND ----------

sql_string = """
select distinct
        b.cli_num cli_num,
        c.prov_nm prov_nm
from    tclient_addresses b left join
        tprovinces c on substr(b.zip_code,1,2) = c.prov_id and b.image_date = c.image_date
where   1=1
    and b.image_date = '{last_mthend}'
    and c.prov_nm not in ('nan','city','Xem Đơn YCBH','City')                         
"""

alt_city_DF = sql_to_df(sql_string, 1, spark)
#print(alt_city_DF.count())

# COMMAND ----------

alt_city_DF.createOrReplaceTempView('alternative_city')
sql_string = """
select 	pol.po_num,
		case when cus.city in ('nan','city','Xem Đơn YCBH','City') then alt.prov_nm 
			 when lower(cus.city) = 'hcm' then 'Hồ Chí Minh'
			 when lower(cus.city) = 'hn' then 'Hà Nội'
      		 else cus.city 	
     	end as cus_city,
		pol.tot_ape ape,
        case when floor(datediff(image_date, pol.pol_eff_dt)/365.25)>1 then 1 else 0
        end as 2yr_pol_ind,
        case when floor(datediff(image_date, pol.pol_iss_dt)/365.25)>9 then 1 else 0
        end as 10yr_pol_ind
        ,pol.image_date
        --,substr(to_date(pol.image_date),1,7) as rpt_mth
from 	tpolidm_mthend pol inner join
		tcustdm_daily cus on pol.po_num=cus.cli_num left join
		alternative_city alt on cus.cli_num=alt.cli_num
where	pol.pol_stat_cd in ('1','2','3','6','8')		-- Select only Premium-paying OR Pending policies
	and nvl(cus.city, alt.prov_nm) is not null
    and pol.tot_ape>0
    and image_date between '2022-05-31' and '{last_mthend}'
"""

cus_ape_raw_DF = sql_to_df(sql_string, 1, spark)

cus_ape_DF = cus_ape_raw_DF.groupBy(['image_date', 'po_num'])\
    .agg(
        F.sum('ape').cast('int').alias('tot_ape'),
        F.max('2yr_pol_ind').alias('f_2yr_pol'),
        F.sum('10yr_pol_ind').alias('10yr_pol_cnt')
    )\
    .orderBy(F.col('image_date').desc())

#cus_ape_DF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tag VIP segment definitions and customers features to all partitions past x years

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Identify VIP customers</strong>

# COMMAND ----------

cus_ape_DF = cus_ape_DF\
    .withColumn('vip_seg', 
                F.when(F.col('tot_ape')>=300000, "1. Elite")
                .when((F.col('tot_ape')>=150000) & (F.col('tot_ape')<300000), "2. Platinum")
                .when((F.col('tot_ape')>=65000) & (F.col('tot_ape')<150000), "3. Gold")
                .when((F.col('tot_ape')>=20000) & (F.col('tot_ape')<65000) & (F.col('10yr_pol_cnt')>=1), "4. Non-Tier")
                .otherwise("4. Non-Tier")
    )\
    .withColumn('current_ape_tier',
                F.when(F.col('tot_ape')<25000, '1. <25M')\
                .when((F.col('tot_ape')>=25000) & (F.col('tot_ape')<50000), '2. 25-50M')
                .when((F.col('tot_ape')>=50000) & (F.col('tot_ape')<65000), '3. 50-65M')
                .when((F.col('tot_ape')>=65000) & (F.col('tot_ape')<75000), '4. 65-75M')
                .when((F.col('tot_ape')>=75000) & (F.col('tot_ape')<85000), '5. 75-85M')
                .when((F.col('tot_ape')>=85000) & (F.col('tot_ape')<100000), '6. 85-100M')
                .when((F.col('tot_ape')>=100000) & (F.col('tot_ape')<150000), '7. 100-150M')
                .when((F.col('tot_ape')>=150000) & (F.col('tot_ape')<300000), '8. 150-300M')
                .otherwise('9. >300M')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Prepare for additional features</strong>

# COMMAND ----------

# DBTITLE 1,Claims after 2 years onboarding
# Get latest claim details
sql_string = """
select  po_num,
        clm_id,
        clm_eff_dt,
        clm_aprov_amt
from    tclaim_details clm inner join
        tpolidm_mthend pol on clm.pol_num=pol.pol_num and clm.image_date=pol.image_date
where   clm_stat_code in ('A')
    and clm_code in ('3','7','8','11','27','28','29')
    and clm.image_date = '{last_mthend}'
"""

claim_details = sql_to_df(sql_string, 1, spark)


# COMMAND ----------

# DBTITLE 1,Onboarding and subsequent policies
# Get latest coverage details
sql_string = """
select  distinct
        cvg.pol_num, cvg.cvg_typ, cvg.plan_code, cvg.vers_num, to_date(cvg.cvg_eff_dt) cvg_eff_dt, 
        cvg.cvg_prem, cvg.cvg_stat_cd, ins.ins_typ, cvg.image_date
from    tporidm_mthend cvg inner join
        tcoverages ins on cvg.pol_num=ins.pol_num and cvg.plan_code=ins.plan_code and 
        cvg.vers_num=ins.vers_num and cvg.cvg_typ=ins.cvg_typ
where   cvg.image_date='{last_mthend}'
    and cvg.plan_code not like 'EV%'
    and cvg.cvg_prem > 0 -- Exclude those already matured or no APE record retained
"""

coverage_details = sql_to_df(sql_string, 1, spark)

# Rank coverages by their effective dates
ranked_pol = coverage_details.alias('cvg')\
        .filter((F.col('cvg_stat_cd').isin(['6','8','A','N','R','X'])==False) & (F.col('cvg_eff_dt') <= last_mthend))\
        .join(list_df['TPOLIDM_MTHEND'].alias('pol'), on=['pol_num', 'image_date'], how='inner')\
        .withColumn('pol_rank', F.dense_rank().over(Window.partitionBy('po_num').orderBy(F.col('pol_eff_dt').asc())))\
        .select('pol.po_num',
                'pol_rank',
                'cvg.pol_num',
                F.to_date('pol_eff_dt').alias('pol_eff_dt'),
                'cvg.cvg_typ',
                'cvg.plan_code',
                (F.col('cvg.cvg_prem')*(F.col('pol.pmt_mode').cast('int'))/12).alias('ape').cast('int'),
                'cvg.cvg_eff_dt',
                'cvg.cvg_stat_cd'
        ).orderBy(['po_num', 'pol_rank']).distinct()

# Identify first policy issued
first_pol = ranked_pol.filter(F.col('pol_rank')==1)

# Identify customers at point-of-onboarding
first_cus = first_pol.groupBy('po_num')\
    .agg(
        F.count('pol_num').alias('no_pol_count'),
        F.min('pol_eff_dt').alias('frst_eff_dt'),
        F.min(F.year('pol_eff_dt')).alias('frst_eff_yr'),
        F.sum('ape').alias('frst_ape')
    )

# Policies (base/rider) acquired after Onboarding
additional_pol = ranked_pol.filter(F.col('pol_rank')>1)
#additional_pol.limit(50).display()

# COMMAND ----------

# Merge the two dataframes
cus_ape_all = first_cus.join(
    additional_pol,
    on='po_num',
    how='left').join(
    claim_details,
    on='po_num',
    how='left'
    )

cus_ape_all.createOrReplaceTempView('cus_ape_all')

# COMMAND ----------

# Derive the additional statistical fields
sql_string = """
select  po_num, no_pol_count, frst_eff_dt, substr(frst_eff_dt,1,7) as frst_rpt_mth, frst_eff_yr, frst_ape,
        count(distinct case when floor(datediff(pol_eff_dt, frst_eff_dt)/365.25) <= 2 and cvg_typ='B' then pol_num end)
        as new_pol_next_2yr,
        sum(case when floor(datediff(pol_eff_dt, frst_eff_dt)/365.25) <= 2 and cvg_typ='B' then ape else 0 end)
        as new_pol_ape_next_2yr,
        count(case when floor(datediff(cvg_eff_dt, frst_eff_dt)/365.25) <= 2 and cvg_typ<>'B' then pol_num end)
        as new_rider_next_2yr,
        sum(case when floor(datediff(cvg_eff_dt, frst_eff_dt)/365.25) <= 2 and cvg_typ<>'B' then ape else 0 end)
        as new_rider_ape_next_2yr,
        count(case when floor(datediff(clm_eff_dt, frst_eff_dt)/365.25) <= 2 then clm_id end)
        as clm_next_2yr,
        sum(case when floor(datediff(clm_eff_dt, frst_eff_dt)/365.25) <= 2 then clm_aprov_amt else 0 end)
        as clm_amt_next_2yr
from    cus_ape_all
group by
        po_num, no_pol_count, frst_eff_dt, frst_eff_yr, frst_ape
"""

cus_ape_all = sql_to_df(sql_string, 0, spark)

#print(cus_ape_all.count())

# COMMAND ----------

#cus_ape_all.filter(F.col('po_num')=='2803270918').display()

# COMMAND ----------

# Add tiers to Onboarding and sub-sequent sales
cus_ape_tier_DF = cus_ape_all\
    .withColumn('onb_yr_tier', 
                F.when(F.col('frst_eff_yr')<2018, '1. <2018')
                .when(F.col('frst_eff_yr')==2018, '2. 2018')
                .when(F.col('frst_eff_yr')==2019, '3. 2019')
                .when(F.col('frst_eff_yr')==2020, '4. 2020')
                .when(F.col('frst_eff_yr')==2021, '5. 2021')
                .when(F.col('frst_eff_yr')==2022, '6. 2022')
                .otherwise('7. 2023')
    )\
    .withColumn('onb_ape_tier',
                F.when(F.col('frst_ape')<25000, '1. <25M')\
                .when((F.col('frst_ape')>=25000) & (F.col('frst_ape')<50000), '2. 25-50M')
                .when((F.col('frst_ape')>=50000) & (F.col('frst_ape')<65000), '3. 50-65M')
                .when((F.col('frst_ape')>=65000) & (F.col('frst_ape')<75000), '4. 65-75M')
                .when((F.col('frst_ape')>=75000) & (F.col('frst_ape')<85000), '5. 75-85M')
                .when((F.col('frst_ape')>=85000) & (F.col('frst_ape')<100000), '6. 85-100M')
                .when((F.col('frst_ape')>=100000) & (F.col('frst_ape')<150000), '7. 100-150M')
                .when((F.col('frst_ape')>=150000) & (F.col('frst_ape')<300000), '8. 150-300M')
                .otherwise('9. >300M')
    )\
    .withColumn('new_pol_next_2yr_tier',
                F.when(F.col('new_pol_next_2yr')==0, '1. 0')
                .when(F.col('new_pol_next_2yr')==1, '2. 1')
                .when(F.col('new_pol_next_2yr')==2, '3. 2')
                .when(F.col('new_pol_next_2yr')==3, '4. 3')
                .otherwise('5. 4+')
    )\
    .withColumn('new_pol_ape_next_2yr_tier',
                F.when(F.col('new_pol_ape_next_2yr')<25000, '1. <25M')
                .when((F.col('new_pol_ape_next_2yr')>=25000) & (F.col('new_pol_ape_next_2yr')<50000), '2. 25-50M')
                .when((F.col('new_pol_ape_next_2yr')>=50000) & (F.col('new_pol_ape_next_2yr')<65000), '3. 50-65M')
                .when((F.col('new_pol_ape_next_2yr')>=65000) & (F.col('new_pol_ape_next_2yr')<75000), '4. 65-75M')
                .when((F.col('new_pol_ape_next_2yr')>=75000) & (F.col('new_pol_ape_next_2yr')<85000), '5. 75-85M')
                .when((F.col('new_pol_ape_next_2yr')>=85000) & (F.col('new_pol_ape_next_2yr')<100000), '6. 85-100M')
                .when((F.col('new_pol_ape_next_2yr')>=100000) & (F.col('new_pol_ape_next_2yr')<150000), '7. 100-150M')
                .when((F.col('new_pol_ape_next_2yr')>=150000) & (F.col('new_pol_ape_next_2yr')<300000), '8. 150-300M')
                .otherwise('9. >300M')
    )\
    .withColumn('new_rider_next_2yr_tier',
                F.when(F.col('new_rider_next_2yr')==0, '1. 0')
                .when(F.col('new_rider_next_2yr')==1, '2. 1')
                .when(F.col('new_rider_next_2yr')==2, '3. 2')
                .when(F.col('new_rider_next_2yr')==3, '4. 3')
                .otherwise('5. 4+')
    )\
    .withColumn('new_rider_ape_next_2yr_tier',
                F.when(F.col('new_rider_ape_next_2yr')<5000, '1. <5M')
                .when((F.col('new_rider_ape_next_2yr')>=5000) & (F.col('new_rider_ape_next_2yr')<10000), '2. 5-10M')
                .when((F.col('new_rider_ape_next_2yr')>=10000) & (F.col('new_rider_ape_next_2yr')<15000), '3. 10-15M')
                .when((F.col('new_rider_ape_next_2yr')>=15000) & (F.col('new_rider_ape_next_2yr')<25000), '4. 15-25M')
                .otherwise('6. >25M')
    )\
    .withColumn('clm_next_2yr_tier',
                F.when(F.col('clm_next_2yr')==0, '1. 0')
                .when(F.col('clm_next_2yr')==1, '2. 1')
                .when(F.col('clm_next_2yr')==2, '3. 2')
                .when(F.col('clm_next_2yr')==3, '4. 3')
                .otherwise('5. 4+')
    )\
    .withColumn('clm_amt_next_2yr_tier',
                F.when(F.col('clm_amt_next_2yr')<5000, '1. <5M')
                .when((F.col('clm_amt_next_2yr')>=5000) & (F.col('clm_amt_next_2yr')<10000), '2. 5-10M')
                .when((F.col('clm_amt_next_2yr')>=10000) & (F.col('clm_amt_next_2yr')<15000), '3. 10-15M')
                .when((F.col('clm_amt_next_2yr')>=15000) & (F.col('clm_amt_next_2yr')<25000), '4. 15-25M')
                .otherwise('6. >25M')
    )

# COMMAND ----------

# Add VIP tier movement
vip_move_DF = cus_ape_DF.withColumn(
    'vip_move_ind',
    F.when(F.col('vip_seg') > F.lag(F.col('vip_seg')).over(Window.partitionBy(F.col('po_num')).orderBy(F.col('image_date'))), 1)
    .when(F.col('vip_seg') < F.lag(F.col('vip_seg')).over(Window.partitionBy(F.col('po_num')).orderBy(F.col('image_date'))), -1)
    .otherwise(0))\
    .select(
    'image_date',
    'po_num',
    'vip_seg',
    'vip_move_ind'
)

#vip_move_DF.limit(50).display()

# COMMAND ----------

# DBTITLE 1,Premium payment
# PO payments
sql_string = """
select  pol.pol_num
		,pol.po_num
		,pol.pd_to_dt
		,pol.prev_due
		,min(datediff(transaction_date,prev_due)) as days_diff
        ,substr(pol.image_date,1,7) as rpt_mth
from 	tpolidm_mthend pol left join 
		tpos_collection pos on pol.pol_num = pos.policy_number 
            and pol.image_date=pos.image_date
			and pos.transaction_date between add_months(pol.prev_due,-2) and add_months(pol.prev_due,2)
where  transaction_date is not null 
group by pol.pol_num
		,pol.po_num
		,pol.pd_to_dt
		,pol.prev_due
        ,substr(pol.image_date,1,7)
"""
base_pmt_DF = sql_to_df(sql_string, 1, spark)

base_pmt_DF.createOrReplaceTempView("base_pmt")

sql_string = """
select 	*
		,case when days_diff >= -60 and days_diff <-30   then '1. > 30 days before due'
			  when days_diff >= -30 and days_diff <0   then '2. 1-30days before due' 
			  when days_diff == 0 then '3. Same day due' 
			  when days_diff >= 1 and days_diff <=30 then '4. 1-30 days after due' 
			  when days_diff >= 30 then '5. >30 days after due' 
			  else '6. No payment recorded within 60 days' end as payment1
		--Indicators
		,case when days_diff >= -60 and days_diff <-30   then 1 else 0 end as f_30_days_before_due
		,case when days_diff >= -30 and days_diff <0   then 1 else 0 end as f_l30days_before_due
		,case when days_diff == 0 then 1 else 0 end as f_same_day_due
		,case when days_diff >= 1 and days_diff <=30 then 1 else 0 end as f_l30_days_after_due
		,case when days_diff >= 30 then 1 else 0 end as f_30_days_after_due
		,case when days_diff is null then 1 else 0 end as f_no_payment_rec_60_days
from	base_pmt
"""
base_pmt_ind_DF = sql_to_df(sql_string, 0, spark)

#Create Indicators
prem_payment_DF = base_pmt_ind_DF.groupby(['rpt_mth', 'po_num'])\
    .agg(F.max('f_30_days_before_due').alias('f_30_days_before_due'),
         F.max('f_l30days_before_due').alias('f_l30days_before_due'),
         F.max('f_same_day_due').alias('f_same_day_due'),
         F.max('f_l30_days_after_due').alias('f_l30_days_after_due'),
		 F.max('f_30_days_after_due').alias('f_30_days_after_due'),
		 F.max('f_no_payment_rec_60_days').alias('f_no_payment_rec_60_days'),
		 F.countDistinct(F.when(F.col("payment1") == "f_30_days_before_due" ,F.col("pol_num"))).alias('k_30_days_before_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_l30days_before_due" ,F.col("pol_num"))).alias('k_l30days_before_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_same_day_due" ,F.col("pol_num"))).alias('k_same_day_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_l30_days_after_due" ,F.col("pol_num"))).alias('k_l30_days_after_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_30_days_after_due" ,F.col("pol_num"))).alias('k_30_days_after_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_no_payment_rec_60_days" ,F.col("pol_num"))).alias('k_no_payment_rec_60_days')
   )

# COMMAND ----------

# Get channel
channelDF = spark.read.parquet('abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/ORC_ACID_to_Parquet_Conversion/apps/hive/warehouse/vn_processing_datamart_temp_db.db/customers_table_mthly/')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Add features to VIP customers</strong>

# COMMAND ----------

cus_ape_DF.createOrReplaceTempView('vip_customers')
vip_move_DF.createOrReplaceTempView('vip_move')
prem_payment_DF.createOrReplaceTempView('prem_payment')
cus_ape_tier_DF.createOrReplaceTempView('cus_ape_tier')
channelDF.createOrReplaceTempView('customers_table_mthly')

sql_string = """
select  substr(vip.image_date,1,7) as rpt_mth,
        vip.po_num,
        vip.tot_ape,
        vip.current_ape_tier,
        vip.f_2yr_pol,
        vip.10yr_pol_cnt,
        vip.vip_seg,
        nvl(old_vip.vip_move_ind,0) as vip_move_ind,
        /*case when vip.cus_city in ('Hồ Chí Minh', 'Hà Nội', 'Hải Phòng', 'Đà Nẵng', 'Cần Thơ')
             then vip.cus_city
             else 'Khác'
        end city_seg,*/
        f_new_pur_12m,
        f_lapsed_12m,
        f_clm_cnt_12m,
        a_total_clm_amt_12m,
        nvl(f_30_days_before_due,0) as f_30_days_before_due,
        nvl(f_l30days_before_due,0) as f_l30days_before_due,
        nvl(f_same_day_due,0) as f_same_day_due,
        nvl(f_l30_days_after_due,0) as f_l30_days_after_due,
        nvl(f_30_days_after_due,0) as f_30_days_after_due,
        nvl(f_no_payment_rec_60_days,0) as f_no_payment_rec_60_days,
        case when frst_rpt_mth=substr(vip.image_date,1,7) then 1 else 0 end as f_onb_same_mth,
        frst_eff_yr,
        onb_yr_tier,
        frst_ape,
        onb_ape_tier,
        new_pol_next_2yr,
        new_pol_next_2yr_tier,
        new_pol_ape_next_2yr,
        new_pol_ape_next_2yr_tier,
        new_rider_next_2yr,
        new_rider_next_2yr_tier,
        new_rider_ape_next_2yr,
        new_rider_ape_next_2yr_tier,
        clm_next_2yr,
        clm_next_2yr_tier,
        clm_amt_next_2yr,
        clm_amt_next_2yr_tier,
        case when chnl.channel not in ('Agency','Banca') then 'Other' else chnl.channel end as channel,
        case when vip.vip_seg not in ('5. Silver', '6. Non-VIP') then 1 else 0
        end as target
from    vip_customers vip left join
        vip_move old_vip on vip.po_num=old_vip.po_num and vip.image_date=old_vip.image_date left join
        cus_rfm rfm on vip.po_num=rfm.po_num and substr(vip.image_date,1,7)=rfm.monthend_dt left join
        prem_payment prem on vip.po_num=prem.po_num and substr(vip.image_date,1,7)=prem.rpt_mth left join
        cus_ape_tier ape on vip.po_num=ape.po_num left join
        customers_table_mthly chnl on vip.po_num=chnl.po_num and vip.image_date=chnl.reporting_date
where   substr(vip.image_date,1,7)>='2022-06'
"""

cus_all_raw_DF = sql_to_df(sql_string, 1, spark)

vip_cus_raw_DF = cus_all_raw_DF.filter(~F.col('vip_seg').isin(['5. Silver', '9. Non-Tier']))
#vip_cus_raw_DF.count()
#vip_cus_raw_DF.limit(50).display()

# COMMAND ----------

#vip_cus_raw_DF.groupBy(['rpt_mth', 'vip_seg', 'f_2yr_pol', 'f_new_pur_12m', 'f_lapsed_12m',
#                        'f_clm_cnt_12m', 'f_30_days_after_due',
#                        ])\
#    .agg(
#        F.countDistinct(F.col('po_num')).alias('no_vip_cus')
#    ).display()
cus_all_raw_DF.groupBy(['rpt_mth', 'channel', 'vip_seg', 'vip_move_ind', 'current_ape_tier', 'onb_yr_tier', 'f_onb_same_mth',
                        'new_pol_next_2yr_tier', 'new_rider_next_2yr_tier', 'f_2yr_pol', 'f_lapsed_12m',
                        'f_30_days_after_due',
                        ])\
    .agg(
        F.countDistinct(F.col('po_num')).alias('no_vip_cus')
    )\
    .where(
        (F.col('channel').isNotNull()) &
        (F.col('vip_seg').isNotNull())
    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Different slices for analysis

# COMMAND ----------

#cus_all_raw_DF.groupBy(['rpt_mth', 'vip_seg', 'f_onb_same_mth', 'current_ape_tier', 'onb_ape_tier',
                        #'new_pol_next_2yr_tier', 'new_rider_next_2yr_tier', 'f_2yr_pol', 'f_lapsed_12m',
                        #'f_30_days_after_due',
#                        ])\
#    .agg(
#        F.countDistinct(F.col('po_num')).alias('no_vip_cus')
#    )\
#    .where(
#        (F.col('vip_seg').isNotNull()) &
#        (F.col('f_onb_same_mth')==1)
#    ).display()

# COMMAND ----------

vip_cus_DF = vip_cus_raw_DF.groupBy(['rpt_mth', 'channel', 'vip_seg', 'vip_move_ind', 'current_ape_tier', 'onb_yr_tier', 'onb_ape_tier',
                                     'new_pol_next_2yr_tier', 'new_pol_ape_next_2yr_tier', 'new_rider_next_2yr_tier', 'new_rider_ape_next_2yr_tier',
                                     'f_2yr_pol', 'f_lapsed_12m', 'f_30_days_after_due',
                                     ])\
    .agg(
        F.countDistinct(F.col('po_num')).alias('no_vip_cus')
    )\
    .where(
        (F.col('channel').isNotNull()) &
        (F.col('vip_seg').isNotNull())
    )

#vip_cus_DF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Random Classification (Test)

# COMMAND ----------

#from sklearn.model_selection import train_test_split
#from sklearn.preprocessing import LabelEncoder
#from sklearn.ensemble import RandomForestClassifier
#import pandas as pd
