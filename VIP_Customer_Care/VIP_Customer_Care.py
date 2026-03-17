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
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load params and paths</strong>

# COMMAND ----------

dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'
alt_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'
pos_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
lab_path = '/mnt/lab/vn/project/'

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

# Set the number of months going backward
x = 1  # Replace 0 with the desired number of months (0 being the last month-end)
y = 12 # Number of months to be captured

# Calculate the last month-end
current_date = pd.Timestamp.now()
last_monthend = current_date - pd.DateOffset(days=current_date.day)
last_monthend = last_monthend - pd.DateOffset(months=x)
last_monthend = last_monthend + pd.offsets.MonthEnd(0)
max_date_str = last_monthend.strftime('%Y-%m-%d')

# Calculate the last 12 months month-end
last_12m_monthend = last_monthend - pd.DateOffset(months=y)
last_12m_monthend = last_12m_monthend + pd.offsets.MonthEnd(0)
min_date_str = last_12m_monthend.strftime('%Y-%m-%d')

yyyymm_max = max_date_str[:4] + max_date_str[5:7]
yyyymm_min = min_date_str[:4] + min_date_str[5:7]
print(f"From: {min_date_str} to: {max_date_str}, yyyymm_min: {yyyymm_min}, yyyymm_max: {yyyymm_max}")

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
    and b.image_date = '{max_date_str}'
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
    and image_date between '{min_date_str}' and '{max_date_str}'
"""

cus_ape_raw_DF = sql_to_df(sql_string, 1, spark)

cus_ape_DF = cus_ape_raw_DF.groupBy(['image_date', 'po_num', 'cus_city'])\
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
                F.when(F.col('tot_ape')>=300000, "1. Diamond")
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

#cus_ape_DF.groupBy("image_date")\
#    .agg(
#        F.count(F.col("po_num")).alias("po_count")
#    ).orderBy(F.desc("image_date")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Prepare for additional features

# COMMAND ----------

# MAGIC %md
# MAGIC <strong><i>Claims after 2 years onboard</strong>

# COMMAND ----------

# Get latest claim details
sql_string = """
select  po_num,
        clm_id,
        clm_eff_dt,
        clm_aprov_amt
from    tclaim_details clm inner join
        tpolidm_mthend pol on clm.pol_num=pol.pol_num and clm.image_date=pol.image_date
where   clm_stat_code in ('A')
    and clm_code in (3,7,11,27,28,29,36,38,50,51)
    and clm.image_date = '{max_date_str}'
"""

claim_details = sql_to_df(sql_string, 1, spark)


# COMMAND ----------

# MAGIC %md
# MAGIC <strong><i>Onboarding and subsequent policies</strong>

# COMMAND ----------

# Get latest coverage details
sql_string = """
select  distinct
        cvg.pol_num, cvg.cvg_typ, cvg.plan_code, cvg.vers_num, to_date(cvg.cvg_eff_dt) cvg_eff_dt, 
        cvg.cvg_prem, cvg.cvg_stat_cd, ins.ins_typ, cvg.image_date
from    tporidm_mthend cvg inner join
        tcoverages ins on cvg.pol_num=ins.pol_num and cvg.plan_code=ins.plan_code and 
        cvg.vers_num=ins.vers_num and cvg.cvg_typ=ins.cvg_typ
where   cvg.image_date='{max_date_str}'
    and cvg.plan_code not like 'EV%'
    and cvg.cvg_prem > 0 -- Exclude those already matured or no APE record retained
"""

coverage_details = sql_to_df(sql_string, 1, spark)

# Rank coverages by their effective dates
ranked_pol = coverage_details.alias('cvg')\
        .filter((F.col('cvg_stat_cd').isin(['6','8','A','N','R','X'])==False) & (F.col('cvg_eff_dt') <= max_date_str))\
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
cus_ape_tmp = first_cus.join(
    additional_pol, on='po_num', how='left').join(
    claim_details,  on='po_num', how='left')

#cus_ape_tmp.createOrReplaceTempView('cus_ape_all')

# COMMAND ----------

# Define a function to calculate the year difference
def calculate_year_difference(date1, date2):
    return F.floor(F.datediff(date1, date2) / 365.25)

# Apply the function to the DataFrame
cus_ape_tmp = cus_ape_tmp.withColumn("year_diff_pol", calculate_year_difference(cus_ape_tmp["pol_eff_dt"], cus_ape_tmp["frst_eff_dt"])) \
                         .withColumn("year_diff_cvg", calculate_year_difference(cus_ape_tmp["cvg_eff_dt"], cus_ape_tmp["frst_eff_dt"])) \
                         .withColumn("year_diff_clm", calculate_year_difference(cus_ape_tmp["clm_eff_dt"], cus_ape_tmp["frst_eff_dt"]))

# Apply the optimized SQL query
cus_ape_tmp.createOrReplaceTempView("cus_ape_temp")
sql_string = """
SELECT
    po_num,
    no_pol_count,
    frst_eff_dt,
    substr(frst_eff_dt, 1, 7) AS frst_rpt_mth,
    frst_eff_yr,
    frst_ape,
    COUNT(DISTINCT CASE WHEN datediff(pol_eff_dt,frst_eff_dt)>0 and cvg_typ='B' THEN pol_num END) AS new_pol,
    SUM(DISTINCT CASE WHEN datediff(pol_eff_dt,frst_eff_dt)>0 and cvg_typ='B' THEN ape else 0 END) AS new_pol_ape,
    COUNT(DISTINCT CASE WHEN year_diff_pol <= 2 AND cvg_typ='B' THEN pol_num END) AS new_pol_next_2yr,
    COUNT(DISTINCT CASE WHEN year_diff_pol > 2 AND cvg_typ='B' THEN pol_num END) AS new_pol_after_2yr,
    SUM(CASE WHEN year_diff_pol <= 2 AND cvg_typ='B' THEN ape ELSE 0 END) AS new_pol_ape_next_2yr,
    SUM(CASE WHEN year_diff_pol > 2 AND cvg_typ='B' THEN ape ELSE 0 END) AS new_pol_ape_after_2yr,
    COUNT(CASE WHEN year_diff_cvg <= 2 AND cvg_typ<>'B' THEN pol_num END) AS new_rider_next_2yr,
    COUNT(CASE WHEN year_diff_cvg > 2 AND cvg_typ<>'B' THEN pol_num END) AS new_rider_after_2yr,
    SUM(CASE WHEN year_diff_cvg <= 2 AND cvg_typ<>'B' THEN ape ELSE 0 END) AS new_rider_ape_next_2yr,
    SUM(CASE WHEN year_diff_cvg > 2 AND cvg_typ<>'B' THEN ape ELSE 0 END) AS new_rider_ape_after_2yr,
    COUNT(CASE WHEN year_diff_clm <= 2 THEN clm_id END) AS clm_next_2yr,
    SUM(CASE WHEN year_diff_clm <= 2 THEN clm_aprov_amt ELSE 0 END) AS clm_amt_next_2yr
FROM
    cus_ape_temp
GROUP BY
    po_num, no_pol_count, frst_eff_dt, frst_eff_yr, frst_ape
"""

cus_ape_all = sql_to_df(sql_string, 0, spark)

# Add the "upsell_ind" and "upsell_base_ind" columns
cus_ape_all = cus_ape_all.withColumn("upsell_ind", 
    F.when((F.col("new_pol_next_2yr") > 0) | (F.col("new_pol_after_2yr") > 0) | (F.col("new_rider_next_2yr") > 0) | (F.col("new_rider_after_2yr") > 0), 1).otherwise(0)
)

cus_ape_all = cus_ape_all.withColumn("upsell_base_ind", 
    F.when((F.col("new_pol_next_2yr") > 0) | (F.col("new_pol_after_2yr") > 0), 1).otherwise(0)
)

# Display all customers and those with upsell
#cus_ape_all.agg(
#        F.count(F.col("po_num")).alias("no_rows"),
#        F.countDistinct(F.col("po_num")).alias("no_po"),
#        F.sum(F.when(F.col("upsell_base_ind")==1, F.lit(1)).otherwise(0)).alias("no_po_upsell")
#        ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong><i>Additional features</strong>

# COMMAND ----------

lapse = spark.read.option("header",True).csv("/mnt/lab/vn/project/analytics_datamart/Phu_Le_Quang_Data/mapping files/Data Persistency.csv")
lapse_filtered = lapse.filter(F.col("Cvg_Status") == 'B')

# Define a UDF to extract the year from the yyyymm integer value
extract_year = F.udf(lambda x: int(str(x)[:4]) if x != 0 else 0)

lapse_filtered = lapse_filtered.withColumn("lapse_year", 
                                           F.greatest(extract_year("Reporting_Period_M4"),
                                                      extract_year("Reporting_Period_M13"), 
                                                      extract_year("Reporting_Period_M25"), 
                                                      extract_year("Reporting_Period_M37")))\
                               .withColumn("IF_duration_year", F.floor(F.col("IF_duration")/12))

# Group persistency policies by Client level
lapse_sum = lapse_filtered.groupBy("Client_number")\
    .agg(
        F.count(F.col("Policy_number")).alias("no_lapse_pols"),
        F.min(F.year(F.col("cvg_eff_dt"))).alias("eff_yr"),
        F.max(F.col("IF_duration")).alias("inf_mth"),
        F.max(F.col("IF_duration_year")).alias("inf_yr"),
        F.sum(F.col("APE")).alias("lapsed_ape"),
        F.max(F.col("Product_Group_Base_1")).alias("lapsed_prd"),
        F.max(F.col("lapse_year")).alias("lapse_yr")
    )
    
# Add the categorical columns to the "lapse_sum" DataFrame
lapse_sum = lapse_sum.withColumn("lapse_yr_tier", 
    F.when(F.col("inf_yr") <= 1, "PY1")
    .when((F.col("inf_yr") > 1) & (F.col("inf_yr") <= 2), "PY2")
    .when((F.col("inf_yr") > 2) & (F.col("inf_yr") <= 3), "PY3")
    .otherwise("PY4+")
)

lapse_sum = lapse_sum.withColumn("lapse_pol_tier",
    F.when(F.col("no_lapse_pols") < 2, "1 lapsed pol")
    .when((F.col("no_lapse_pols") >= 2) & (F.col("no_lapse_pols") < 3), "2 lapsed pols")
    .otherwise("3+ lapse pols")
)

lapse_sum = lapse_sum.withColumn("lapse_ape_tier",
    F.when(F.col('lapsed_ape') < 25000, '1. <25M')
    .when((F.col('lapsed_ape') >= 25000) & (F.col('lapsed_ape') < 50000), '2. 25-50M')
    .when((F.col('lapsed_ape') >= 50000) & (F.col('lapsed_ape') < 65000), '3. 50-65M')
    .when((F.col('lapsed_ape') >= 65000) & (F.col('lapsed_ape') < 75000), '4. 65-75M')
    .when((F.col('lapsed_ape') >= 75000) & (F.col('lapsed_ape') < 85000), '5. 75-85M')
    .when((F.col('lapsed_ape') >= 85000) & (F.col('lapsed_ape') < 100000), '6. 85-100M')
    .when((F.col('lapsed_ape') >= 100000) & (F.col('lapsed_ape') < 150000), '7. 100-150M')
    .when((F.col('lapsed_ape') >= 150000) & (F.col('lapsed_ape') < 300000), '8. 150-300M')
    .otherwise('9. >300M')
)
#lapse_sum.display()

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
    .withColumn('new_pol_ape_tier',
                F.when(F.col('new_pol_ape')<25000, '1. <25M')
                .when((F.col('new_pol_ape')>=25000) & (F.col('new_pol_ape')<50000), '2. 25-50M')
                .when((F.col('new_pol_ape')>=50000) & (F.col('new_pol_ape')<65000), '3. 50-65M')
                .when((F.col('new_pol_ape')>=65000) & (F.col('new_pol_ape')<75000), '4. 65-75M')
                .when((F.col('new_pol_ape')>=75000) & (F.col('new_pol_ape')<85000), '5. 75-85M')
                .when((F.col('new_pol_ape')>=85000) & (F.col('new_pol_ape')<100000), '6. 85-100M')
                .when((F.col('new_pol_ape')>=100000) & (F.col('new_pol_ape')<150000), '7. 100-150M')
                .when((F.col('new_pol_ape')>=150000) & (F.col('new_pol_ape')<300000), '8. 150-300M')
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
		,case when days_diff >= -30 and days_diff <0   then 1 else 0 end as f_l30_days_before_due
		,case when days_diff == 0 then 1 else 0 end as f_same_day_due
		,case when days_diff >= 1 and days_diff <30 then 1 else 0 end as f_l30_days_after_due
		,case when days_diff >= 30 then 1 else 0 end as f_30_days_after_due
		,case when days_diff is null then 1 else 0 end as f_no_payment_rec_60_days
from	base_pmt
"""
base_pmt_ind_DF = sql_to_df(sql_string, 0, spark)

#Create Indicators
prem_payment_DF = base_pmt_ind_DF.groupby(['rpt_mth', 'po_num'])\
    .agg(F.max('f_30_days_before_due').alias('f_30_days_before_due'),
         F.max('f_l30_days_before_due').alias('f_l30_days_before_due'),
         F.max('f_same_day_due').alias('f_same_day_due'),
         F.max('f_l30_days_after_due').alias('f_l30_days_after_due'),
		 F.max('f_30_days_after_due').alias('f_30_days_after_due'),
		 F.max('f_no_payment_rec_60_days').alias('f_no_payment_rec_60_days'),
		 F.countDistinct(F.when(F.col("payment1") == "f_30_days_before_due" ,F.col("pol_num"))).alias('k_30_days_before_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_l30_days_before_due" ,F.col("pol_num"))).alias('k_l30_days_before_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_same_day_due" ,F.col("pol_num"))).alias('k_same_day_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_l30_days_after_due" ,F.col("pol_num"))).alias('k_l30_days_after_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_30_days_after_due" ,F.col("pol_num"))).alias('k_30_days_after_due'),
		 F.countDistinct(F.when(F.col("payment1") == "f_no_payment_rec_60_days" ,F.col("pol_num"))).alias('k_no_payment_rec_60_days')
   )

# COMMAND ----------

# DBTITLE 1,New purchase in 2023
spark.read.parquet('/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TPOLICYS/').createOrReplaceTempView('tpolicys')
spark.read.parquet('/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLIENT_POLICY_LINKS/').createOrReplaceTempView('tclient_policy_links')
spark.read.parquet('/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS/').createOrReplaceTempView('tams_agents')
spark.read.parquet('/mnt/prod/Staging/Incremental/VN_PUBLISHED_CAMPAIGN_FILEBASED_DB/NBV_MARGIN_HISTORIES/').createOrReplaceTempView('nbv_margin_histories')
spark.read.parquet('/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/VN_PLAN_CODE_MAP/').createOrReplaceTempView('vn_plan_code_map')

newSales_df = spark.sql(f"""
select	tb2.cli_num,
        tb1.pol_num,
        tb1.plan_code_base,
        NVL(pln.nbv_factor_group,nbv.nbv_factor_group) as nbv_factor_group,
        TO_DATE(tb1.pol_iss_dt) as pol_iss_dt,
        tb1.agt_code,
        CASE when tb1.pmt_mode = '12' then 1 * tb1.mode_prem
             when tb1.pmt_mode = '06' then 2 * tb1.mode_prem
             when tb1.pmt_mode = '03' then 4 * tb1.mode_prem
             else 12 * tb1.mode_prem
        END as APE_Upsell,
        CASE when agt.loc_code is null then 
            (case when tb1.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_other_banks,nbv.nbv_margin_banca_other_banks),2) -- 'Banca'
                 when tb1.dist_chnl_cd in ('48') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel_affinity,nbv.nbv_margin_other_channel_affinity),2)--'Affinity'
                 when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
                 when tb1.dist_chnl_cd in ('05','06','07','34','36') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_dmtm,nbv.nbv_margin_dmtm),2)--'DMTM'
                 when tb1.dist_chnl_cd in ('09') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*-1.34,2)--'MI'
                 else ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) END) --'Unknown'
            when tb1.dist_chnl_cd in ('01', '02', '08', '50', '*') then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_agency,nbv.nbv_margin_agency),2)--'Agency'
            when agt.loc_code like 'TCB%' then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_tcb,nbv.nbv_margin_banca_tcb),2) --'TCB'
            when agt.loc_code like 'SAG%' then ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_banca_scb,nbv.nbv_margin_banca_scb),2) --'SCB'
            else ROUND(tb1.mode_prem*(12/CAST(pmt_mode as INT))*NVL(pln.nbv_margin_other_channel,nbv.nbv_margin_other_channel),2) 
        END as NBV_Upsell		
from    tpolicys tb1 inner join 
        tclient_policy_links tb2 on tb1.pol_num=tb2.pol_num left join
        tams_agents agt on tb1.wa_cd_1=agt.agt_code left join
        nbv_margin_histories pln on pln.plan_code=tb1.plan_code_base and 
                                    floor(months_between(tb1.pol_iss_dt,pln.effective_date)) between 0 and 2 left join
        vn_plan_code_map nbv on nbv.plan_code=tb1.plan_code_base
where   tb1.plan_code_base not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
    and tb2.link_typ = 'O'
    and tb2.rec_status='A'
    and tb1.pol_stat_cd not in ('6','8','A','N','R','X')
    and year(tb1.pol_iss_dt)=year('{max_date_str}')
""")

custSales_df = newSales_df.groupBy('cli_num')\
    .agg(
        F.count(F.col('pol_num')).cast('Int').alias('no_cc'),
        F.sum(F.col('APE_Upsell')).cast('double').alias('APE_Upsell'),
        F.sum(F.col('NBV_Upsell')).alias('NBV_Upsell')
    )\
    .withColumn('newSale_ind', F.lit('Y'))

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
lapse_sum.createOrReplaceTempView('lapse_sum')
channelDF.createOrReplaceTempView('customers_table_mthly')
custSales_df.createOrReplaceTempView('cust_sale')

sql_string = """
select  substr(vip.image_date,1,7) as rpt_mth,
        vip.po_num,
        vip.tot_ape,
        vip.current_ape_tier,
        vip.f_2yr_pol,
        vip.10yr_pol_cnt,
        vip.vip_seg,
        nvl(old_vip.vip_move_ind,0) as vip_move_ind,
        case when vip.cus_city in ('Hồ Chí Minh', 'Hà Nội', 'Hải Phòng', 'Đà Nẵng', 'Cần Thơ')
             then vip.cus_city
             else 'Khác'
        end city_seg,
        --f_new_pur_12m,
        --f_lapsed_12m,
        --f_clm_cnt_12m,
        --a_total_clm_amt_12m,
        nvl(f_30_days_before_due,0) as f_30_days_before_due,
        nvl(f_l30_days_before_due,0) as f_l30_days_before_due,
        nvl(f_same_day_due,0) as f_same_day_due,
        nvl(f_l30_days_after_due,0) as f_l30_days_after_due,
        nvl(f_30_days_after_due,0) as f_30_days_after_due,
        nvl(f_no_payment_rec_60_days,0) as f_no_payment_rec_60_days,
        case when frst_rpt_mth=substr(vip.image_date,1,7) then 1 else 0 end as f_onb_same_mth,
        frst_eff_yr,
        onb_yr_tier,
        frst_ape,
        onb_ape_tier,
        nvl(new_pol_ape,0) new_pol_ape,
        new_pol_ape_tier,
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
        case when vip.vip_seg <> '4. Non-Tier' then 1 else 0
        end as target,
        nvl(upsell_ind,0) as upsell_ind,
        nvl(upsell_base_ind,0) as upsell_base_ind,
        nvl(lapsed_ape,0) as lapsed_ape,
        nvl(lapse_yr_tier,'No Lapse') as lapse_yr_tier,
        nvl(lapse_pol_tier,0) as lapse_pol_tier,
        nvl(lapse_ape_tier,'1. <25M') as lapse_ape_tier
        /*nvl(no_cc,0) as no_cc,
        nvl(APE_UpSell,0) as ape_upsell,
        nvl(NBV_UpSell,0) as nbv_upsell*/
from    vip_customers vip left join
        vip_move old_vip on vip.po_num=old_vip.po_num and vip.image_date=old_vip.image_date left join
        cus_rfm rfm on vip.po_num=rfm.po_num and substr(vip.image_date,1,7)=rfm.monthend_dt left join
        prem_payment prem on vip.po_num=prem.po_num and substr(vip.image_date,1,7)=prem.rpt_mth left join
        cus_ape_tier ape on vip.po_num=ape.po_num left join
        lapse_sum lps on vip.po_num=lps.Client_number left join
        customers_table_mthly chnl on vip.po_num=chnl.po_num and vip.image_date=chnl.reporting_date --left join
        --cust_sale nsale on vip.po_num=nsale.cli_num
where   substr(vip.image_date,1,7)>=substr('{max_date_str}',1,7) --'2022-12'
"""

cus_all_raw_DF = sql_to_df(sql_string, 1, spark)

vip_cus_raw_DF = cus_all_raw_DF.filter(F.col('vip_seg') != '4. Non-Tier')
nonvip_cus_raw_DF = cus_all_raw_DF.filter(F.col('vip_seg') == '4. Non-Tier')
#vip_cus_raw_DF.count()
#vip_cus_raw_DF.limit(50).display()

vip_cus_raw_DF.groupBy("vip_seg")\
        .agg(F.count(F.col("po_num")).alias("no_cus"))\
        .orderBy("vip_seg")\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong> Store everything</strong>

# COMMAND ----------

#vip_cus_raw_DF.orderBy(F.desc('tot_ape')).limit(100).display()

cus_all_raw_DF.coalesce(1).write.mode('overwrite').option('partitionOverwriteMode', 'dynamic').partitionBy('rpt_mth').parquet('/mnt/lab/vn/project/cpm/Adhoc/VIP_Customer_Care/')

# COMMAND ----------

#cus_all_raw_DF.groupBy(['rpt_mth', 'channel', 'vip_seg', 'vip_move_ind', 'current_ape_tier', 'onb_yr_tier', 'f_onb_same_mth',
#                        'new_pol_next_2yr_tier', 'new_rider_next_2yr_tier', 'f_2yr_pol', 'f_lapsed_12m',
#                        'f_30_days_after_due',
#                        ])\
#    .agg(
#        F.countDistinct(F.col('po_num')).alias('no_vip_cus')
#    )\
#    .where(
#        (F.col('channel').isNotNull()) &
#        (F.col('vip_seg').isNotNull())
#    ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Different slices for analysis

# COMMAND ----------

vip_cus_pd = vip_cus_raw_DF\
    .filter(
        (F.col('channel').isNotNull()) &
        (F.col('vip_seg').isNotNull()))\
    .groupBy(['rpt_mth', 'channel', 'vip_seg', 'vip_move_ind', 'current_ape_tier', 'onb_yr_tier', 'onb_ape_tier',
               'upsell_ind', 'upsell_base_ind', 'new_pol_ape_tier', 'new_pol_ape_next_2yr_tier', 'lapse_yr_tier',
               'lapse_pol_tier', 'lapse_ape_tier'
               ])\
    .agg(
        F.countDistinct(F.col("po_num")).alias("no_vip_cus"),
        F.sum(F.col("new_pol_ape")/1000).alias("upsell_ape_mVND"),
        F.sum(F.col("lapsed_ape")/1000).alias("lapsed_ape_mVND"),
        F.sum(F.col("tot_ape")/1000).alias("total_ape_mVND"))\
    .toPandas()

#display(vip_cus_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Random Classification (Test)

# COMMAND ----------

vip_cus_pd.to_csv('/dbfs/mnt/lab/vn/project/cpm/Adhoc/VIP_Customer_Care.csv', index=False, header=True)
