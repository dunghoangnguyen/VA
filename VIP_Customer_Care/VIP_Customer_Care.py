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
from datetime import datetime, timedelta
import calendar

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load params and paths</strong>

# COMMAND ----------

dm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cas_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
alt_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'
pos_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/'

tbl_src1 = 'TCUSTDM_daily/'
tbl_src2 = 'TPOLIDM_MTHEND/'
tbl_src3 = 'TAGTDM_MTHEND/'
tbl_src4 = 'TCLIENT_ADDRESSES/'
tbl_src5 = 'tprovinces/'
tbl_src6 = 'CUS_RFM/'
tbl_src7 = 'tpos_collection/'

abfss_paths = [dm_path,cas_path,alt_path,pos_path]
parquet_files = [tbl_src1,tbl_src2,tbl_src3,
                 tbl_src4,tbl_src5,tbl_src6,tbl_src7]

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
print(last_mthend)

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
        tprovinces c on substr(b.zip_code,1,2) = c.prov_id
where   1=1
    and c.prov_nm not in ('nan','city','Xem Đơn YCBH','City')                         
"""

alt_city_DF = sql_to_df(sql_string, 0, spark)
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
        case when floor(datediff(image_date, pol.pol_iss_dt)/365.25)>9 then 1 else 0
        end as 10yr_pol_ind,
        substr(to_date(pol.image_date),1,7) as rpt_mth
from 	tpolidm_mthend pol inner join
		tcustdm_daily cus on pol.po_num=cus.cli_num left join
		alternative_city alt on cus.cli_num=alt.cli_num
where	pol.pol_stat_cd in ('1','2','3','6','8')		-- Select only Premium-paying OR Pending policies
	and nvl(cus.city, alt.prov_nm) is not null
    and pol.tot_ape>0
    and image_date between '2022-06-30' and '{last_mthend}'
"""

cus_ape_raw_DF = sql_to_df(sql_string, 1, spark)

cus_ape_DF = cus_ape_raw_DF.groupBy(['rpt_mth', 'cus_city', 'po_num'])\
    .agg(
        F.sum('ape').cast('int').alias('tot_ape'),
        F.sum('10yr_pol_ind').alias('10yr_pol_cnt')
    )\
    .orderBy(F.col('rpt_mth').desc())

#cus_ape_DF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Tag VIP segment definitions and customers features to all partitions past x years

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Identify VIP customers</strong>

# COMMAND ----------

cus_ape_DF = cus_ape_DF.withColumn('vip_seg', 
                                   F.when(F.col('tot_ape')>=300000, "f_vip_elite")
                                    .when((F.col('tot_ape')>150000) & (F.col('tot_ape')<300000), "f_vip_plat")
                                    .when((F.col('tot_ape')>=65000) & (F.col('tot_ape')<150000), "f_vip_gold")
                                    .when((F.col('tot_ape')>=20000) & (F.col('tot_ape')<65000) & (F.col('10yr_pol_cnt')>=1), "f_vip_silver")
                                    .otherwise("Others")
                                   )

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Add features to VIP customers</strong>

# COMMAND ----------

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

cus_ape_DF.createOrReplaceTempView('vip_customers')
prem_payment_DF.createOrReplaceTempView('prem_payment')

sql_string = """
select  vip.*,
        case when vip.cus_city in ('Hồ Chí Minh', 'Hà Nội', 'Hải Phòng', 'Đà Nẵng', 'Cần Thơ')
             then vip.cus_city
             else 'Khác'
        end city_seg,
        f_new_pur_1m,
        f_new_pur_3m,
        f_new_pur_6m,
        f_new_pur_12m,
        f_lapsed_1m,
        f_lapsed_3m,
        f_lapsed_6m,
        f_lapsed_12m,
        f_clm_cnt_1m,
        f_clm_cnt_3m,
        f_clm_cnt_6m,
        f_clm_cnt_12m,
        a_total_clm_amt_1m,
        a_total_clm_amt_3m,
        a_total_clm_amt_6m,
        a_total_clm_amt_12m,
        nvl(f_30_days_before_due,0) as f_30_days_before_due,
        nvl(f_l30days_before_due,0) as f_l30days_before_due,
        nvl(f_same_day_due,0) as f_same_day_due,
        nvl(f_l30_days_after_due,0) as f_l30_days_after_due,
        nvl(f_30_days_after_due,0) as f_30_days_after_due,
        nvl(f_no_payment_rec_60_days,0) as f_no_payment_rec_60_days,
        case when vip.vip_seg not in ('f_vip_silver', 'Others') then 1 else 0
        end as target
from    vip_customers vip inner join
        cus_rfm rfm on vip.po_num=rfm.po_num and vip.rpt_mth=rfm.monthend_dt left join
        prem_payment prem on vip.po_num=prem.po_num and vip.rpt_mth=prem.rpt_mth
"""

cus_all_raw_DF = sql_to_df(sql_string, 0, spark)

vip_cus_raw_DF = cus_all_raw_DF.filter(~F.col('vip_seg').isin(['f_vip_silver', 'Others']))
vip_cus_raw_DF.count()
#vip_cus_raw_DF.display()

# COMMAND ----------

vip_cus_DF = vip_cus_raw_DF.groupBy(['rpt_mth', 'vip_seg', 'city_seg', 'f_new_pur_12m', 
                                     'f_lapsed_12m', 'f_clm_cnt_12m', 'f_30_days_after_due'])\
    .agg(
        F.countDistinct(F.col('po_num')).alias('no_vip_cus')
    )

vip_cus_DF.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Random Classification (Test)

# COMMAND ----------

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
import pandas as pd

# COMMAND ----------

# Convert dataset to Pandas
vipDF = vip_cus_raw_DF.toPandas()

# Set 'target' variable
X = vipDF.drop('target', axis=1)
y = vipDF['target']

# Preprocessing labels
label_encoder = LabelEncoder()
X['rpt_mth_encoded'] = label_encoder.fit_transform(X['rpt_mth'])
X['vip_seg_encoded'] = label_encoder.fit_transform(X['vip_seg'])
X['city_seg_encoded'] = label_encoder.fit_transform(X['city_seg'])
X['cus_city_encoded'] = label_encoder.fit_transform(X['cus_city'])
X = X.drop(['rpt_mth', 'vip_seg', 'city_seg', 'cus_city'], axis=1)

# Split train and test data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Modeling
model = RandomForestClassifier()
model.fit(X_train, y_train)

# Predict probability scores
probabilities = model.predict_proba(X_test)[:,1]

# Assign probability scores to the corresponding po_num's
results_DF = pd.DataFrame({'po_num': X_test['po_num'], 'score': probabilities})

# Now rank VIP customers by score and select top 70%
selected_vip_customers = results_DF.sort_values('score', ascending=False).head(int(len(results_DF)*0.7))

# COMMAND ----------

#display(selected_vip_customers)
