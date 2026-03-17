# Databricks notebook source
# MAGIC %md
# MAGIC # ADEC Metrics Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### GENERATE AUTO METRICS

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load libls</strong>

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Initialize tables and params</strong>

# COMMAND ----------

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
rpt_yr = last_mthend[0:4]
rpt_mth = last_mthend[2:4]+last_mthend[5:7]
lmth = -1
llmth = -2
exclusion_list_full = ('MI007','PA007','PA008', 'EV001', 'EV002', 'EV003', 'EV004', 'EV005', 'EV006', 'EV007', 'EV008', 'EV009', 'EV010', 'EV011', 'EV012', 'EV013', 'EV014', 'EV015', 'EV016', 'EV017', 'EV018', 'EV019', 'EV101', 'EV102', 'EV103', 'EV104', 'EV105', 'EV201', 'EV202', 'EV203', 'EVS01', 'EVS02', 'EVS03', 'EVS04', 'EVS05', 'FC103', 'FC208', 'FD101', 'FD102', 'FD103', 'FD104', 'FD105', 'FD106', 'FD107', 'FD108', 'FD109', 'FD204', 'FD205', 'FD206', 'FD207', 'FD208', 'FD209','FD210', 'FD211', 'FD212', 'FD213', 'FD214', 'FS101', 'FS102', 'FS103', 'FS104', 'FS105', 'FS106', 'FS107', 'FS108', 'FS109', 'FS205', 'FS206', 'FS207', 'FS208', 'FS209', 'FS210', 'FS211', 'FS212', 'FS213', 'FS214', 'FC101', 'FC102', 'FC104', 'FC105', 'FC106', 'FC107', 'FC108', 'FC109', 'FC206', 'FC207', 'FC209', 'FC210','FC211', 'FC212', 'FC213', 'FC214', 'VEH10', 'VEU14', 'VEP18', 'FC205', 'FS204', 'FC204', 'EVX03', 'FD203', 'FS203', 'FC203', 'FD202', 'FS202', 'FC202', 'VEDCL', 'VEDEN') 
exclusion_list_sub = ('MI007','PA007','PA008')
active_sts = ('1','2','3','5')
pi_wam_cust = 20223 # Total Number of Unique Customers (r 6)
pi_wam_no_per_cust = 1.1336 # Number of Funds per Customer (r 32)
pi_wam_no_per_agt_cust = 1.1336 # r 33
pi_wam_no_per_banca_cust = 1.0735 # r 34
pi_no_cyst_ly = 1322260
pi_no_cust_tgt = 0 # pre 1151196
pi_digital_leads = 441 # Retrieve from Digital Leads dashboard
pi_rnps = 70
pi_entry_goal_rnps = 0
pi_strech_goal_rnps = 0
pi_top_comp_rnps = 0
pi_top_comp_name = "Manulife"

customer_table = spark.read.parquet(f"/mnt/lab/vn/project/cpm/ADEC/WorkingData/customer_table_{rpt_mth}")
product_table = spark.read.parquet(f"/mnt/lab/vn/project/cpm/ADEC/WorkingData/product_table_{rpt_mth}")
policy_base = spark.read.parquet(f"/mnt/lab/vn/project/cpm/ADEC/WorkingData/policy_base_{rpt_mth}")
att_cus = spark.read.parquet(f"/mnt/lab/vn/project/cpm/ADEC/WorkingData/att_cus_{rpt_mth}")
newcustomer_year = spark.read.parquet(f"/mnt/lab/vn/project/cpm/ADEC/WorkingData/newcustomer_{rpt_yr}")
newcustomer = spark.read.parquet(f"/mnt/lab/vn/project/cpm/ADEC/WorkingData/newcustomer_{rpt_mth}")
custdm_table = spark.read.parquet(f"/mnt/lab/vn/project/cpm/datamarts/TCUSTDM_MTHEND/image_date={last_mthend}")

#print("customer_table:",customer_table.count())
#print("product_table:",product_table.count())
#print("policy_base:",policy_base.count())
#print("att_cus:",att_cus.count())
#print("newcustomer:",newcustomer.count())
print(last_mthend)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create Temp View for all DF</strong>

# COMMAND ----------

customer_table.createOrReplaceTempView(f"customer_table_{rpt_mth}")
product_table.createOrReplaceTempView(f"product_table_{rpt_mth}")
policy_base.createOrReplaceTempView(f"policy_base_{rpt_mth}")
att_cus.createOrReplaceTempView(f"att_cus_{rpt_mth}")
newcustomer_year.createOrReplaceTempView(f"newcustomer_{rpt_yr}")
newcustomer.createOrReplaceTempView(f"newcustomer_{rpt_mth}")
custdm_table.createOrReplaceTempView(f"custdm_table_{rpt_mth}")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create CS</strong>

# COMMAND ----------

tcustdm_mthend_path = f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TCUSTDM_MTHEND/image_date={last_mthend}'
tpolidm_mthend_path = f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/image_date={last_mthend}'
tagtdm_mthend_path = f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TAGTDM_MTHEND/image_date={last_mthend}'

tcustdm_mthend_df = spark.read.format("parquet").load(tcustdm_mthend_path)
tpolidm_mthend_df = spark.read.format("parquet").load(tpolidm_mthend_path)
tagtdm_mthend_df = spark.read.format("parquet").load(tagtdm_mthend_path)

tcustdm_mthend_df.createOrReplaceTempView("tcustdm_mthend")
tpolidm_mthend_df.createOrReplaceTempView("tpolidm_mthend")
tagtdm_mthend_df.createOrReplaceTempView("tagtdm_mthend")

# COMMAND ----------

df_temp_kpi5_cws_move_base = spark.sql(f"""
  select distinct	
			pol.pol_num
			,pol.plan_code
			,pol.po_num
			,pol.insrd_num
			,pol.pol_eff_dt
			,agt.agt_code
			,agt.loc_cd
			,case
				when agt.loc_cd is null then (case
												when pol.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','88','49','51','52','53','58') then 'Banca'
												when pol.dist_chnl_cd in ('48') then 'Affinity'
												when pol.dist_chnl_cd in ('01', '02', '08', '50', '*') then 'Agency'
												when pol.dist_chnl_cd in ('05','06','07','34','36','35') then 'DMTM'
												when pol.dist_chnl_cd in ('09') then 'MI'
												else 'Unknown'
											end)
				when pol.dist_chnl_cd in ('*') then 'Agency'
				else
					nvl(agt.channel,'Unknown')
			end as channel
		,cus.mobl_phon_num
		,cus.move_lst_login_dt
		,cus.city
	from
		tpolidm_mthend pol
	left join 
		tagtdm_mthend agt on (pol.wa_code = agt.agt_code)
 	left join 
  		tcustdm_mthend cus on (pol.po_num = cus.cli_num)
	where
		pol.pol_eff_dt <= '{last_mthend}'
		and pol.pol_stat_cd in ('1','2','3','5')
		and pol.plan_code not in ('MI007','PA007','PA008')
 """)

df_temp_kpi5_cws_move_base_dedup = df_temp_kpi5_cws_move_base.select('po_num', 'channel', 'pol_eff_dt').orderBy(df_temp_kpi5_cws_move_base.po_num, df_temp_kpi5_cws_move_base.pol_eff_dt.desc()).dropDuplicates(['po_num'])
df_cws_move = df_temp_kpi5_cws_move_base.drop('channel').join(df_temp_kpi5_cws_move_base_dedup, on=['po_num','pol_eff_dt'],how='left') 
df_cws_move.createOrReplaceTempView("temp_kpi5_cws_base")

# COMMAND ----------

cws_old = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Full/VN_PUBLISHED_EXTERNAL_DB/VN_DECREE53_CWS/*/*/*/*.parquet')
cws_old.createOrReplaceTempView('cws_old')

files = dbutils.fs.ls('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MONGO_DB/VN-CWS/VN-AUDIT/')
vn_audit = None
for parquet_file in files:
    if vn_audit is None:
        # print(parquet_file.path)
        vn_audit = spark.read.format("parquet").load(parquet_file.path)
        vn_audit = vn_audit.select("userId","clientType","eventTimestamp","responseStatus","userAgent","transactionType","eventType")  # 读取第一个文件
    else:
        vn_audit = vn_audit.union(spark.read.parquet(parquet_file.path).select("userId","clientType","eventTimestamp","responseStatus","userAgent","transactionType","eventType"))
# print(parquet_file.path)
vn_audit.createOrReplaceTempView("vn_audit")

# COMMAND ----------

df_temp_kpi5_cws_active_users = spark.sql(f"""
    (SELECT 
        cs.po_num as client_number
        ,to_date(from_utc_timestamp(c.eventTimestamp,'UTC+7'),"yyyy-MM-dd'T'HH:mm:ss") as login_date
    from cws_old c
    JOIN
        vn_published_sfdc_easyclaims_db.Account acc 
        ON c.`userId` = acc.MCF_User_Id__pc   
    JOIN temp_kpi5_cws_base cs 
        ON acc.INTEGRATION_KEY__C = cs.po_num
    where transactionType = 'login' 
        and eventType = 'authenticate' 
        and responseStatus = 200  
        and to_date(from_utc_timestamp(c.eventTimestamp,'UTC+7'),"yyyy-MM-dd'T'HH:mm:ss") between date_add(ADD_MONTHS('{last_mthend}',-12),1) AND '{last_mthend}'
    UNION ALL 
    SELECT  
        cs.po_num as client_number
        ,to_date(from_utc_timestamp(c.eventTimestamp,'UTC+7'),"yyyy-MM-dd'T'HH:mm:ss") as login_date
    from vn_audit c 
    JOIN
        vn_published_sfdc_easyclaims_db.Account acc 
        ON c.`userId` = acc.MCF_User_Id__pc      
    JOIN temp_kpi5_cws_base cs 
        ON acc.INTEGRATION_KEY__C = cs.po_num
    where transactionType = 'login' 
        and eventType = 'authenticate' 
        and (responseStatus = 200 or regexp_extract(responseStatus, '"\\$numberInt":"(\\d+)"', 1) = '200')
        and to_date(from_utc_timestamp(c.eventTimestamp,'UTC+7'),"yyyy-MM-dd'T'HH:mm:ss") between date_add(ADD_MONTHS('{last_mthend}',-12),1) AND '{last_mthend}')   
""")
df_temp_kpi5_cws_active_users.createOrReplaceTempView("temp_kpi5_cws_active_users")

# df_cws_move_sum = df_cws_move.groupBy('image_date').agg(
#   F.countDistinct("po_num").alias("CWS_activation_denominator"),
#   F.countDistinct(F.when(F.last_day(F.col("move_lst_login_dt")) == F.last_day(F.add_months(F.current_date(), -1)), F.col("po_num"))).alias("MOVE_activation_numerator"),
#   F.countDistinct(F.when(F.col("city").isin(['HN','Hà Nội','Hải Phòng','Đà Nẵng','HCM','Hồ Chí Minh','Cần Thơ'])&F.col("channel").isin(['Agency','DMTM']), F.col("po_num"))).alias("MOVE_activation_denominator"))

#df_cws_move.createOrReplaceTempView('df_cws_move')

# map with client id and get only those in the denominator.
mongo_n_cosmo = spark.sql("""
select  mli.client_number as cli_num, max(login_date) lst_login_dt
from    temp_kpi5_cws_active_users mli
group by
        mli.client_number
""")

mongo_n_cosmo.createOrReplaceTempView("cws_login")

# COMMAND ----------

# Exact denominator for CWS activation KPI
cws_po = spark.sql(f"""
select	{rpt_mth} as yr_mth
		,count(cli_num) CWS_activation_numerator
from 	cws_login
""")
cws_po.createOrReplaceTempView("cws_po")

cs = spark.sql(f"""
with cs as (
select
		{rpt_mth} as yr_mth
		,count(po_num) as No_IFP_Cust
		,count(case when need_cnt=1 then po_num end) as No_Cust_1_Need
		,count(case when need_cnt>1 then po_num end) as No_Cust_2more_Need
		,count(case when need_cnt=2 then po_num end) as No_Cust_2_Need
		,count(case when need_cnt=3 then po_num end) as No_Cust_3_Need
		,count(case when need_cnt=4 then po_num end) as No_Cust_4_Need
		,count(case when need_cnt>4 then po_num end) as No_Cust_5more_Need
		,count(case when prod_cnt=1 then po_num end) as no_cust_1_IFP
		,count(case when prod_cnt=2 then po_num end) as no_cust_2_IFP
		,count(case when prod_cnt=3 then po_num end) as no_cust_3_IFP
		,count(case when prod_cnt=4 then po_num end) as no_cust_4_IFP
		,count(case when prod_cnt>4 then po_num end) as no_cust_5more_IFP
		,cast(sum(need_cnt)/count(po_num) as decimal(7,4)) no_IFP_Category_per_cust
		,cast(count(case when need_type='SINGLE' then po_num end)/count(po_num) as decimal(7,4)) percent_IFP_Single_Category
		,cast(count(case when need_type='MULTIPLE' then po_num end)/count(po_num) as decimal(7,4)) percent_IFP_Multiple_Category
		,count(case when digital_reg_ind_v2 = 'Y' then po_num end) as no_digital_reg_v2
		--,count(case
		--			when cws_lst_login_dt >= date_add(last_day(add_months(current_date,{lmth})),-365) then po_num
		--		end)
		--,sum(coalesce(cws.CWS_activation_numerator,0)) as CWS_activation_numerator
		,count(distinct po_num) as CWS_activation_denominator -- only select those with distinct mobile phone number
		,count(case when last_day(move_lst_login_dt) = last_day(add_months(current_date,{lmth})) then po_num end) MOVE_activation_numerator
		,count(case when city in ('HN','Hà Nội','Hải Phòng','Đà Nẵng','HCM','Hồ Chí Minh','Cần Thơ') and channel in ('Agency','DMTM') then po_num end) MOVE_activation_denominator
		,count(case when clm_cws_ind='Y' or clm_ezc_ind='Y' then po_num end) no_customers_claimed_online,
		count(case when clm_ind='Y' then po_num end) no_customers_claimed_all,				
		count(case when pmt_ind='Y' then po_num end) no_payment_cust,
		count(case when pmt_onl_ind='Y' then po_num end) no_digital_payment_cust,
		cast(sum(tot_ape)/count(po_num) as decimal(11,2)) avg_APE_per_cust,
		cast(count(case when mobile_contact is not null and sms_ind='Y' then po_num end)/
		count(case when mobile_contact is not null then po_num end) as decimal(7,4))
		percent_mobile_register,
		cast(count(case when email_contact='Y' and sms_ind='Y' then po_num end)/
		count(case when email_contact is not null then po_num end) as decimal(7,4))
		percent_email_register,
		cast(sum(tenure)/count(po_num) as decimal(7,4)) avg_cust_tenure,
		cast(sum(age_curr)/count(po_num) as decimal(7,4)) avg_cust_age,
		count(case when customer_type='New Customer' then po_num end) no_new_IFP_cust,
		count(case when customer_type='Existing Customer' then po_num end) no_existing_IFP_cust,
		sum(case when customer_type='New Customer' then prod_cnt_mtd end) no_newbiz_new_IFP_cust_MTD,
		sum(case when customer_type='Existing Customer' then prod_cnt_mtd end) no_newbiz_existing_IFP_cust_MTD,
		round(sum(case when customer_type='New Customer' then ape_mtd end)/23145,2) APE_new_IFP_cust_MTD,
		round(sum(case when customer_type='Existing Customer' then ape_mtd end)/23145,2) APE_existing_IFP_cust_MTD,
		round(sum(case when customer_type='New Customer' then nbv_mtd end)/23145,2) NBV_new_IFP_cust_MTD,
		round(sum(case when customer_type='Existing Customer' then nbv_mtd end)/23145,2) NBV_existing_IFP_cust_MTD
	from
		customer_table_{rpt_mth}
group by yr_mth)
select	cs.*,
		cws.CWS_activation_numerator
from  	cs left join
		cws_po cws on cs.yr_mth = cws.yr_mth
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create CT</strong>

# COMMAND ----------

ct = spark.sql(f"""

select
		{rpt_mth} yr_mth,		
		count(case when pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then po_num end ) Attrition_MTD,
		sum(case when pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then prod_cnt end ) Attrition_MTD_Prod,
		count(case when att_reason='Lapsed' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then po_num end) Attrition_by_Lapse_MTD,
		sum(case when att_reason='Lapsed' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then prod_cnt end) Attrition_by_Lapse_MTD_Prod,
		count(case when att_reason='Matured' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then po_num end
			 ) Attrition_by_Policy_Matured_MTD,
		sum(case when att_reason='Matured' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then prod_cnt end
			 ) Attrition_by_Policy_Matured_MTD_Prod,
		count(case when att_reason='Paid' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth})) 
					then po_num end
			 ) Attrition_by_Benefit_Paid_MTD,
		sum(case when att_reason='Paid' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth})) 
					then prod_cnt end
			 ) Attrition_by_Benefit_Paid_MTD_Prod,
		count(case when att_reason='Surrendered' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then po_num end
			 ) Attrition_by_Policy_Surrender_MTD,
		sum(case when att_reason='Surrendered' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then prod_cnt end
			 ) Attrition_by_Policy_Surrender_MTD_Prod,
		count(case when att_reason='Others' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then po_num end
			 ) Attrition_by_Others_MTD,
		sum(case when att_reason='Others' and pol_trmn_dt between date_add(last_day(add_months(current_date,{llmth})),1) and last_day(add_months(current_date,{lmth}))
					then prod_cnt end
			 ) Attrition_by_Others_MTD_Prod,
		count(*) Attrition_YTD,
		sum(prod_cnt) Attrition_YTD_Prod,
		count(case when att_reason='Lapsed' then po_num end) Attrition_by_Lapse_YTD,
		sum(case when att_reason='Lapsed' then prod_cnt end) Attrition_by_Lapse_YTD_Prod,
		count(case when att_reason='Matured' then po_num end) Attrition_by_Policy_Matured_YTD,
		sum(case when att_reason='Matured' then prod_cnt end) Attrition_by_Policy_Matured_YTD_Prod,
		count(case when att_reason='Paid' then po_num end) Attrition_by_Benefit_Paid_YTD,
		sum(case when att_reason='Paid' then prod_cnt end) Attrition_by_Benefit_Paid_YTD_Prod,
		count(case when att_reason='Surrendered' then po_num end) Attrition_by_Policy_Surrender_YTD
		,sum(case when att_reason='Surrendered' then prod_cnt end) Attrition_by_Policy_Surrender_YTD_Prod
		,count(case when att_reason='Others' then po_num end) Attrition_by_Others_YTD
		,sum(case when att_reason='Others' then prod_cnt end) Attrition_by_Others_YTD_Prod
	from
		att_cus_{rpt_mth}
	where
		year(pol_trmn_dt) = year(last_day(add_months(current_date,{lmth})))

""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create CP</strong>

# COMMAND ----------

cp = spark.sql(f"""
               
select
		{rpt_mth} yr_mth,
		cast(count(a.pol_num)/count(distinct b.po_num) as decimal(5,2)) no_IFP_per_cust,
		cast(count(case when b.customer_type='New Customer' then a.pol_num end)/
		count(distinct case when b.customer_type='New Customer' then b.po_num end) as decimal(5,2))
		no_IFP_per_new_cust,
		cast(count(case when b.customer_type='Existing Customer' then a.pol_num end)/
		count(distinct case when b.customer_type='Existing Customer' then b.po_num end) as decimal(5,2))
		no_IFP_per_existing_cust,
		cast(count(case when b.channel='Agency' then a.pol_num end)/count(distinct case when b.channel='Agency' then b.po_num end) as decimal(5,2)) no_IFP_per_agency_cust,
		cast(count(case when b.channel='Banca' then a.pol_num end)/count(distinct case when b.channel='Banca' then b.po_num end) as decimal(5,2)) no_IFP_per_banca_cust,
		count(distinct case when b.loc_code_agt like 'TCB%' then b.po_num end) 	Ex_Banca_1_Cust,
		count(distinct case when b.loc_code_agt like 'SAG%' then b.po_num end) Ex_Banca_2_Cust
		,count(distinct case when b.loc_code_agt like 'VTI%' then b.po_num end) Ex_Banca_3_Cust
	from	
		product_table_{rpt_mth} a
		left join policy_base_{rpt_mth} b on a.pol_num=b.pol_num	
	where
		a.plan_code not in {exclusion_list_full}
		and	b.pol_num is not null            
               
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create RS_CN</strong>

# COMMAND ----------

rs_cn = spark.sql(f"""
                  
select
		{rpt_mth} yr_mth,
		count(case when b.po_num is not null then a.po_num end) New_Cust_MTD,
		count(a.po_num) New_Cust_YTD		
	from
		newcustomer_{rpt_yr} a
		left join newcustomer_{rpt_mth} b on a.po_num=b.po_num               
                  
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create RS_CPC</strong>

# COMMAND ----------

rs_cpc = spark.sql(f"""
                   
select
		{rpt_mth} yr_mth,
		cast(count(case when a.cvg_typ='B' then a.pol_num end)/count(distinct b.po_num) as decimal(5,2)) IFP_per_cust_B,
		cast(count(case when a.cvg_typ<>'B' then a.pol_num end)/count(distinct b.po_num) as decimal(5,2)) IFP_per_cust_R,
		count(case when a.cvg_typ='B' and a.pol_tenure=0 then a.pol_num end) new_business_MTD,
		cast(sum(a.face_amt)/count(distinct b.po_num) as decimal(11,2)) avg_FA_per_cust				
	from
		product_table_{rpt_mth} a
		left join customer_table_{rpt_mth} b on (a.po_num = b.po_num)
	where
		a.plan_code not in {exclusion_list_full}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>ADEC Inputs Summary</strong>

# COMMAND ----------

cs.createOrReplaceTempView("cs")
cp.createOrReplaceTempView("cp")
ct.createOrReplaceTempView("ct")
rs_cn.createOrReplaceTempView("rs_cn")
rs_cpc.createOrReplaceTempView("rs_cpc")

adec_report = spark.sql(f"""
                        
select
	'Vietnam' country,
	last_day(add_months(current_date,{lmth})) year_month
	,cs.no_ifp_cust no_ifp_cust
	,0  as no_glh_cust
	,{pi_wam_cust} as no_wam_cust
	,0 as no_rinv_cust_beta
	,0 as no_pension_cust
	,0 as no_gi_cust
	,0 as no_credit_card_cust
	,(cs.no_cust_1_ifp + 0 + {pi_wam_cust}) no_cust_1_product
	,(cs.no_cust_2_ifp + cs.no_cust_3_ifp + cs.no_cust_4_ifp + cs.no_cust_5more_ifp) no_cust_2more_product
	,cs.no_cust_2_ifp no_cust_2_product
	,cs.no_cust_3_ifp no_cust_3_product
	,cs.no_cust_4_ifp no_cust_4_product
	,cs.no_cust_5more_ifp no_cust_5more_product
	,cn.new_cust_mtd new_cust_mtd,
	0 as new_ifp_cust_mtd,
	0  as new_glh_cust_mtd,
	0 as new_rinv_cust_mtd,
	0 as new_pension_cust_mtd,
	0 as new_gi_cust_mtd,	
	0 as new_credit_card_cust_mtd,
	cn.new_cust_ytd new_cust_ytd,
	0 as new_ifp_cust_ytd,
	0  as new_glh_cust_ytd,
	0 as new_rinv_cust_ytd,
	0 as new_pension_cust_ytd,
	0 as new_gi_cust_ytd,	
	0 as new_credit_card_cust_ytd,
	ct.attrition_mtd attrition_mtd,
	0 as attrition_ifp_cust_mtd,
	0  as attrition_glh_cust_mtd,
	0 as attrition_rinv_cust_mtd,
	0 as attrition_pension_cust_mtd,
	0 as attrition_gi_cust_mtd,	
	0 as attrition_credit_card_cust_mtd,
	ct.attrition_by_lapse_mtd attrition_by_lapse_mtd,
	ct.attrition_by_policy_matured_mtd attrition_by_policy_matured_mtd,
	ct.attrition_by_benefit_paid_mtd attrition_by_benefit_paid_mtd,
	ct.attrition_by_policy_surrender_mtd attrition_by_policy_surrender_mtd,
	0 as attrition_by_other_mtd,
	0 as attrition_mtd_product,
	0 as attrition_by_lapse_mtd_product,
	0 as attrition_by_policy_matured_mtd_product,
	0 as attrition_by_benefit_paid_mtd_product,
	0 as attrition_by_surrender_mtd_product,
	0 as attrition_by_other_mtd_product,
	ct.attrition_ytd attrition_ytd,
	0 as attrition_ifp_cust_ytd,
	0 as attrition_glh_cust_ytd,
	0 as attrition_rinv_cust_ytd,
	0 as attrition_pension_cust_ytd,
	0 as attrition_gi_cust_ytd,	
	0 as attrition_credit_card_cust_ytd,
	ct.attrition_by_lapse_ytd attrition_by_lapse_ytd,
	ct.attrition_by_policy_matured_ytd attrition_by_policy_matured_ytd,
	ct.attrition_by_benefit_paid_ytd attrition_by_benefit_paid_ytd,
	ct.attrition_by_policy_surrender_ytd attrition_by_policy_surrender_ytd,
	0 as attrition_by_other_ytd,
	0 as attrition_ytd_product,
	0 as attrition_by_lapse_ytd_product,
	0 as attrition_by_policy_matured_ytd_product,
	0 as attrition_by_benefit_paid_ytd_product,
	0 as attrition_by_surrender_ytd_product,
	0 as attrition_by_other_ytd_product,
	0 as no_lob_per_cust,		
	cp.no_ifp_per_cust no_ifp_per_cust,
	cs.no_cust_1_ifp no_cust_1_ifp,
	cs.no_cust_2_ifp no_cust_2_ifp,
	cs.no_cust_3_ifp no_cust_3_ifp,
	cs.no_cust_4_ifp no_cust_4_ifp,
	cs.no_cust_5more_ifp no_cust_5more_ifp,
	cs.no_ifp_category_per_cust no_ifp_category_per_cust,
	cs.percent_ifp_single_category percent_ifp_single_category,
	cs.percent_ifp_multiple_category percent_ifp_multi_category
	-- cs.no_digital_register no_digital_register no more in use
	,cs.no_digital_reg_v2 no_digital_register	
	-- ,cs.no_digital_180day_active no_digital_180day_active
	,0 as no_digital_180day_active
	,0 as no_digital_lead
	,{pi_digital_leads} new_business_DTC_MTD
	,cpc.new_business_MTD new_business_MTD
	,0 as new_business_mtd_ifp
	,0 as new_business_mtd_gi
	,CWS_activation_numerator
	,CWS_activation_denominator
	,MOVE_activation_numerator
	,MOVE_activation_denominator
	,((CWS_activation_numerator/CWS_activation_denominator)+(MOVE_activation_numerator/MOVE_activation_denominator))/2 WeChat_activation_numerator
	,cp.Ex_Banca_1_Cust+cp.Ex_Banca_2_Cust WeChat_activation_denominator
	,'Techcombank' Ex_Banca_1_Name,
	cp.Ex_Banca_1_Cust Ex_Banca_1_Cust,
	'Saigon Commercial Bank' Ex_Banca_2_Name,
	cp.Ex_Banca_2_Cust Ex_Banca_2_Cust,
	'Viettin Bank' as Ex_Banca_3_Name,
	cp.Ex_Banca_3_Cust,
	0 as Ex_Banca_4_Name,
	0 as Ex_Banca_4_Cust,
	cs.no_new_IFP_cust no_new_IFP_cust,
	cs.no_existing_IFP_cust no_existing_IFP_cust,
	cs.no_newbiz_new_IFP_cust_MTD no_newbiz_new_IFP_cust_MTD,
	cs.no_newbiz_existing_IFP_cust_MTD no_newbiz_existing_IFP_cust_MTD,
	cs.APE_new_IFP_cust_MTD*1000 APE_new_IFP_cust_MTD,
	cs.APE_existing_IFP_cust_MTD*1000 APE_existing_IFP_cust_MTD,
	cs.NBV_new_IFP_cust_MTD*1000 NBV_new_IFP_cust_MTD,
	cs.NBV_existing_IFP_cust_MTD*1000 NBV_existing_IFP_cust_MTD
	,last_day(add_months(current_date,{lmth})) as rpt_mth
from
	cs
	inner join rs_cn cn on (cs.yr_mth = cn.yr_mth)
	inner join rs_cpc cpc on (cs.yr_mth = cpc.yr_mth)
	left join ct on cs.yr_mth=ct.yr_mth	
	left join cp on (cs.yr_mth=cp.yr_mth)                 
                        
""")

# COMMAND ----------

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

adec_report.write.mode("overwrite").partitionBy("year_month").parquet(f"/mnt/lab/vn/project/cpm/ADEC/ADEC_REPORT_{rpt_yr}/")

# COMMAND ----------

# convert Spark DataFrame into a Pandas DataFrame
adec_report_pd = adec_report.toPandas()

# Write the Pandas DataFrame to a CSV file 
adec_report_pd.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/ADEC/ADEC_REPORT_{rpt_mth}.csv", index=False)

