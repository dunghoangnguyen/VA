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
exclusion_list_full = []
exclusion_list_sub = ['MI007','PA007','PA008']
active_sts = ['1','2','3','5']
pi_wam_cust = 16391 # Total Number of Unique Customers (r 6)
pi_wam_no_per_cust = 1.1336 # Number of Funds per Customer (r 32)
pi_wam_no_per_agt_cust = 1.1336 # r 33
pi_wam_no_per_banca_cust = 1.0735 # r 34
pi_no_cyst_ly = 1561144
pi_no_cust_tgt = 0 # pre 1151196
pi_digital_leads = 637 # Retrieve from Digital Leads dashboard
pi_rnps = 70
pi_entry_goal_rnps = 0
pi_strech_goal_rnps = 0
pi_top_comp_rnps = 0
pi_top_comp_name = "Manulife"

customer_table = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/customer_table_{rpt_mth}")
product_table = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/product_table_{rpt_mth}")
policy_base = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/policy_base_{rpt_mth}")
att_cus = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/att_cus_{rpt_mth}")
newcustomer_year = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/newcustomer_{rpt_yr}")
newcustomer = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/newcustomer_{rpt_mth}")

#print("customer_table:",customer_table.count())
#print("product_table:",product_table.count())
#print("policy_base:",policy_base.count())
#print("att_cus:",att_cus.count())
#print("newcustomer:",newcustomer.count())

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

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Create CS</strong>

# COMMAND ----------

cs = spark.sql(f"""

select
		{rpt_mth} yr_mth
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
		,count(case
					when cws_lst_login_dt >= date_add(last_day(add_months(current_date,{lmth})),-365) then po_num
				end
		) CWS_activation_numerator
		,count(distinct mobl_phon_num) as CWS_activation_denominator -- only select those with distinct mobile phone number
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
		a.plan_code not in ('MI007','PA007','PA008', 'EV001', 'EV002', 'EV003', 'EV004', 'EV005', 'EV006', 'EV007', 'EV008', 'EV009', 'EV010', 'EV011', 'EV012', 'EV013', 'EV014', 'EV015', 'EV016', 'EV017', 'EV018', 'EV019', 'EV101', 'EV102', 'EV103', 'EV104', 'EV105', 'EV201', 'EV202', 'EV203', 'EVS01', 'EVS02', 'EVS03', 'EVS04', 'EVS05', 'FC103', 'FC208', 'FD101', 'FD102', 'FD103', 'FD104', 'FD105', 'FD106', 'FD107', 'FD108', 'FD109', 'FD204', 'FD205', 'FD206', 'FD207', 'FD208', 'FD209','FD210', 'FD211', 'FD212', 'FD213', 'FD214', 'FS101', 'FS102', 'FS103', 'FS104', 'FS105', 'FS106', 'FS107', 'FS108', 'FS109', 'FS205', 'FS206', 'FS207', 'FS208', 'FS209', 'FS210', 'FS211', 'FS212', 'FS213', 'FS214', 'FC101', 'FC102', 'FC104', 'FC105', 'FC106', 'FC107', 'FC108', 'FC109', 'FC206', 'FC207', 'FC209', 'FC210','FC211', 'FC212', 'FC213', 'FC214', 'VEH10', 'VEU14', 'VEP18', 'FC205', 'FS204', 'FC204', 'EVX03', 'FD203', 'FS203', 'FC203', 'FD202', 'FS202', 'FC202', 'VEDCL', 'VEDEN') 
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
		a.plan_code not in ('MI007','PA007','PA008', 'EV001', 'EV002', 'EV003', 'EV004', 'EV005', 'EV006', 'EV007', 'EV008', 'EV009', 'EV010', 'EV011', 'EV012', 'EV013', 'EV014', 'EV015', 'EV016', 'EV017', 'EV018', 'EV019', 'EV101', 'EV102', 'EV103', 'EV104', 'EV105', 'EV201', 'EV202', 'EV203', 'EVS01', 'EVS02', 'EVS03', 'EVS04', 'EVS05', 'FC103', 'FC208', 'FD101', 'FD102', 'FD103', 'FD104', 'FD105', 'FD106', 'FD107', 'FD108', 'FD109', 'FD204', 'FD205', 'FD206', 'FD207', 'FD208', 'FD209','FD210', 'FD211', 'FD212', 'FD213', 'FD214', 'FS101', 'FS102', 'FS103', 'FS104', 'FS105', 'FS106', 'FS107', 'FS108', 'FS109', 'FS205', 'FS206', 'FS207', 'FS208', 'FS209', 'FS210', 'FS211', 'FS212', 'FS213', 'FS214', 'FC101', 'FC102', 'FC104', 'FC105', 'FC106', 'FC107', 'FC108', 'FC109', 'FC206', 'FC207', 'FC209', 'FC210','FC211', 'FC212', 'FC213', 'FC214', 'VEH10', 'VEU14', 'VEP18', 'FC205', 'FS204', 'FC204', 'EVX03', 'FD203', 'FS203', 'FC203', 'FD202', 'FS202', 'FC202', 'VEDCL', 'VEDEN')
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

adec_report.write.mode("overwrite").partitionBy("year_month").parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/ADEC_REPORT_{rpt_yr}/")

# COMMAND ----------

# convert Spark DataFrame into a Pandas DataFrame
adec_report_pd = adec_report.toPandas()

# Write the Pandas DataFrame to a CSV file 
adec_report_pd.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/ADEC/ADEC_REPORT_{rpt_mth}.csv", index=False)

