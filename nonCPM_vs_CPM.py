# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
al_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'
cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'

table0 = 'tcustdm_daily/'
table1 = 'tpolidm_daily/'
table2 = 'tagtdm_daily/'
table3 = 'vn_plan_code_map/'
table4 = 'agent_scorecard/'
table5 = 'targetm_dm/'

path_list = [dm_path, al_path, cpm_path]
table_list = [table0, table1, table2, table3, table4, table5]

cmpgn_st_dt='2023-01-01'
start_date='2023-01-01'
end_date='2023-12-31'
yr=2023

# COMMAND ----------

df_list = load_parquet_files(path_list, table_list)
generate_temp_view(df_list)

# COMMAND ----------

# DBTITLE 1,Select total policy portfolio
allcus_allsts = spark.sql(f"""
select	po_num, pol_num, wa_code, sa_code, tot_ape, pol_iss_dt, pol_stat_cd, dist_chnl_cd, case when b.comp_prvd_num='01' and b.channel='Agency' then 'Y' else 'N' end act_agt_ind
from 	tpolidm_daily a inner join
		tagtdm_daily b on a.sa_code=b.agt_code
where	pol_stat_cd not in ('8','A','N','R','X')
""")
allcus_allsts.createOrReplaceTempView('allcus_allsts')

# COMMAND ----------

# DBTITLE 1,Get all valid sales in year (only exclude those pending/not-taken)

agency_cus_sales = spark.sql(f"""
select	po_num, pol_num, pol.plan_code, nbv_factor_group plan_code_desc, tot_ape, sbmt_dt new_sbmt_dt, pol_iss_dt new_iss_dt, wa_code, 
        floor(months_between('{end_date}',agt.CNTRCT_EFF_DT)) tenure, scr.AGENT_TIER,
        case when dist_chnl_cd in ('01','02','08','50','*') then 'Agency'
             when dist_chnl_cd in ('05','06','07','34','36') then 'DMTM'
             else 'Banca'
        end as channel,
        case when pol_stat_cd in ('1','2','3','5','7','9') then '01-Inforce'
             when pol_stat_cd in ('B','E') then '03-Lapsed/Surrendered'
             when pol_stat_cd in ('F','H') then '02-Matured'
             else '04-Others'
        end as pol_status,
        to_date(pol_trmn_dt) pol_trmn_dt
from	tpolidm_daily pol left join
		(select distinct plan_code, nbv_factor_group from vn_plan_code_map) pln on	pol.plan_code=pln.plan_code left join
		tagtdm_daily agt on pol.wa_code=agt.agt_code left join
		agent_scorecard scr on agt.agt_code=scr.agt_code and scr.monthend_dt='{end_date}'
where	pol_iss_dt>='{start_date}'
	and	pol_stat_cd not in ('8','A','N','R','X')
	--and DIST_CHNL_CD in ('01', '02', '08', '50', '*')
""")
agency_cus_sales.createOrReplaceTempView(f'agency_cus_sales_{yr}')

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Group Agency to identify New customers (customers whose first policy purchased in year)
agency_cus_sum = spark.sql(f"""
select	po_num, sum(tot_ape) total_ape, count(pol_num) pol_cnt, count(distinct wa_code) agt_cnt, min(new_iss_dt) frst_iss_dt
from	agency_cus_sales_{yr}
group by po_num
""")
agency_cus_sum.createOrReplaceTempView(f'agency_cus_{yr}')

# COMMAND ----------

# DBTITLE 1,Select all NON-pending/not-taken customers from total portfolio
agency_cus_inforce_v2 = spark.sql(f"""
select	po_num, sum(tot_ape) total_ape, count(pol_num) pol_cnt, count(distinct wa_code) agt_cnt, max(case when act_agt_ind='Y' then wa_code end) wa_code,
		max(case when act_agt_ind='Y' then sa_code end) sa_code, max(case when sa_code=wa_code then 'Y' end) ori_agt_ind,  
		min(pol_iss_dt) frst_iss_dt
from 	allcus_allsts
where	pol_stat_cd in ('1','2','3','5','7') -- pol_stat_cd not in ('8','A','N','R','X') -- for CPM dashboard : 
--and DIST_CHNL_CD in ('01', '02', '08', '50', '*')
group by po_num
""")
agency_cus_inforce_v2.createOrReplaceTempView('agency_cus_inforce_v2')

# COMMAND ----------

# DBTITLE 1,Select customers in CPM campaigns
cpmcus = spark.sql(f"""
select	tgt_cust_id, min(batch_start_date) cpm_st_dt, if(collect_set(substr(cmpgn_id,5,3))[0]='ONB','Y','N') onb_ind, 
		if(	array_contains(collect_set(substr(cmpgn_id,5,3)),'PRD') or 
			array_contains(collect_set(substr(cmpgn_id,5,3)),'MED') or 
			array_contains(collect_set(substr(cmpgn_id,5,3)),'ILP'),'Y','N'	) prd_ind,
		'Y' cpm_ind
from	targetm_dm
where	true
	and batch_start_date between '{cmpgn_st_dt}' and '{end_date}'
	and	btch_id < 900000
	and	cmpgn_id like 'MIL-%-202%'
	and cmpgn_id not like '%POC%'
	and cmpgn_id not like '%RAND%'
group by tgt_cust_id
union
select	tgt_insrd_id tgt_cust_id, min(batch_start_date) cpm_st_dt, if(collect_set(substr(cmpgn_id,5,3))[0]='ONB','Y','N') onb_ind, 
		if(	array_contains(collect_set(substr(cmpgn_id,5,3)),'PRD') or 
			array_contains(collect_set(substr(cmpgn_id,5,3)),'MED') or 
			array_contains(collect_set(substr(cmpgn_id,5,3)),'ILP'),'Y','N'	) prd_ind,
		'Y' cpm_ind
from	targetm_dm
where	true
	and batch_start_date between '{cmpgn_st_dt}' and '{end_date}'
	and	btch_id < 900000
	and	cmpgn_id like 'MIL-%-202%'
	and cmpgn_id not like '%POC%'
	and cmpgn_id not like '%RAND%'
group by tgt_insrd_id
order by tgt_cust_id
""")
cpmcus.createOrReplaceTempView(f'cpmcus_{yr}')

# COMMAND ----------

# DBTITLE 1,Identify repurchased customers
cusrepo = spark.sql(f"""
select	a.po_num, 'Y' repo_ind
from	agency_cus_inforce_v2 a inner join
		agency_cus_sales_{yr} b on a.po_num=b.po_num and a.frst_iss_dt<b.new_sbmt_dt
group by a.po_num
""")
cusrepo.createOrReplaceTempView(f'cusrepo_{yr}')

# COMMAND ----------

# DBTITLE 1,Summarized customers with New/Existing and CPM/nonCPM break-down (no demographic info)
agency_cus_inforce_summary_v2 = spark.sql(f"""
select	ori.po_num po_num,
		ori.total_ape total_ape,
		ori.pol_cnt	pol_cnt,
		ori.agt_cnt	agt_cnt,
		ori.wa_code	wa_code,
		ori.sa_code	sa_code,
		ori.ori_agt_ind	ori_agt_ind,
		ori.frst_iss_dt	frst_iss_dt,
		cpm.cpm_st_dt cpm_st_dt,
		nvl(cpm.cpm_ind,'N') cpm_ind,
		if(ncus.po_num is null,'N','Y') new_ind,
		nvl(repo.repo_ind,'N') repo_ind,
		nvl(cpm.onb_ind,'N') onb_ind,
		nvl(cpm.prd_ind,'N') prd_ind
from 	agency_cus_inforce_v2 ori
	left join
		cusrepo_{yr} repo
	on ori.po_num=repo.po_num
	left join
		agency_cus_{yr} ncus
	on ori.po_num=ncus.po_num and ncus.frst_iss_dt<=ori.frst_iss_dt
	left join
		cpmcus_{yr} cpm
	on ori.po_num=cpm.tgt_cust_id
""")
agency_cus_inforce_summary_v2.createOrReplaceTempView('agency_cus_inforce_summary_v2')

# COMMAND ----------

# DBTITLE 1,Summarized Agency customers with 2021 sales details
noncpm_repo_1_v2 = spark.sql(f"""
select	ori.po_num po_num,
		ori.total_ape total_ape,
		ori.pol_cnt	pol_cnt,
		ori.agt_cnt	agt_cnt,
		ori.wa_code	wa_code,
		ori.sa_code	sa_code,
		ori.ori_agt_ind	ori_agt_ind,
		ori.frst_iss_dt	frst_iss_dt, 
		nsale.pol_num pol_num, 
		nsale.plan_code plan_code, 
		nsale.plan_code_desc plan_code_desc, 
		nsale.tot_ape tot_ape, 
		nsale.new_sbmt_dt new_sbmt_dt, 
		nsale.new_iss_dt new_iss_dt, 
		nsale.wa_code new_wa_code, 
		nsale.tenure agent_tenure, 
		nsale.agent_tier agent_tier,
		nsale.channel channel,
        nsale.pol_status pol_status,
        nsale.pol_trmn_dt pol_trmn_dt,
		ori.cpm_st_dt cpm_st_dt,
		floor(months_between(nsale.new_sbmt_dt,ori.frst_iss_dt)) mth_buy_new,
		floor(months_between(nsale.new_sbmt_dt,ori.cpm_st_dt)) mth_cpm_repo,		
		if(nsale.po_num is null,'N','Y') sale_ind,
		ori.cpm_ind cpm_ind,
		ori.new_ind new_ind,
		ori.repo_ind repo_ind,
		if(ori.wa_code=nsale.wa_code,'Y','N') same_repo_agt_ind,
		ori.onb_ind onb_ind,
		ori.prd_ind prd_ind
from 	agency_cus_inforce_summary_v2 ori
	left join
		agency_cus_sales_{yr} nsale
	on ori.po_num=nsale.po_num
""")
noncpm_repo_1_v2.createOrReplaceTempView('noncpm_repo_1_v2')

# COMMAND ----------

# DBTITLE 1,add segmentation and other info
 noncpm_repo_2_v2 = spark.sql(f"""
 select	b.po_num 			AS po_num
		,b.total_ape 		AS total_ape
		, b.pol_cnt 		AS pol_cnt
		, b.agt_cnt 		AS agt_cnt
		, b.wa_code 		AS wa_code
		, b.sa_code 		AS sa_code
		, b.ori_agt_ind 	AS ori_agt_ind
		, b.frst_iss_dt 	AS frst_iss_dt
		, b.pol_num 		AS pol_num
		, b.plan_code 		AS plan_code
		, b.plan_code_desc 	AS plan_code_desc
		, b.tot_ape			AS tot_ape
		, b.new_sbmt_dt		AS new_sbmt_dt
		, b.new_iss_dt		AS new_iss_dt
		, b.new_wa_code		AS new_wa_code
		, b.agent_tenure	AS agent_tenure	
		, b.agent_tier		AS agent_tier
		, b.channel			AS channel
        , b.pol_status      AS pol_status
        , b.pol_trmn_dt     AS pol_trmn_dt
		, b.cpm_st_dt		AS cpm_st_dt
		, b.mth_buy_new		AS mth_buy_new
		, b.mth_cpm_repo	AS mth_cpm_repo	
		, b.sale_ind		AS sale_ind
		, b.cpm_ind			AS cpm_ind
		, b.new_ind			AS new_ind
		, b.repo_ind		AS repo_ind
		, b.same_repo_agt_ind AS same_repo_agt_ind
		, b.onb_ind			AS onb_ind
		, b.prd_ind         AS prd_ind
		,a.cur_age cur_age
		,CASE
		WHEN a.cur_age < 18               THEN '1. <18'
		WHEN a.cur_age BETWEEN 18 AND 25  THEN '2. 18-25'
		WHEN a.cur_age BETWEEN 26 AND 35  THEN '3. 26-35'
		WHEN a.cur_age BETWEEN 36 AND 45  THEN '4. 36-45'
		WHEN a.cur_age BETWEEN 46 AND 55  THEN '5. 46-55'
		WHEN a.cur_age BETWEEN 56 AND 65  THEN '6. 56-65'
		WHEN a.cur_age > 65               THEN '7. >65'
		ELSE 'Unknown'
		END po_age_cur_grp
		,a.sex_code gender
		,CASE
		WHEN b.pol_cnt = 1               THEN '1'
		WHEN b.pol_cnt = 2               THEN '2'
		WHEN b.pol_cnt BETWEEN 3 AND 5   THEN '3-5'
		WHEN b.pol_cnt > 5               THEN '>5'
		ELSE 'NA'
		END                                                AS pol_cnt_cat
		,CASE
		WHEN b.total_APE <  20000         THEN '1) < 20M'
		WHEN b.total_APE >= 20000
		AND b.total_APE < 40000       THEN '2) 20-40M'
		WHEN b.total_APE >= 40000
		AND b.total_APE < 60000       THEN '3) 40-60M'
		WHEN b.total_APE >= 60000
		AND b.total_APE < 80000       THEN '4) 60-80M'
		WHEN b.total_APE >= 80000         THEN '5) 80M+'
		ELSE '6) Unknown'
		END                                                 AS total_APE_cat
		,CASE
		WHEN b.total_APE >= 20000
		AND b.total_APE < 65000
		AND FLOOR(DATEDIFF('2023-03-31',b.frst_iss_dt)/365.25) >= 10       THEN 'Silver'
		WHEN b.total_APE >= 65000
		AND b.total_APE < 80000       THEN 'Gold'
		WHEN b.total_APE >= 80000
		AND b.total_APE < 150000      THEN 'Diamond'
		WHEN b.total_APE >= 150000
		AND b.total_APE < 300000      THEN 'Platinum'
		WHEN b.total_APE >= 300000        THEN 'Platinum Elite'
		ELSE 'Not VIP'
		END                                                AS VIP_cat
		,FLOOR(DATEDIFF('{end_date}',b.frst_iss_dt)/365.25) AS tenure
		,NVL(a.mthly_incm,0)							   AS mthly_income
from	noncpm_repo_1_v2 b
	inner join
		tcustdm_daily a
	on	a.cli_num=b.po_num
 """)

# COMMAND ----------

# DBTITLE 1,Convert to Pandas for deep-dive analysis
pd_tmp = noncpm_repo_2_v2.toPandas()
pd_tmp.shape

# COMMAND ----------

# Convert date to datetime
pd_tmp['new_iss_dt'] = pd.to_datetime(pd_tmp['new_iss_dt'])

# Apply date range (select Dec's sales only)
pd_tmp_filtered = pd_tmp[(pd_tmp['new_iss_dt'] >= '2023-12-01') & (pd_tmp['new_iss_dt'] <= '2023-12-31')]

pd_dec = pd_tmp_filtered.groupby(['new_ind', 'channel']).agg(
    num_cus=('po_num', 'nunique'),
    num_pols=('pol_num', 'count')
).reset_index()

# Rename the columns
pd_dec = pd_dec.rename(columns={'po_num': 'num_cus', 'pol_num': 'num_pols'})

pd_dec

# COMMAND ----------

pd_all = pd_tmp.groupby(['new_ind','channel']).agg(
    num_cus=('po_num', 'nunique'),
    num_pols=('pol_num', 'count')
).reset_index()

pd_all = pd_all.rename(columns={'po_num': 'num_cus', 'pol_num': 'num_pols'})

pd_all

# COMMAND ----------

# Select only new sales incurred in 2023
pd_tmp_filtered = pd_tmp[(pd_tmp['new_ind'] == 'Y') & 
                        (pd_tmp['new_iss_dt'] <= '2023-12-31') &
                        (pd_tmp['pol_status'] == '03-Lapsed/Surrendered')]

# Convert date to string
pd_tmp_filtered['pol_trmn_dt'] = pd.to_datetime(pd_tmp_filtered['pol_trmn_dt'])
pd_tmp_filtered['pol_trmn_mth'] = pd_tmp_filtered['pol_trmn_dt'].dt.strftime('%Y-%m') #.str.slice(0, 7)

# Group by 'pol_status' and 'pol_trmn_mth'
pd_grp = pd_tmp_filtered.groupby(['pol_status', 'pol_trmn_mth']).agg(
    #num_cus=('po_num', 'nunique'),
    num_pols=('pol_num', 'count')
).reset_index()

# Pivot the table to have 'pol_status' as columns
pd_grp_pivot = pd_grp.pivot(index='pol_trmn_mth', columns='pol_status', values=['num_pols']).fillna(0)

pd_grp_pivot

# COMMAND ----------


