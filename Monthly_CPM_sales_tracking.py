# Databricks notebook source
# MAGIC %md
# MAGIC # Monthly CPM leads sales tracking for VEA report

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reports path: C:\Users\dung_nguyen_hoang\OneDrive - Manulife\Data Analytics\02. Use Cases\2019\1. CPM\01. DASHBOARD\01. Campaign Tracking\For Huamin
# MAGIC ###
# MAGIC <strong>Deadline for submission: 7th every month</strong>

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load defined functions</strong>

# COMMAND ----------

# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Initialize libl, params and tables</strong>

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

exRate = 23145
lead_dt = '2023-01-01'  # Change to the first date leads taking effective

x = 0 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
iss_ed_dt = last_day_of_x_months_ago.strftime('%Y-%m-%d')
iss_st_dt = last_day_of_x_months_ago.replace(day=1).strftime('%Y-%m-%d')
rpt_mth = iss_st_dt[0:4]+iss_st_dt[5:7]
print("exRate:",exRate)
print("iss_st_dt:",iss_st_dt)
print("iss_ed_dt:",iss_ed_dt)
print("lead_dt:",lead_dt)
print("rpt_mth:",rpt_mth)

# COMMAND ----------

cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
nbv_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAMPAIGN_FILEBASED_DB/'
aws_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_ADOBE_PWS_DB/'
rpt_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
lab_path = '/mnt/lab/vn/project/cpm/'

tbl_path1 = 'TARGETM_DM/'
tbl_path2 = 'TRACKINGM/'
tbl_path3 = 'AWS_CPM_DAILY/'
tbl_path4 = 'tabd_sbmt_pols/'
tbl_path5 = 'CAMPP/'

path_list = [cpm_path,aws_path,rpt_path,nbv_path]
tbl_list = [tbl_path1,tbl_path2,tbl_path3,tbl_path4,tbl_path5]

list_df = load_parquet_files(path_list, tbl_list)

# COMMAND ----------

generate_temp_view(list_df)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate CPM sales data</strong>

# COMMAND ----------

sql_string = """                             
SELECT	DISTINCT TGT_ID, CUST_ID
FROM 	aws_cpm_daily
WHERE	month_dt = '{rpt_mth}'
"""

aws_result = sql_to_df(sql_string, 1, spark)
#print("trackingm_result:",aws_result.count())
#aws_result.display()
aws_result.createOrReplaceTempView('aws_result')

# COMMAND ----------

huamin_monthly_data = spark.sql(f"""
                                
SELECT	CAMPAIGN_NAME,
		MIN(OFFER_START_DATE) OFFER_START_DATE,
		MAX(OFFER_END_DATE) OFFER_END_DATE,
		SUM(LEADS) LEADS,
		SUM(RESPONSES) RESPONSES,
		SUM(NB_CC) NB_CC,
		SUM(NB_PRODUCTS) NB_PRODUCTS,
		SUM(NB_APE_USD) NB_APE_USD,
		CAST(SUM(NB_APE_USD)/SUM(NB_CC) AS DECIMAL(9,2)) CASE_SIZE_USD,
		SUM(NB_NBV_USD) NB_NBV_USD,
		SUM(AWS_LEADS) AWS_LEADS
FROM	(SELECT	CASE 					
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) IN ('ONB','CTS','STS')  THEN CONCAT_WS(' ','CPM -',SUBSTR(TGT.CMPGN_ID,9,4),CONCAT(SUBSTR(TGT.CMPGN_ID,5,3),SUBSTR(TGT.CMPGN_ID,13,2)))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) = 'AVY' THEN CONCAT('Anniversary ',SUBSTR(TGT.CMPGN_ID,9,4))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) =  'MAT' THEN CONCAT('Maturity ',SUBSTR(TGT.CMPGN_ID,9,4))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) =  'BDY' THEN CONCAT('Birthday ',SUBSTR(TGT.CMPGN_ID,9,4))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) =  'MKT' THEN CONCAT('Digital ',SUBSTR(TGT.CMPGN_ID,9,4))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) =  'UCM' THEN CONCAT('UCM - ',YEAR(TGT.OFFR_ST_DT))
				ELSE
					CASE
						WHEN SUBSTR(TGT.CMPGN_ID,5,3) IN ('WMN','MED','ILP','PRD') THEN	CONCAT('Product Launch ',SUBSTR(TGT.CMPGN_ID,9,4))
						WHEN SUBSTR(TGT.CMPGN_ID,5,3) = ('REC') THEN CONCAT('Recycled Leads ',SUBSTR(TGT.CMPGN_ID,9,4))
						ELSE 'Others'
					END
				END CAMPAIGN_NAME,
				MIN(tgt.offr_st_dt) offer_start_date,
				MAX(tgt.offr_end_dt) offer_end_date,
				COUNT(DISTINCT tgt.TGT_CUST_ID) LEADS,
				COUNT(DISTINCT trk.NEW_POL_CUST_ID) RESPONSES,
				COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) NB_CC,
				COUNT(CASE WHEN trk.NEW_POL_CVG_TYP IN ('B','R','W') THEN trk.NEW_POL_NUM END) NB_PRODUCTS,
				CAST(SUM(trk.NEW_POL_APE)/23.145 AS DECIMAL(11,2)) NB_APE_USD,
				CAST(SUM(trk.NEW_POL_NBV)/23.145 AS DECIMAL(11,2)) NB_NBV_USD,
				COUNT(DISTINCT aws.CUST_ID) AWS_LEADS
		FROM	TARGETM_DM tgt
			LEFT JOIN
				(SELECT	a.*,
						ROW_NUMBER() OVER (PARTITION BY new_pol_num, new_pol_cvg_typ, new_pol_plan_cd, --new_pol_cust_id, 
						new_pol_insrd_id, new_pol_writing_agt ORDER BY new_pol_num, cmpgn_priority) rn
				 FROM	(SELECT	DISTINCT
								trk.cmpgn_id,
								CASE WHEN SUBSTR(trk.cmpgn_id,5,3)='ONB' THEN 1
									 WHEN SUBSTR(trk.cmpgn_id,5,3) IN ('MAT','AVY') THEN 2
									 WHEN SUBSTR(trk.cmpgn_id,5,3)='CTS' THEN 3
									 WHEN SUBSTR(trk.cmpgn_id,5,3)='UCM' THEN 5
									 ELSE 4
								END cmpgn_priority,
								trk.btch_id,
								trk.tgt_id,
								trk.new_pol_num, 
								trk.new_pol_cvg_typ, 
								trk.new_pol_plan_cd, 
								trk.new_pol_cust_id, 
								trk.new_pol_insrd_id, 
								trk.new_pol_writing_agt,
								trk.new_cvg_stat,
								trk.new_pol_sbmt_dt,
								trk.new_cvg_iss_dt,
								trk.new_cvg_eff_dt,
								trk.new_pol_ape,
								trk.new_pol_nbv,
								trk.lead_gen_ind
						 FROM	TRACKINGM trk
						 WHERE	trk.cmpgn_id NOT LIKE 'CTR%'	-- exclude LP
							AND	trk.cmpgn_id NOT LIKE '%RAND%'	-- exclude 10% Random HP
							AND	trk.cmpgn_ID NOT LIKE '%POC%'	-- exclude POC campaign
							AND trk.btch_id NOT LIKE '9%'
							AND	new_cvg_stat NOT IN ('8','A','N','R','X')
							AND SUBSTR(trk.new_pol_plan_cd,1,3) NOT IN ('FDB','BIC','PN0')
							AND	new_cvg_eff_dt BETWEEN '{iss_st_dt}' AND '{iss_ed_dt}'
						) a
				) TRK
			ON	TGT.TGT_ID = TRK.TGT_ID
			LEFT JOIN
				aws_result aws
			ON	tgt.TGT_ID = aws.TGT_ID
		WHERE	SUBSTR(tgt.cmpgn_id,1,3) = 'MIL'
			AND tgt.BTCH_ID NOT LIKE '9%'
			AND tgt.CMPGN_ID NOT LIKE '%POC%'
			AND tgt.CMPGN_ID NOT LIKE '%RAND%'
			AND	tgt.BATCH_START_DATE>='{lead_dt}'	-- Lead start date
			AND (trk.rn	= 1 OR trk.rn IS NULL)
			AND	(trk.NEW_POL_SBMT_DT IS NULL OR trk.NEW_POL_SBMT_DT <= tgt.OFFR_END_DT)			
			AND tgt.OFFR_END_DT>=LAST_DAY(ADD_MONTHS(CURRENT_DATE,-1))	-- only batches not passed the VEA tracking period
		GROUP BY
				TGT.CMPGN_ID, TGT.OFFR_ST_DT
		) T
GROUP BY
		CAMPAIGN_NAME
HAVING CAMPAIGN_NAME <> 'Others'                                
""")

huamin_monthly_data = huamin_monthly_data.toDF(*[col.lower() for col in huamin_monthly_data.columns])

# COMMAND ----------

huamin_monthly_data.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f'{lab_path}Huamin_monthly_data')
huamin_monthly_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Huamin monthly CTR</strong>

# COMMAND ----------

huamin_CTR_data = spark.sql(f"""
                                
SELECT	CAMPAIGN_NAME,
		MIN(OFFER_START_DATE) OFFER_START_DATE,
		MAX(OFFER_END_DATE) OFFER_END_DATE,
		SUM(LEADS) LEADS,
		SUM(RESPONSES) RESPONSES,
		SUM(NB_CC) NB_CC,
		SUM(NB_PRODUCTS) NB_PRODUCTS,
		SUM(NB_APE_USD) NB_APE_USD,
		CAST(SUM(NB_APE_USD)/SUM(NB_CC) AS DECIMAL(9,2)) CASE_SIZE_USD,
		SUM(NB_NBV_USD) NB_NBV_USD,
		SUM(AWS_LEADS) AWS_LEADS
FROM	(SELECT	CASE 					
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) IN ('ONB','CTS','STS')  THEN CONCAT_WS(' ','CPM -',SUBSTR(TGT.CMPGN_ID,9,4),CONCAT(SUBSTR(TGT.CMPGN_ID,5,3),SUBSTR(TGT.CMPGN_ID,13,2)))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) = 'AVY' THEN CONCAT('Anniversary ',SUBSTR(TGT.CMPGN_ID,9,4))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) =  'MAT' THEN CONCAT('Maturity ',SUBSTR(TGT.CMPGN_ID,9,4))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) =  'BDY' THEN CONCAT('Birthday ',SUBSTR(TGT.CMPGN_ID,9,4))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) =  'MKT' THEN CONCAT('Digital ',SUBSTR(TGT.CMPGN_ID,9,4))
					WHEN SUBSTR(TGT.CMPGN_ID,5,3) =  'UCM' THEN CONCAT('UCM - ',YEAR(TGT.OFFR_ST_DT))
				ELSE
					CASE
						WHEN SUBSTR(TGT.CMPGN_ID,5,3) IN ('WMN','MED','ILP','PRD') THEN	CONCAT('Product Launch ',SUBSTR(TGT.CMPGN_ID,9,4))
						WHEN SUBSTR(TGT.CMPGN_ID,5,3) = ('REC') THEN CONCAT('Recycled Leads ',SUBSTR(TGT.CMPGN_ID,9,4))
						ELSE 'Others'
					END
				END CAMPAIGN_NAME,
				MIN(tgt.offr_st_dt) offer_start_date,
				MAX(tgt.offr_end_dt) offer_end_date,
				COUNT(DISTINCT tgt.TGT_CUST_ID) LEADS,
				COUNT(DISTINCT trk.NEW_POL_CUST_ID) RESPONSES,
				COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) NB_CC,
				COUNT(CASE WHEN trk.NEW_POL_CVG_TYP IN ('B','R','W') THEN trk.NEW_POL_NUM END) NB_PRODUCTS,
				CAST(SUM(trk.NEW_POL_APE)/23.145 AS DECIMAL(11,2)) NB_APE_USD,
				CAST(SUM(trk.NEW_POL_NBV)/23.145 AS DECIMAL(11,2)) NB_NBV_USD,
				COUNT(DISTINCT aws.CUST_ID) AWS_LEADS
		FROM	TARGETM_DM tgt
			LEFT JOIN
				(SELECT	a.*,
						ROW_NUMBER() OVER (PARTITION BY new_pol_num, new_pol_cvg_typ, new_pol_plan_cd, --new_pol_cust_id, 
						new_pol_insrd_id, new_pol_writing_agt ORDER BY new_pol_num, cmpgn_priority) rn
				 FROM	(SELECT	DISTINCT
								trk.cmpgn_id,
								CASE WHEN SUBSTR(trk.cmpgn_id,5,3)='ONB' THEN 1
									 WHEN SUBSTR(trk.cmpgn_id,5,3) IN ('MAT','AVY') THEN 2
									 WHEN SUBSTR(trk.cmpgn_id,5,3)='CTS' THEN 3
									 WHEN SUBSTR(trk.cmpgn_id,5,3)='UCM' THEN 5
									 ELSE 4
								END cmpgn_priority,
								trk.btch_id,
								trk.tgt_id,
								trk.new_pol_num, 
								trk.new_pol_cvg_typ, 
								trk.new_pol_plan_cd, 
								trk.new_pol_cust_id, 
								trk.new_pol_insrd_id, 
								trk.new_pol_writing_agt,
								trk.new_cvg_stat,
								trk.new_pol_sbmt_dt,
								trk.new_cvg_iss_dt,
								trk.new_cvg_eff_dt,
								trk.new_pol_ape,
								trk.new_pol_nbv,
								trk.lead_gen_ind
						 FROM	TRACKINGM trk
						 WHERE	trk.cmpgn_id NOT LIKE 'MIL%'	-- exclude LP
							AND	trk.cmpgn_id NOT LIKE '%RAND%'	-- exclude 10% Random HP
							AND	trk.cmpgn_ID NOT LIKE '%POC%'	-- exclude POC campaign
							AND trk.btch_id NOT LIKE '9%'
							AND	new_cvg_stat NOT IN ('8','A','N','R','X')
							AND SUBSTR(trk.new_pol_plan_cd,1,3) NOT IN ('FDB','BIC','PN0')
							AND	new_cvg_eff_dt BETWEEN '{iss_st_dt}' AND '{iss_ed_dt}'
						) a
				) TRK
			ON	TGT.TGT_ID = TRK.TGT_ID
			LEFT JOIN
				aws_result aws
			ON	tgt.TGT_ID = aws.TGT_ID
		WHERE	SUBSTR(tgt.cmpgn_id,1,3) = 'CTR'
			AND tgt.BTCH_ID NOT LIKE '9%'
			AND tgt.CMPGN_ID NOT LIKE '%POC%'
			AND tgt.CMPGN_ID NOT LIKE '%RAND%'
			AND	tgt.BATCH_START_DATE>='{lead_dt}'	-- Lead start date
			AND (trk.rn	= 1 OR trk.rn IS NULL)
			AND	(trk.NEW_POL_SBMT_DT IS NULL OR trk.NEW_POL_SBMT_DT <= tgt.OFFR_END_DT)			
			AND tgt.OFFR_END_DT>=LAST_DAY(ADD_MONTHS(CURRENT_DATE,-1))	-- only batches not passed the VEA tracking period
		GROUP BY
				TGT.CMPGN_ID, TGT.OFFR_ST_DT
		) T
GROUP BY
		CAMPAIGN_NAME
HAVING CAMPAIGN_NAME <> 'Others'                                
""")

huamin_CTR_data = huamin_CTR_data.toDF(*[col.lower() for col in huamin_CTR_data.columns])
huamin_CTR_data.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f'{lab_path}Huamin_monthly_data_CTR')
huamin_CTR_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>CPM EXECUTIVE SUMMARY</strong>

# COMMAND ----------

tgt_string = """
select 	cmpgn_id, count(distinct tgt_cust_id) leads
from 	targetm_dm
where 	substr(cmpgn_id,1,3) = 'MIL'
	AND	substr(cmpgn_id,9,3) NOT IN ('POC','RAN')
	AND	batch_start_date >= '{lead_dt}'
	AND target_channel='Agency'
	AND btch_id NOT LIKE '9%'
group by cmpgn_id 
"""

trk_string = """
SELECT	YEAR(trk.NEW_CVG_ISS_DT) year,
		tgt.cmpgn_id cmpgn_id,
		CASE
		    WHEN tgt.cmpgn_id LIKE 'MIL-ONB-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Onboarding Trigger - Batch ',SUBSTR(tgt.cmpgn_id,13,2))
			WHEN tgt.cmpgn_id='MIL-WMN-20200302' THEN '2020 Product Launch Trigger - Batch 1'
			WHEN tgt.cmpgn_id='MIL-WMN-201008' THEN '2020 Product Launch Trigger - Batch 2'
			WHEN tgt.cmpgn_id='MIL-MED-20210621' THEN '2021 Product Launch Trigger - Billion Cash'
			WHEN tgt.cmpgn_id='MIL-ILP-20210809' THEN '2021 Product Launch Trigger - New ILP'
			WHEN tgt.cmpgn_id='MIL-MED-20211026' THEN '2021 Product Launch Trigger - Recycled'
			WHEN tgt.cmpgn_id='MIL-PRD-20220101' THEN '2022 Product Launch Trigger - Batch 1'
			WHEN tgt.cmpgn_id='MIL-PRD-20220315' THEN '2022 Product Launch Trigger - Batch 2'
			WHEN tgt.cmpgn_id='MIL-REC-20221015' THEN '2022 Recycled Leads'
			WHEN tgt.cmpgn_id LIKE 'MIL-CTS-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Claims-to-Sell Trigger - Batch ',SUBSTR(tgt.cmpgn_id,13,2))
			WHEN tgt.cmpgn_id LIKE 'MIL-STS-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Service-to-Sell Trigger - Batch ',SUBSTR(tgt.cmpgn_id,13,2))
			WHEN tgt.cmpgn_id LIKE 'MIL-UCM-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' UCM Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-MAT-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Maturity Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-AVY-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Anniversary Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-BDY-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Birthday Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-LPS-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Prelapsed Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-NPS-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' tNPS Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-REI-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Reinstatement Trigger')
		END AS cmpgn_name,
		--COUNT(DISTINCT tgt.TGT_CUST_ID) leads,
		COUNT(DISTINCT trk.NEW_POL_CUST_ID) all_responses,
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) NB_CC,
		CAST(SUM(trk.NEW_POL_APE+NVL(d.ape_topup,0))*1000/{exRate} AS DECIMAL(11,0)) ALL_APE_USD,
		CEILING(CAST(SUM(trk.NEW_POL_APE+NVL(d.ape_topup,0))*1000/{exRate} AS DECIMAL(11,0)) /
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END)) CASE_SIZE_USD,	
		CAST(SUM(trk.NEW_POL_NBV)*1000/{exRate} AS DECIMAL(11,0)) ALL_NBV_USD
FROM	targetm_dm tgt
	INNER JOIN
		(SELECT	a.*,
				ROW_NUMBER() OVER (PARTITION BY new_pol_num, new_pol_cvg_typ, new_pol_plan_cd, --new_pol_cust_id,
				new_pol_insrd_id, new_pol_writing_agt ORDER BY new_pol_num, cmpgn_priority) rn
		 FROM	(SELECT	DISTINCT
						trk.cmpgn_id,
				 		CASE WHEN SUBSTR(trk.cmpgn_id,5,3)='ONB' THEN 1
				 			 WHEN SUBSTR(trk.cmpgn_id,5,3) IN ('MAT','AVY') THEN 2
							 WHEN SUBSTR(trk.cmpgn_id,5,3)='CTS' THEN 3
				 			 WHEN SUBSTR(trk.cmpgn_id,5,3)='UCM' THEN 5
				 			 ELSE 4
				 		END cmpgn_priority,
						trk.btch_id,
						trk.tgt_id,
						trk.new_pol_num, 
						trk.new_pol_cvg_typ, 
						trk.new_pol_plan_cd, 
						trk.new_pol_cust_id, 
						trk.new_pol_insrd_id, 
						trk.new_pol_writing_agt,
						trk.new_cvg_stat,
						trk.new_pol_sbmt_dt,
						trk.new_cvg_iss_dt,
						trk.new_cvg_eff_dt,
						trk.new_pol_ape,
						trk.new_pol_nbv,
				 		trk.offer_freeze_ind
		 		 FROM	TRACKINGM trk
					INNER JOIN
						CAMPP cpp
					ON	trk.cmpgn_id=cpp.cmpgn_id AND trk.btch_id=cpp.btch_id
		 		 WHERE	trk.cmpgn_id NOT LIKE 'CTR-%'
					AND trk.btch_id NOT LIKE '9%'
				 	--AND	trk.assign_agent_ind=1
				 	AND	trk.new_cvg_stat IS NOT NULL
				 	AND	trk.new_cvg_stat NOT IN ('8','A','N','R','X')
					AND trk.tgt_id IS NOT NULL
					AND	trk.new_pol_plan_cd NOT IN ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
					AND	((trk.new_pol_cvg_typ='B' AND trk.new_cvg_iss_dt BETWEEN cpp.offr_st_dt AND '{iss_ed_dt}')
					  OR (trk.new_pol_cvg_typ<>'B' AND trk.new_cvg_eff_dt BETWEEN cpp.offr_st_dt AND '{iss_ed_dt}'))
				) a
		) TRK
	ON	TGT.TGT_ID = TRK.TGT_ID
	LEFT JOIN
  	(SELECT	pol_num,
	 	'B' as cvg_typ,
		SUM(DISTINCT fyp_top_up*0.1) ape_topup,	--[ver10]
		SUM(DISTINCT fyp+(fyp_top_up*0.06)) new_fyp
	 FROM 	tabd_sbmt_pols
	 WHERE	layer_typ IN ('B','I')
	 GROUP BY
		pol_num
	) d
	ON TRK.NEW_POL_NUM=d.POL_NUM AND TRK.NEW_POL_CVG_TYP=d.CVG_TYP
	LEFT JOIN
	(SELECT	DISTINCT new_pol_cust_id
	 FROM	trackingm
	 WHERE	SUBSTR(cmpgn_id,1,3) = 'MIL'
	 	AND	SUBSTR(cmpgn_id,9,3) NOT IN ('POC','RAN')
	 	AND	btch_id NOT LIKE '9%'
	 	AND	new_pol_cvg_typ='B'
	) base
	ON TRK.NEW_POL_CUST_ID=BASE.new_pol_cust_id
WHERE	SUBSTR(tgt.cmpgn_id,1,3) = 'MIL'
	AND	SUBSTR(tgt.cmpgn_id,9,3) NOT IN ('POC','RAN')
	AND (trk.rn	= 1 OR trk.rn IS NULL) 
	AND	tgt.batch_start_date >= '{lead_dt}' --BETWEEN '2019-10-01' AND '2021-12-31'
	AND tgt.target_channel='Agency'
	AND tgt.btch_id NOT LIKE '9%'		-- [Exclude UCM of the existing campaign]		
GROUP BY
		YEAR(trk.NEW_CVG_ISS_DT),
		tgt.cmpgn_id
ORDER BY
		year,
		cmpgn_name
"""

tgtDF = sql_to_df(tgt_string, 1, spark)
trackingDF = sql_to_df(trk_string, 1, spark)

cpm_sales_tracking = tgtDF.alias('tgt')\
    .join(trackingDF.alias('trk'), 
          on='cmpgn_id', how='left')\
    .select(
        'year',
        'trk.cmpgn_id',
        'cmpgn_name',
        'leads',
        'all_responses',
        'nb_cc',
        'all_ape_usd',
        'case_size_usd',
        'all_nbv_usd'
    )\
    .where(col('year').isNotNull())

# COMMAND ----------

#cpm_sales_tracking.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f'{lab_path}cpm_executive_summary_by_year')
cpm_sales_tracking.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Control Group</strong>

# COMMAND ----------

tgtCTR_string = """
select 	cmpgn_id, count(distinct tgt_cust_id) leads
from 	targetm_dm
where 	substr(cmpgn_id,1,3) = 'CTR'
	AND	substr(cmpgn_id,9,3) NOT IN ('POC','RAN')
	AND	batch_start_date >= '{lead_dt}'
	AND target_channel='Agency'
	AND btch_id NOT LIKE '9%'
group by cmpgn_id 
"""

trkCTR_string = """
SELECT	YEAR(trk.NEW_CVG_ISS_DT) year,
		tgt.cmpgn_id cmpgn_id,
		CASE
		    WHEN tgt.cmpgn_id LIKE 'CTR-ONB-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Onboarding Trigger - Batch ',SUBSTR(tgt.cmpgn_id,13,2))
			WHEN tgt.cmpgn_id='CTR-WMN-20200302' THEN '2020 Product Launch Trigger - Batch 1'
			WHEN tgt.cmpgn_id='CTR-WMN-201008' THEN '2020 Product Launch Trigger - Batch 2'
			WHEN tgt.cmpgn_id='CTR-MED-20210621' THEN '2021 Product Launch Trigger - Billion Cash'
			WHEN tgt.cmpgn_id='CTR-ILP-20210809' THEN '2021 Product Launch Trigger - New ILP'
			WHEN tgt.cmpgn_id='CTR-MED-20211026' THEN '2021 Product Launch Trigger - Recycled'
			WHEN tgt.cmpgn_id='CTR-PRD-20220101' THEN '2022 Product Launch Trigger - Batch 1'
			WHEN tgt.cmpgn_id='CTR-PRD-20220315' THEN '2022 Product Launch Trigger - Batch 2'
			WHEN tgt.cmpgn_id='CTR-REC-20221015' THEN '2022 Recycled Leads'
			WHEN tgt.cmpgn_id LIKE 'CTR-CTS-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Claims-to-Sell Trigger - Batch ',SUBSTR(tgt.cmpgn_id,13,2))
			WHEN tgt.cmpgn_id LIKE 'CTR-STS-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Service-to-Sell Trigger - Batch ',SUBSTR(tgt.cmpgn_id,13,2))
			WHEN tgt.cmpgn_id LIKE 'CTR-UCM-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' UCM Trigger')
			WHEN tgt.cmpgn_id LIKE 'CTR-MAT-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Maturity Trigger')
			WHEN tgt.cmpgn_id LIKE 'CTR-AVY-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Anniversary Trigger')
			WHEN tgt.cmpgn_id LIKE 'CTR-BDY-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Birthday Trigger')
			WHEN tgt.cmpgn_id LIKE 'CTR-LPS-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Prelapsed Trigger')
			WHEN tgt.cmpgn_id LIKE 'CTR-NPS-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' tNPS Trigger')
			WHEN tgt.cmpgn_id LIKE 'CTR-REI-20%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Reinstatement Trigger')
		END AS cmpgn_name,
		--COUNT(DISTINCT tgt.TGT_CUST_ID) leads,
		COUNT(DISTINCT trk.NEW_POL_CUST_ID) all_responses,
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) NB_CC,
		CAST(SUM(trk.NEW_POL_APE+NVL(d.ape_topup,0))*1000/{exRate} AS DECIMAL(11,0)) ALL_APE_USD,
		CEILING(CAST(SUM(trk.NEW_POL_APE+NVL(d.ape_topup,0))*1000/{exRate} AS DECIMAL(11,0)) /
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END)) CASE_SIZE_USD,	
		CAST(SUM(trk.NEW_POL_NBV)*1000/{exRate} AS DECIMAL(11,0)) ALL_NBV_USD
FROM	targetm_dm tgt
	INNER JOIN
		(SELECT	a.*,
				ROW_NUMBER() OVER (PARTITION BY new_pol_num, new_pol_cvg_typ, new_pol_plan_cd, --new_pol_cust_id,
				new_pol_insrd_id, new_pol_writing_agt ORDER BY new_pol_num, cmpgn_priority) rn
		 FROM	(SELECT	DISTINCT
						trk.cmpgn_id,
				 		CASE WHEN SUBSTR(trk.cmpgn_id,5,3)='ONB' THEN 1
				 			 WHEN SUBSTR(trk.cmpgn_id,5,3) IN ('MAT','AVY') THEN 2
							 WHEN SUBSTR(trk.cmpgn_id,5,3)='CTS' THEN 3
				 			 WHEN SUBSTR(trk.cmpgn_id,5,3)='UCM' THEN 5
				 			 ELSE 4
				 		END cmpgn_priority,
						trk.btch_id,
						trk.tgt_id,
						trk.new_pol_num, 
						trk.new_pol_cvg_typ, 
						trk.new_pol_plan_cd, 
						trk.new_pol_cust_id, 
						trk.new_pol_insrd_id, 
						trk.new_pol_writing_agt,
						trk.new_cvg_stat,
						trk.new_pol_sbmt_dt,
						trk.new_cvg_iss_dt,
						trk.new_cvg_eff_dt,
						trk.new_pol_ape,
						trk.new_pol_nbv,
				 		trk.offer_freeze_ind
		 		 FROM	TRACKINGM trk
					INNER JOIN
						CAMPP cpp
					ON	trk.cmpgn_id=cpp.cmpgn_id AND trk.btch_id=cpp.btch_id
		 		 WHERE	trk.cmpgn_id LIKE 'CTR-%'
					AND trk.btch_id NOT LIKE '9%'
				 	--AND	trk.assign_agent_ind=1
				 	AND	trk.new_cvg_stat IS NOT NULL
				 	AND	trk.new_cvg_stat NOT IN ('8','A','N','R','X')
					AND trk.tgt_id IS NOT NULL
					AND	trk.new_pol_plan_cd NOT IN ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
					AND	((trk.new_pol_cvg_typ='B' AND trk.new_cvg_iss_dt BETWEEN cpp.offr_st_dt AND '{iss_ed_dt}')
					  OR (trk.new_pol_cvg_typ<>'B' AND trk.new_cvg_eff_dt BETWEEN cpp.offr_st_dt AND '{iss_ed_dt}'))
				) a
		) TRK
	ON	TGT.TGT_ID = TRK.TGT_ID
	LEFT JOIN
  	(SELECT	pol_num,
	 	'B' as cvg_typ,
		SUM(DISTINCT fyp_top_up*0.1) ape_topup,	--[ver10]
		SUM(DISTINCT fyp+(fyp_top_up*0.06)) new_fyp
	 FROM 	tabd_sbmt_pols
	 WHERE	layer_typ IN ('B','I')
	 GROUP BY
		pol_num
	) d
	ON TRK.NEW_POL_NUM=d.POL_NUM AND TRK.NEW_POL_CVG_TYP=d.CVG_TYP
	LEFT JOIN
	(SELECT	DISTINCT new_pol_cust_id
	 FROM	trackingm
	 WHERE	SUBSTR(cmpgn_id,1,3) = 'CTR'
	 	AND	SUBSTR(cmpgn_id,9,3) NOT IN ('POC','RAN')
	 	AND	btch_id NOT LIKE '9%'
	 	AND	new_pol_cvg_typ='B'
	) base
	ON TRK.NEW_POL_CUST_ID=BASE.new_pol_cust_id
WHERE	SUBSTR(tgt.cmpgn_id,1,3) = 'CTR'
	AND	SUBSTR(tgt.cmpgn_id,9,3) NOT IN ('POC','RAN')
	AND (trk.rn	= 1 OR trk.rn IS NULL) 
	AND	tgt.batch_start_date >= '{lead_dt}' --BETWEEN '2019-10-01' AND '2021-12-31'
	AND tgt.target_channel='Agency'
	AND tgt.btch_id NOT LIKE '9%'		-- [Exclude UCM of the existing campaign]		
GROUP BY
		YEAR(trk.NEW_CVG_ISS_DT),
		tgt.cmpgn_id
ORDER BY
		year,
		cmpgn_name
"""

tgtCTRDF = sql_to_df(tgtCTR_string, 1, spark)
trkCTRDF = sql_to_df(trkCTR_string, 1, spark)

ctr_sales_tracking = tgtCTRDF.alias('tgt')\
    .join(trkCTRDF.alias('trk'), 
          on='cmpgn_id', how='left')\
    .select(
        'year',
        'trk.cmpgn_id',
        'cmpgn_name',
        'leads',
        'all_responses',
        'nb_cc',
        'all_ape_usd',
        'case_size_usd',
        'all_nbv_usd'
    )\
    .where(col('year').isNotNull())

# COMMAND ----------

ctr_sales_tracking.coalesce(1).write.mode('overwrite').option('header', 'true').csv(f'{lab_path}cpm_executive_summary_by_year_CTR')
ctr_sales_tracking.display()
