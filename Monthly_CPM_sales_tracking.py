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

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

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
print("iss_st_dt:",iss_st_dt)
print("iss_ed_dt:",iss_ed_dt)
print("lead_dt:",lead_dt)
print("rpt_mth:",rpt_mth)

# COMMAND ----------

cpm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
aws_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ADOBE_PWS_DB/'

tbl_path1 = 'TARGETM_DM/'
tbl_path2 = 'TRACKINGM/'
tbl_path3 = 'AWS_CPM_DAILY/'

path_list = [cpm_path,aws_path]
tbl_list = [tbl_path1,tbl_path2,tbl_path3]

df_list = {}
cpm_df = load_parquet_files(cpm_path,tbl_list)
aws_df = load_parquet_files(aws_path,tbl_list)

df_list.update(cpm_df)
df_list.update(aws_df)

# COMMAND ----------

generate_temp_view(df_list)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Generate CPM sales data</strong>

# COMMAND ----------

aws_result = spark.sql(f"""                             

SELECT	--cpmid,
		agent_id agentid,
		CASE SUBSTR(page,55,32)
			WHEN 'customerportfoliomanagement' THEN 'CPM Landing Page'
			WHEN 'newcustomerpolicys' THEN 'Lead List Page'
			WHEN 'customerpotentiallist' THEN 'Lead List Page'			
			WHEN 'customerpolicysdetail' THEN 'Lead Info Page'
		END page,
		pageviews pageview,
		cmpgn_id_list,
		-- com_id comid,
		compaign_id compaignid,
		tgt_id,
		cust_id,
		loc_cd loc_code,
		br_code,
		rh_name,
		-- manager_name_0,
		hit_date
FROM	aws_cpm_daily
		--lateral view explode (cmpgn_id_list) cmpgn_id_list as cpmid) a
WHERE	tgt_id IS NOT NULL
""")

print("trackingm_result:",aws_result.count())
aws_result.display()

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
		SUM(NB_APE_USD)/SUM(NB_CC) CASE_SIZE_USD,
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
							AND	((new_pol_cvg_typ='B' AND new_cvg_iss_dt BETWEEN '{iss_st_dt}' AND '{iss_ed_dt}')
							  OR (new_pol_cvg_typ<>'B' AND new_cvg_eff_dt BETWEEN '{iss_st_dt}' AND '{iss_ed_dt}'))
						) a
				) TRK
			ON	TGT.TGT_ID = TRK.TGT_ID
			LEFT JOIN
				(SELECT	a.CPMID, a.TGT_ID, a.CUST_ID
				 FROM 	(SELECT	cpmid,
						agent_id agentid,
						CASE SUBSTR(page,55,32)
							WHEN 'customerportfoliomanagement' THEN 'CPM Landing Page'
							WHEN 'newcustomerpolicys' THEN 'Lead List Page'
							WHEN 'customerpotentiallist' THEN 'Lead List Page'			
							WHEN 'customerpolicysdetail' THEN 'Lead Info Page'
						END page,
						pageviews pageview,
						com_id comid,
						compaign_id compaignid,
						tgt_id,
						cust_id,
						loc_cd loc_code,
						br_code,
						rh_name,
						manager_name_0,
						hit_date
				FROM	aws_cpm_daily
				lateral view explode (cmpgn_id_list) cmpgn_id_list as cpmid ) a
					INNER JOIN
						(SELECT	cpmid,
								MIN(hit_date) hit_date
						 FROM	aws_cpm_daily
						 lateral view explode (cmpgn_id_list) cmpgn_id_list as cpmid
						 GROUP BY cpmid) b
					 ON	a.cpmid=b.cpmid	
				WHERE	TO_DATE(a.hit_date) BETWEEN '{iss_st_dt}' AND '{iss_ed_dt}'
				) aws
			ON	tgt.TGT_ID = aws.TGT_ID AND tgt.CMPGN_ID = aws.CPMID
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

huamin_monthly_data.display()

# COMMAND ----------


