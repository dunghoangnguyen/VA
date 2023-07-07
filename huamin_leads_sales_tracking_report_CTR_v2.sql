SET hivevar:iss_st_dt='2023-03-01';	-- first date of sales tracking
SET hivevar:iss_ed_dt='2023-03-31'; -- last date of sales tracking period
SET hivevar:lead_dt='2023-01-01';

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
				CAST(SUM(trk.NEW_POL_APE)*1000*0.000043 AS DECIMAL(11,0)) NB_APE_USD,
				CAST(SUM(trk.NEW_POL_NBV)*1000*0.000043 AS DECIMAL(11,0)) NB_NBV_USD,
				COUNT(DISTINCT aws.CUST_ID) AWS_LEADS
		FROM	vn_published_campaign_db.TARGETM_DM tgt
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
						 FROM	vn_published_campaign_db.TRACKINGM trk
							INNER JOIN
								vn_published_campaign_db.CAMPP cpp
							ON	trk.cmpgn_id=cpp.cmpgn_id AND trk.btch_id=cpp.btch_id					
						 WHERE	trk.cmpgn_id NOT LIKE 'MIL%'	-- exclude LP
							AND	trk.cmpgn_id NOT LIKE '%RAND%'	-- exclude 10% Random HP
							AND	trk.cmpgn_ID NOT LIKE '%POC%'	-- exclude POC campaign
							AND trk.btch_id NOT LIKE '9%'
							--AND	trk.lead_gen_ind=1
							AND	new_cvg_stat NOT IN ('8','A','N','R','X')
							AND SUBSTR(trk.new_pol_plan_cd,1,3) NOT IN ('FDB','BIC','PN0')
							AND	((new_pol_cvg_typ='B' AND new_cvg_iss_dt BETWEEN ${iss_st_dt} AND ${iss_ed_dt})-- AND MONTHS_BETWEEN(LAST_DAY(ADD_MONTHS(CURRENT_DATE,-1)),cpp.batch_start_date)<6)
							  OR (new_pol_cvg_typ<>'B' AND new_cvg_eff_dt BETWEEN ${iss_st_dt} AND ${iss_ed_dt}))-- AND MONTHS_BETWEEN(LAST_DAY(ADD_MONTHS(CURRENT_DATE,-1)),cpp.batch_start_date)<6))
							
						) a
				) TRK
			ON	TGT.TGT_ID = TRK.TGT_ID --AND TGT.CMPGN_ID = TRK.CMPGN_ID
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
				FROM	vn_curated_adobe_pws_db.aws_cpm_daily
				lateral view explode (cmpgn_id_list) cmpgn_id_list as cpmid ) a
					INNER JOIN
						(SELECT	cpmid,
								MIN(hit_date) hit_date
						 FROM	vn_curated_adobe_pws_db.aws_cpm_daily
						 lateral view explode (cmpgn_id_list) cmpgn_id_list as cpmid
						 GROUP BY cpmid) b
					 ON	a.cpmid=b.cpmid		 
				) aws
			ON	tgt.TGT_ID = aws.TGT_ID AND tgt.CMPGN_ID = aws.CPMID
			 LEFT JOIN
				vn_processing_datamart_temp_db.leads_removal_additional_2303 rem ON trk.tgt_id=rem.tgt_id		
		WHERE	SUBSTR(tgt.cmpgn_id,1,3) = 'CTR'
			AND tgt.BTCH_ID NOT LIKE '9%'
			AND tgt.CMPGN_ID NOT LIKE '%POC%'
			AND tgt.CMPGN_ID NOT LIKE '%RAND%'
			AND	tgt.BATCH_START_DATE>=${lead_dt}	-- Lead start date
			AND (trk.rn	= 1 OR trk.rn IS NULL)
			AND	(trk.NEW_POL_SBMT_DT IS NULL OR trk.NEW_POL_SBMT_DT <= tgt.OFFR_END_DT)			
			AND tgt.OFFR_END_DT>=LAST_DAY(ADD_MONTHS(CURRENT_DATE,-1))	-- only batches not passed the VEA tracking period
			AND	rem.tgt_id IS NULL
		GROUP BY
				TGT.CMPGN_ID, TGT.OFFR_ST_DT
		) T
GROUP BY
		CAMPAIGN_NAME
HAVING CAMPAIGN_NAME <> 'Others'