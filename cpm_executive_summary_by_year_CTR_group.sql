SET hivevar:exRate = 23145;	-- make sure to check against CPM Tracking dashboard
SET hivevar:start_date = '2023-01-01';
SET hivevar:end_date = '2023-05-31';

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
		MIN(tgt.batch_start_date) batch_start_date,
		MAX(tgt.batch_end_date) batch_end_date,
		COUNT(DISTINCT tgt.TGT_CUST_ID) leads,
		--COUNT(DISTINCT tgt.TGT_WA_CD_1) joined_agt,		
		COUNT(DISTINCT trk.NEW_POL_CUST_ID) responses,
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) NB_CC,
		CAST(SUM(trk.NEW_POL_APE)*1000/${exRate} AS DECIMAL(11,0)) NB_APE_USD,
		CAST(SUM(trk.NEW_POL_APE)*1000/${exRate} AS DECIMAL(11,0)) /
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) CASE_SIZE_USD,																					  
		CAST(SUM(trk.NEW_POL_NBV)*1000/${exRate} AS DECIMAL(11,0)) NB_NBV_USD
FROM	vn_published_campaign_db.targetm_dm tgt
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
						trk.new_pol_nbv
		 		 FROM	vn_published_campaign_db.TRACKINGM trk
					INNER JOIN
						vn_published_campaign_db.CAMPP cpp
					ON	trk.cmpgn_id=cpp.cmpgn_id AND trk.btch_id=cpp.btch_id
		 		 WHERE	trk.cmpgn_id LIKE 'CTR-%'
					AND trk.btch_id NOT LIKE '9%'
				 	--AND	trk.lead_gen_ind=1				 	
				 	AND	trk.new_cvg_stat IS NOT NULL
				 	AND	trk.new_cvg_stat NOT IN ('8','A','N','R','X')
					AND trk.tgt_id IS NOT NULL
					AND	SUBSTR(trk.new_pol_plan_cd,1,3) NOT IN ('FDB','BIC','PN0')
					AND	(trk.new_cvg_iss_dt BETWEEN ${start_date} AND ${end_date}
					  OR trk.new_cvg_eff_dt BETWEEN ${start_date} AND ${end_date})
				 	--AND	trk.new_cvg_iss_dt>='2021-01-01'
				) a
		) TRK
	ON	TGT.TGT_ID = TRK.TGT_ID --AND TGT.CMPGN_ID = TRK.CMPGN_ID
WHERE	SUBSTR(tgt.cmpgn_id,1,3) = 'CTR'
	AND	tgt.cmpgn_id NOT LIKE '%-POC-%'
	AND (trk.rn	= 1 OR trk.rn IS NULL) 
	AND	tgt.batch_start_date>='2023-01-01'
	AND tgt.target_channel='Agency'
	AND tgt.btch_id NOT LIKE '9%'		-- [Exclude UCM of the existing campaign]
		
GROUP BY
		YEAR(trk.NEW_CVG_ISS_DT),
		tgt.cmpgn_id
ORDER BY
		YEAR(trk.NEW_CVG_ISS_DT),
		CMPGN_NAME;