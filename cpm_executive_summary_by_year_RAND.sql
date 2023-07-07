SET hivevar:exRate = 23145;	-- make sure to check against CPM Tracking dashboard
SET hivevar:start_date = '2023-01-01';
SET hivevar:end_date = '2023-04-30';	-- update on May 9th 2023

SELECT	YEAR(trk.NEW_CVG_ISS_DT) year,
		tgt.cmpgn_id cmpgn_id,
		CASE
		    WHEN tgt.cmpgn_id LIKE 'MIL-ONB-RAND%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Onboarding Trigger - Batch ',SUBSTR(tgt.cmpgn_id,17,2))
			WHEN tgt.cmpgn_id LIKE 'MIL-PRD-RAND%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Product Launch Trigger - Batch ',SUBSTR(tgt.cmpgn_id,17,2))
			WHEN tgt.cmpgn_id LIKE 'MIL-CTS-RAND%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Claims-to-Sell Trigger - Batch ',SUBSTR(tgt.cmpgn_id,17,2))
			WHEN tgt.cmpgn_id LIKE 'MIL-STS-RAND%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Service-to-Sell Trigger - Batch ',SUBSTR(tgt.cmpgn_id,17,2))
			WHEN tgt.cmpgn_id LIKE 'MIL-UCM-RAND%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' UCM Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-MAT-RAND%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Maturity Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-AVY-RAND%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Anniversary Trigger')
			WHEN tgt.cmpgn_id LIKE 'MIL-BDY-RAND%' THEN CONCAT(SUBSTR(tgt.cmpgn_id,9,4),' Birthday Trigger')
		END AS cmpgn_name,
		COUNT(DISTINCT tgt.TGT_CUST_ID) leads,
		COUNT(DISTINCT trk.NEW_POL_CUST_ID) all_responses,
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) NB_CC,
		CAST(SUM(trk.NEW_POL_APE)*1000/${exRate} AS DECIMAL(11,0)) ALL_APE_USD,
		CAST(SUM(trk.NEW_POL_APE)*1000/${exRate} AS DECIMAL(11,0)) /
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) CASE_SIZE_USD,
		CAST(SUM(trk.NEW_POL_NBV)*1000/${exRate} AS DECIMAL(11,0)) ALL_NBV_USD
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
						trk.new_pol_nbv,
				 		trk.offer_freeze_ind
		 		 FROM	vn_published_campaign_db.TRACKINGM trk
					INNER JOIN
						vn_published_campaign_db.CAMPP cpp
					ON	trk.cmpgn_id=cpp.cmpgn_id AND trk.btch_id=cpp.btch_id
		 		 WHERE	trk.cmpgn_id LIKE 'MIL-%RAND%'
					AND trk.btch_id NOT LIKE '9%'
				 	--AND	trk.lead_gen_ind=1
				 	AND	trk.new_cvg_stat IS NOT NULL
				 	AND	trk.new_cvg_stat NOT IN ('8','A','N','R','X')
					AND trk.tgt_id IS NOT NULL
					AND	trk.new_pol_plan_cd NOT IN ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001')
					AND	((trk.new_pol_cvg_typ='B' AND trk.new_cvg_iss_dt BETWEEN ${start_date} AND ${end_date})
					  OR (trk.new_pol_cvg_typ<>'B' AND trk.new_cvg_eff_dt BETWEEN ${start_date} AND ${end_date}))
				) a
		) TRK
	ON	TGT.TGT_ID = TRK.TGT_ID --AND TGT.CMPGN_ID = TRK.CMPGN_ID
	LEFT JOIN
	(SELECT	DISTINCT new_pol_cust_id
	 FROM	vn_published_campaign_db.trackingm
	 WHERE	cmpgn_id LIKE 'MIL-%RAND%'
	 	AND	btch_id NOT LIKE '9%'
	 	AND	new_pol_cvg_typ='B'
	) base
	ON TRK.NEW_POL_CUST_ID=BASE.new_pol_cust_id
WHERE	tgt.cmpgn_id LIKE 'MIL-%RAND%'
	AND (trk.rn	= 1 OR trk.rn IS NULL) 
	AND	tgt.batch_start_date >= ${start_date} --BETWEEN '2019-10-01' AND '2021-12-31'
	AND tgt.target_channel='Agency'
	AND tgt.btch_id NOT LIKE '9%'		-- [Exclude UCM of the existing campaign]		
GROUP BY
		YEAR(trk.NEW_CVG_ISS_DT),
		tgt.cmpgn_id
ORDER BY
		year,
		cmpgn_name;