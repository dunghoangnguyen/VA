SET hivevar:exRate = 23145;	-- make sure to check against CPM Tracking dashboard
SET hivevar:start_date = '2023-01-01';
SET hivevar:end_date = '2023-05-31';	-- update on May 9th 2023

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
		COUNT(DISTINCT tgt.TGT_CUST_ID) leads,
		COUNT(DISTINCT trk.NEW_POL_CUST_ID) all_responses,
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) NB_CC,
		CAST(SUM(trk.NEW_POL_APE+NVL(d.ape_topup,0))*1000/${exRate} AS DECIMAL(11,0)) ALL_APE_USD,
		CAST(SUM(trk.NEW_POL_APE+NVL(d.ape_topup,0))*1000/${exRate} AS DECIMAL(11,0)) /
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
		 		 WHERE	trk.cmpgn_id NOT LIKE 'CTR-%'
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
  	(SELECT	pol_num,
	 	'B' as cvg_typ,
		SUM(DISTINCT fyp_top_up*0.1) ape_topup,	--[ver10]
		SUM(DISTINCT fyp+(fyp_top_up*0.06)) new_fyp
	 FROM 	vn_published_reports_db.tabd_sbmt_pols
	 WHERE	layer_typ IN ('B','I')
	 GROUP BY
		pol_num
	) d
	ON TRK.NEW_POL_NUM=d.POL_NUM AND TRK.NEW_POL_CVG_TYP=d.CVG_TYP
	LEFT JOIN
	(SELECT	DISTINCT new_pol_cust_id
	 FROM	vn_published_campaign_db.trackingm
	 WHERE	SUBSTR(cmpgn_id,1,3) = 'MIL'
	 	AND	SUBSTR(cmpgn_id,9,3) NOT IN ('POC','RAN')
	 	AND	btch_id NOT LIKE '9%'
	 	AND	new_pol_cvg_typ='B'
	) base
	ON TRK.NEW_POL_CUST_ID=BASE.new_pol_cust_id
WHERE	SUBSTR(tgt.cmpgn_id,1,3) = 'MIL'
	AND	SUBSTR(tgt.cmpgn_id,9,3) NOT IN ('POC','RAN')
	AND (trk.rn	= 1 OR trk.rn IS NULL) 
	--AND	tgt.batch_start_date >= ${start_date} --BETWEEN '2019-10-01' AND '2021-12-31'
	AND tgt.target_channel='Agency'
	AND tgt.btch_id NOT LIKE '9%'		-- [Exclude UCM of the existing campaign]		
GROUP BY
		YEAR(trk.NEW_CVG_ISS_DT),
		tgt.cmpgn_id
--HAVING year<2022
ORDER BY
		year,
		cmpgn_name;