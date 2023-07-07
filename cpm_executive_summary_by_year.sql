SET hivevar:exRate = 23000;	-- make sure to check against CPM Tracking dashboard
SET hivevar:start_date = '2022-01-01';
SET hivevar:end_date = '2022-04-30';

SELECT	YEAR(trk.NEW_CVG_ISS_DT) year,
		tgt.cmpgn_id cmpgn_id,
		CASE
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='UCM' THEN CONCAT_WS(' ',SUBSTR(tgt.cmpgn_id,9,4),'UCM Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='AVY' THEN CONCAT_WS(' ',SUBSTR(tgt.cmpgn_id,9,4),'Anniversary Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='BDY' THEN CONCAT_WS(' ',SUBSTR(tgt.cmpgn_id,9,4),'Birthday Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='MAT' THEN CONCAT_WS(' ',SUBSTR(tgt.cmpgn_id,9,4),'Maturity Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3) IN ('MED','ILP','PRD') THEN CONCAT_WS(' ',SUBSTR(tgt.cmpgn_id,9,4),'Product Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='ONB' THEN CONCAT_WS(' ',SUBSTR(tgt.cmgpn_id,9,4),'Onboarding Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='STS' THEN CONCAT_WS(' ',SUBSTR(tgt.cmgpn_id,9,4),'Service-to-Sales Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='CTS' THEN CONCAT_WS(' ',SUBSTR(tgt.cmgpn_id,9,4),'Claims-to-Sales Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='MKT' THEN CONCAT_WS(' ',SUBSTR(tgt.cmgpn_id,9,4),'Shopee Trigger')
			WHEN SUBSTR(tgt.cmpgn_id,5,3)='LPS' THEN CONCAT_WS(' ',SUBSTR(tgt.cmgpn_id,9,4),'Prelapsed Trigger')
		END AS cmpgn_name,
		MIN(tgt.batch_start_date) batch_start_date,
		MAX(tgt.batch_end_date) batch_end_date,
		COUNT(DISTINCT tgt.TGT_CUST_ID) leads,
		--COUNT(DISTINCT tgt.TGT_WA_CD_1) joined_agt,		
		COUNT(DISTINCT trk.NEW_POL_CUST_ID) responses,
		COUNT(DISTINCT CASE WHEN trk.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) NB_CC,
		CAST(SUM(trk.NEW_POL_APE+NVL(d.ape_topup,0))*1000/${exRate} AS DECIMAL(11,0)) NB_APE_USD,
		CAST(SUM(trk.NEW_POL_APE+NVL(d.ape_topup,0))*1000/${exRate} AS DECIMAL(11,0)) /
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
				 			 WHEN SUBSTR(trk.cmpgn_id,5,3)='MAT' THEN 2
				 			 WHEN SUBSTR(trk.cmpgn_id,5,3)='UCM' THEN 4
				 			 ELSE 3
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
		 		 WHERE	trk.cmpgn_id NOT LIKE 'CTR-%'
					AND trk.btch_id NOT LIKE '9%'
				 	--AND	lead_gen_ind=1				 	
				 	AND	trk.new_cvg_stat IS NOT NULL
				 	AND	trk.new_cvg_stat NOT IN ('8','A','N','R','X')
					AND trk.tgt_id IS NOT NULL
					AND	trk.new_pol_plan_cd NOT IN ('FDB01','BIC01','BIC02','BIC03','BIC04')
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
WHERE	SUBSTR(tgt.cmpgn_id,1,3) = 'MIL'
	AND	tgt.cmpgn_id NOT LIKE '%-POC-%'
	AND (trk.rn	= 1 OR trk.rn IS NULL) 
	--AND	tgt.batch_start_date>='2021-01-01'
	AND tgt.target_channel='Agency'
	AND tgt.btch_id NOT LIKE '9%'		-- [Exclude UCM of the existing campaign]
		
GROUP BY
		YEAR(trk.NEW_CVG_ISS_DT),
		tgt.cmpgn_id
ORDER BY
		YEAR(trk.NEW_CVG_ISS_DT),
		SUBSTR(tgt.cmpgn_id,9,8);