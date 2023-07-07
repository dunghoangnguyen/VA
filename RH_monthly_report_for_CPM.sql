SET hivevar:st_dt='2023-01-01';
SET hivevar:ed_dt='2023-05-31';
SET hivevar:lead_yr=2023;

----- CPM Region -----
SELECT	TGT_AGT_REGIONAL_HEAD region,
		COUNT(DISTINCT TGT_AGT_CODE) joined_agt,
		COUNT(DISTINCT TGT_CUST_ID) leads
FROM	vn_published_campaign_db.targetm_dm
WHERE	YEAR(batch_start_date)=${lead_yr}-- BETWEEN ${st_dt} AND ${ed_dt}
	AND	cmpgn_id LIKE 'MIL-%-202%'
	AND SUBSTR(cmpgn_id,9,3) NOT IN ('POC','RAN')
	AND	btch_id not like '9%'
GROUP BY									   
	TGT_AGT_REGIONAL_HEAD

----- CPM Region CT -----
SELECT	TGT_AGT_REGIONAL_HEAD region,
		SUBSTR(cmpgn_id,5,3) cmp_typ,
		COUNT(DISTINCT TGT_AGT_CODE) joined_agt,
		COUNT(DISTINCT TGT_CUST_ID) leads
FROM	vn_published_campaign_db.targetm_dm
WHERE	YEAR(batch_start_date)=${lead_yr}-- BETWEEN ${st_dt} AND ${ed_dt}
	AND	cmpgn_id LIKE 'MIL-%-202%'
	AND SUBSTR(cmpgn_id,9,3) NOT IN ('POC','RAN')
	AND	btch_id not like '9%'
GROUP BY									   
	TGT_AGT_REGIONAL_HEAD,
	SUBSTR(cmpgn_id,5,3)

----- CPM APE Performance -----
SELECT	TGT.TGT_AGT_REGIONAL_HEAD region,
		SUBSTR(TGT.CMPGN_ID,5,3) cmp_typ,
		LPAD(MONTH(TRK.NEW_CVG_ISS_DT),2,'00') month,
		COUNT(DISTINCT tgt.TGT_CUST_ID) leads,
        COUNT(DISTINCT tgt.TGT_AGT_CODE) agents,	
		COUNT(DISTINCT trk.NEW_POL_CUST_ID) responses,
		SUM(trk.NEW_POL_APE) ape,
		COUNT(DISTINCT CASE WHEN TRK.NEW_POL_CVG_TYP='B' THEN trk.NEW_POL_NUM END) cases,
		SUM(trk.NEW_POL_NBV) nbv
FROM	vn_published_campaign_db.targetm_dm tgt
	LEFT JOIN
		(SELECT	a.*,
				ROW_NUMBER() OVER (PARTITION BY new_pol_num, new_pol_cvg_typ, new_pol_plan_cd, --new_pol_cust_id,
				new_pol_insrd_id, new_pol_writing_agt ORDER BY new_pol_num, cmpgn_priority) rn
		 FROM	(SELECT	DISTINCT
						trk.cmpgn_id,
				 		CASE WHEN SUBSTR(trk.cmpgn_id,5,3)='ONB' THEN 1
				 			 WHEN SUBSTR(trk.cmpgn_id,5,3) IN ('MAT','AVY') THEN 2
							  WHEN SUBSTR(trk.cmpgn_id,5,3)='CTS' THEN 2
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
		 		 WHERE	trk.cmpgn_id NOT LIKE 'CTR-%'
				 	AND	trk.cmpgn_id NOT LIKE '%POC%'
					AND	trk.cmpgn_id NOT LIKE '%RAND%'
					AND trk.btch_id NOT LIKE '9%'
				 	--AND	lead_gen_ind=1				 	
				 	AND	trk.new_cvg_stat IS NOT NULL
				 	AND	trk.new_cvg_stat NOT IN ('8','A','N','R','X')
					AND trk.tgt_id IS NOT NULL
					AND	SUBSTR(trk.new_pol_plan_cd,1,3) NOT IN ('FDB','BIC','PN0')
					AND	((trk.new_pol_cvg_typ='B' AND trk.new_cvg_iss_dt BETWEEN ${st_dt} AND ${ed_dt}) --AND ${end_date})
					  OR (trk.new_pol_cvg_typ<>'B' AND trk.new_cvg_eff_dt BETWEEN ${st_dt} AND ${ed_dt})) --AND ${end_date}))
				 	--AND	trk.new_cvg_iss_dt>='2021-01-01'
				) a
		) TRK
	ON	TGT.TGT_ID = TRK.TGT_ID 
--WHERE	tgt.batch_start_date BETWEEN ${st_dt} AND ${ed_dt}
GROUP BY									   
	TGT.TGT_AGT_REGIONAL_HEAD,
	SUBSTR(TGT.CMPGN_ID,5,3),
	MONTH(TRK.NEW_CVG_ISS_DT)
HAVING region IS NOT NULL

----- AWS & DTK Adoption -----
SELECT	'AWS' system,
		tgt.tgt_agt_regional_head region_head,
		p2.month month,
		SUM(tgt.leads) leads,
		--COUNT(DISTINCT tgt.agentid) agentid,
		COUNT(DISTINCT p2.agentid) agt_act
FROM	(SELECT	tgt_agt_regional_head,
				cmpgn_id,
				tgt_agt_code agentid,
				COUNT(DISTINCT tgt_cust_id) leads,
				MIN(batch_start_date) batch_start_date
		 FROM 	vn_published_campaign_db.TARGETM_DM
		 WHERE	cmpgn_id LIKE 'MIL-%-202%'
			AND cmpgn_id NOT LIKE '%-POC-%'
			AND cmpgn_id NOT LIKE '%-RAND-%'
			AND	btch_id NOT LIKE '9%'
		GROUP BY tgt_agt_regional_head, cmpgn_id, tgt_agt_code
		) tgt
		INNER JOIN
				(SELECT	LPAD(MONTH(hit_date),2,'00') month,
				 		a.CPMID, agentid
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
						TO_DATE(hit_date) hit_date
				FROM	vn_curated_adobe_pws_db.aws_cpm_daily
				lateral view explode (cmpgn_id_list) cmpgn_id_list as cpmid ) a
                WHERE page IN ('Lead Info Page')
				 	AND	hit_date>=${st_dt}
				GROUP BY LPAD(MONTH(hit_date),2,'00'), a.CPMID, agentid
				) p2
			ON	tgt.agentid = p2.agentid AND tgt.CMPGN_ID = p2.CPMID
GROUP BY tgt.tgt_agt_regional_head, p2.month
HAVING region_head IS NOT NULL
UNION
SELECT	'DTK' system,
		tgt.tgt_agt_regional_head region_head,
		LPAD(MONTH(dtk.txn_date),2,'00') month,
		SUM(tgt.leads) leads,
		--COUNT(DISTINCT dtk.agt_code) agentid,
		COUNT(DISTINCT CASE WHEN dtk.lead_status NOT IN ('NEW','EXPIRED') THEN dtk.agt_code END) agt_act
FROM	(SELECT	tgt_agt_regional_head,
				cmpgn_id,
				tgt_agt_code agentid,
				COUNT(DISTINCT tgt_cust_id) leads,
				MIN(batch_start_date) batch_start_date
		 FROM 	vn_published_campaign_db.TARGETM_DM
		 WHERE	cmpgn_id LIKE 'MIL-%-202%'
			AND cmpgn_id NOT LIKE '%-POC-%'
			AND	btch_id NOT LIKE '9%'
		GROUP BY tgt_agt_regional_head, cmpgn_id, tgt_agt_code
		) tgt
		INNER JOIN
			(select a.agent_code agt_code, opp.opportunity_type cmpgn_nm, SUBSTR(opp.external_id,1,16) cmpgn_id, opp.last_updated_date txn_date,
					to_date(opp.created_date) created_date, to_date(opp.opportunity_end_date) end_date , opp.opportunity_status lead_status, act.activity_type_name activity_name, ao.activity_outcome activity_outcome
			 from vn_published_dtk_db.opportunity opp
					inner join vn_published_dtk_db.activity act on opp.opportunity_id = act.opportunity_id
					inner join vn_published_dtk_db.party_agent_role par on par.party_agent_role_id = opp.party_agent_role_id
					inner join vn_published_dtk_db.agent a on a.agent_id = par.agent_id
					inner join vn_published_dtk_db.activity_outcome ao on ao.activity_id = act.activity_id
			 where true
					and opp.opportunity_source not in ('SELF_GENERATED','DIGITAL_LEADS') 
			) dtk
			ON tgt.agentid = dtk.agt_code AND tgt.CMPGN_ID = dtk.cmpgn_id
GROUP BY tgt.tgt_agt_regional_head, LPAD(MONTH(dtk.txn_date),2,'00')
HAVING region_head IS NOT NULL
ORDER BY system