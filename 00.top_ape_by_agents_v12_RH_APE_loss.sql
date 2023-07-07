SET hivevar:cmpgn_st_dt='2023-01-01'; 	-- Don't change this date (date where it starts)
SET hivevar:start_date='2023-01-01';	-- Date range customers buying new policies
SET hivevar:end_date='2023-04-30';		-- Date range customers buying new policies
SET hivevar:rpt_mth=2304;

DROP TABLE IF EXISTS vn_processing_datamart_temp_db.top_ape_by_agents_${rpt_mth}_v8;
CREATE TABLE vn_processing_datamart_temp_db.top_ape_by_agents_${rpt_mth}_v8 AS
SELECT	DISTINCT
		tgt.tgt_agt_code agt_code, 	--[ver12]
		ape.cmpgn_id cmpgn_id,
		lead.no_leads,				--[ver17]
		tgt.loc_cd,
		NVL(loc.manager_name_0,
			loc.manager_name_1,
			loc.manager_name_2,
			loc.manager_name_3,
			loc.manager_name_4,
			loc.manager_name_5,
			loc.manager_name_6,
		"Open") sm_name,
		NVL(loc.manager_code_0,
			loc.manager_code_1,
			loc.manager_code_2,
			loc.manager_code_3,
			loc.manager_code_4,
			loc.manager_code_5,
			loc.manager_code_6,
		"Open") sm_code,
		loc.rh_name,
		agt.agt_nm,
		agt.rank_code,
		agt.mobl_phon_num,
		TO_DATE(agt.birth_dt) birth_dt,
		TO_DATE(agt.agt_join_dt) agt_join_dt,
		agt.channel,
		tgt.new_pol_num,
		tgt.new_pol_cust_id,		--[ver17]
		tgt.new_cvg_stat,
		TO_DATE(tgt.sbmt_dt) sbmt_dt,
		TO_DATE(tgt.pol_iss_dt) pol_iss_dt,
		TO_DATE(tgt.new_pol_cnfrm_dt) new_pol_cnfrm_dt,	--[ver7]
		tgt.new_ape,
		tgt.new_ape+NVL(tgt.ape_topup,0) tot_ape,	--[ver10]
		tgt.new_fyp,
		NVL(tgt.fyc,0) fyc,--[ver11]
		tgt.cc,
		tgt.new_pol_writing_agt,--[ver6]
		tgt.new_loc_code,--[ver12]
		tgt.new_rh_name,
		tgt.new_sub_rh_name, --[ver16]
		tgt.assign_agent_ind,	--[ver6]
		tgt.assign_sm_ind,
		tgt.assign_sub_rh_ind,	--[ver16]
		tgt.assign_rh_ind,		--[ver17]
		tgt.epos_ind
FROM	(SELECT	p.cmpgn_id	 		
		 FROM	(SELECT	*,
						 ROW_NUMBER() OVER (PARTITION BY cmpgn_id, prev_pol_num) rn
				 FROM	vn_published_campaign_db.TARGETM_DM
		 		 WHERE	cmpgn_id LIKE 'MIL-%-20%'
					AND btch_id NOT LIKE '9%'
					AND target_channel='Agency'
				) p
		 WHERE	p.rn=1
		 GROUP BY
				p.cmpgn_id
		 ) ape
	INNER JOIN
		(SELECT	c.cmpgn_id,
		 		c.tgt_agt_regional_head rh_name,
				c.tgt_sub_rh_name sub_rh_name,	--[ver16]
				c.loc_cd,
				c.tgt_agt_code,
				c.new_pol_writing_agt,
				c.loc_code new_loc_code,
		 		c.rh_name new_rh_name,
				c.sub_rh_name new_sub_rh_name,	--[ver16]
				c.new_pol_num,
				c.new_pol_cust_id,	--[ver17]
				c.new_cvg_stat,
		 		c.assign_agent_ind,
		 		c.assign_sm_ind,
				c.assign_sub_rh_ind,	--[ver16]
		 		c.assign_rh_ind,		--[ver17]
		 		c.sbmt_dt,
		 		c.pol_iss_dt,
		 		d.cnfrm_dt new_pol_cnfrm_dt,
		 		c.cc,
				c.new_ape,
		 		d.ape_topup,
				d.new_fyp,
		 		e.fyc,
		 		f.epos_ind
		 FROM	(SELECT a.cmpgn_id,
				 		a.tgt_agt_regional_head,
						a.tgt_sub_rh_name,
						a.tgt_agt_loc_code loc_cd,
						a.tgt_agt_code,
						b.new_pol_writing_agt,
						b.loc_code,--[ver12]
				 		b.rh_name,
						b.sub_rh_name,
						b.new_pol_num,
						b.new_pol_cust_id,
						b.new_cvg_stat,
				 		CASE WHEN a.tgt_agt_code=b.new_pol_writing_agt THEN 1 ELSE 0 END assign_agent_ind,
				 		CASE WHEN a.tgt_agt_loc_code=b.loc_code THEN 1 ELSE 0 END assign_sm_ind,
						CASE WHEN a.tgt_sub_rh_name IS NOT NULL AND a.tgt_sub_rh_name=b.sub_rh_name THEN 1 ELSE 0 END assign_sub_rh_ind,
				 		CASE WHEN a.tgt_agt_regional_head IS NOT NULL and a.tgt_agt_regional_head=b.rh_name THEN 1 ELSE 0 END assign_rh_ind,
				 		MAX(CASE WHEN b.new_pol_cvg_typ='B' THEN b.new_pol_sbmt_dt END) sbmt_dt,
				 		MAX(CASE WHEN b.new_pol_cvg_typ='B' THEN b.new_cvg_iss_dt END) pol_iss_dt,
				 		MAX(CASE WHEN b.new_pol_cvg_typ='B' THEN b.new_pol_cnfrm_dt  END) new_pol_cnfrm_dt,
				 		SUM(DISTINCT CASE WHEN b.new_cvg_iss_dt IS NOT NULL AND b.new_pol_cvg_typ='B' THEN 1 ELSE 0 END) cc,
						SUM(CASE WHEN b.new_cvg_iss_dt IS NOT NULL THEN b.new_pol_ape ELSE 0 END) new_ape
				 FROM	(SELECT	a.*,
								CASE
									WHEN rh_code='UY597' THEN '1. Bạch Xuân Trung'
									WHEN rh_code='E9420' THEN '2. Nguyễn Mạnh Lương'
									WHEN rh_code='TA010' THEN '3. Nguyễn Pilot'
									WHEN rh_code='T6753' THEN	-- Pham Mai Phuong
										CASE
											WHEN MANAGER_CODE_5='NI010' OR MANAGER_CODE_4='NI010' OR MANAGER_CODE_3='NI010' OR MANAGER_CODE_2='NI010' OR MANAGER_CODE_1='NI010' OR MANAGER_CODE_0='NI010' THEN '6. Đinh Ngọc Anh'
											WHEN MANAGER_CODE_5='X0753' OR MANAGER_CODE_4='X0753' OR MANAGER_CODE_3='X0753' OR MANAGER_CODE_2='X0753' OR MANAGER_CODE_1='X0753' OR MANAGER_CODE_0='X0753' THEN '5. Đặng Thanh Tuấn'
											WHEN MANAGER_CODE_5='T6774' OR MANAGER_CODE_4='T6774' OR MANAGER_CODE_3='T6774' OR MANAGER_CODE_2='T6774' OR MANAGER_CODE_1='T6774' OR MANAGER_CODE_0='T6774' THEN '8. Nguyễn Thái Hưng'
											ELSE '10. Phạm Mai Phương(others)'
										END
								   WHEN rh_code='S4199' THEN	-- Lê Thị Minh Loan
										CASE
											WHEN MANAGER_CODE_5='S3715' OR MANAGER_CODE_4='S3715' OR MANAGER_CODE_3='S3715' OR MANAGER_CODE_2='S3715' OR MANAGER_CODE_1='S3715' OR MANAGER_CODE_0='S3715' THEN '9.Nguyễn Thị Hồng Vinh' 
											ELSE '7. Lê Thị Minh Loan(others)'
										END
									WHEN rh_code='Y8847' THEN
										CASE
											WHEN MANAGER_CODE_5='G0180' OR MANAGER_CODE_4='G0180' OR MANAGER_CODE_3='G0180' OR MANAGER_CODE_2='G0180' OR MANAGER_CODE_1='G0180' OR MANAGER_CODE_0='G0180' THEN '4. Cao Việt Anh' 
											WHEN MANAGER_CODE_5='Y5942' OR MANAGER_CODE_4='Y5942' OR MANAGER_CODE_3='Y5942' OR MANAGER_CODE_2='Y5942' OR MANAGER_CODE_1='Y5942' OR MANAGER_CODE_0='Y5942' THEN '11. Phan Thị Thanh Bình' 
											WHEN MANAGER_CODE_5='G0096' OR MANAGER_CODE_4='G0096' OR MANAGER_CODE_3='G0096' OR MANAGER_CODE_2='G0096' OR MANAGER_CODE_1='G0096' OR MANAGER_CODE_0='G0096' THEN '12. Phùng Duy Long' 
											ELSE '13. Vũ Thị Hoài Nam(others)'
										END
									END tgt_sub_rh_name	--[ver16]
						 FROM	vn_published_campaign_db.TARGETM_DM a
							LEFT JOIN
								vn_published_reports_db.loc_to_sm_mapping_hist b
							 ON	a.tgt_agt_loc_code=b.loc_cd AND b.image_date=${end_date}
						) a
					INNER JOIN
						(SELECT	t.*
						 FROM	(SELECT	a.*,
										ROW_NUMBER() OVER (PARTITION BY new_pol_num, new_pol_cvg_typ, new_pol_plan_cd, new_pol_insrd_id, new_pol_writing_agt ORDER BY new_pol_num, cmpgn_priority) rn
								 FROM	(SELECT	c.*, 
										 		d.loc_code,
												CASE
													WHEN e.rh_code='UY597' THEN '1. Bạch Xuân Trung'
													WHEN e.rh_code='E9420' THEN '2. Nguyễn Mạnh Lương'
													WHEN e.rh_code='TA010' THEN '3. Nguyễn Pilot'
													WHEN e.rh_code='T6753' THEN	-- Pham Mai Phuong
														CASE
															WHEN MANAGER_CODE_5='NI010' OR MANAGER_CODE_4='NI010' OR MANAGER_CODE_3='NI010' OR MANAGER_CODE_2='NI010' OR MANAGER_CODE_1='NI010' OR MANAGER_CODE_0='NI010' THEN '6. Đinh Ngọc Anh'
															WHEN MANAGER_CODE_5='X0753' OR MANAGER_CODE_4='X0753' OR MANAGER_CODE_3='X0753' OR MANAGER_CODE_2='X0753' OR MANAGER_CODE_1='X0753' OR MANAGER_CODE_0='X0753' THEN '5. Đặng Thanh Tuấn'
															WHEN MANAGER_CODE_5='T6774' OR MANAGER_CODE_4='T6774' OR MANAGER_CODE_3='T6774' OR MANAGER_CODE_2='T6774' OR MANAGER_CODE_1='T6774' OR MANAGER_CODE_0='T6774' THEN '8. Nguyễn Thái Hưng'
															ELSE '10. Phạm Mai Phương(others)'
														END
												   WHEN e.rh_code='S4199' THEN	-- Lê Thị Minh Loan
														CASE
															WHEN MANAGER_CODE_5='S3715' OR MANAGER_CODE_4='S3715' OR MANAGER_CODE_3='S3715' OR MANAGER_CODE_2='S3715' OR MANAGER_CODE_1='S3715' OR MANAGER_CODE_0='S3715' THEN '9.Nguyễn Thị Hồng Vinh' 
															ELSE '7. Lê Thị Minh Loan(others)'
														END
													WHEN e.rh_code='Y8847' THEN
														CASE
															WHEN MANAGER_CODE_5='G0180' OR MANAGER_CODE_4='G0180' OR MANAGER_CODE_3='G0180' OR MANAGER_CODE_2='G0180' OR MANAGER_CODE_1='G0180' OR MANAGER_CODE_0='G0180' THEN '4. Cao Việt Anh' 
															WHEN MANAGER_CODE_5='Y5942' OR MANAGER_CODE_4='Y5942' OR MANAGER_CODE_3='Y5942' OR MANAGER_CODE_2='Y5942' OR MANAGER_CODE_1='Y5942' OR MANAGER_CODE_0='Y5942' THEN '11. Phan Thị Thanh Bình' 
															WHEN MANAGER_CODE_5='G0096' OR MANAGER_CODE_4='G0096' OR MANAGER_CODE_3='G0096' OR MANAGER_CODE_2='G0096' OR MANAGER_CODE_1='G0096' OR MANAGER_CODE_0='G0096' THEN '12. Phùng Duy Long' 
															ELSE '13. Vũ Thị Hoài Nam(others)'
														END
													END sub_rh_name,	--[ver16]
										 		e.rh_name,
											    CASE
												  WHEN SUBSTR(cmpgn_id,5,3)='ONB' THEN 1
												  WHEN SUBSTR(cmpgn_id,5,3) IN ('MAT','AVY') THEN 2
												  WHEN SUBSTR(cmpgn_id,5,3)='CTS' THEN 3
												  WHEN SUBSTR(cmpgn_id,5,3)='UCM' THEN 5
												  ELSE 4
											    END	cmpgn_priority
										 FROM	vn_published_campaign_db.TRACKINGM c
										 	INNER JOIN
										 		vn_published_ams_db.TAMS_AGENTS d
											 ON	c.new_pol_writing_agt=d.agt_code
										 	LEFT JOIN
										 		vn_published_reports_db.LOC_TO_SM_MAPPING_HIST e
										 	ON d.loc_code=e.loc_cd AND e.image_date=${end_date}
										 WHERE	cmpgn_id LIKE 'MIL-%-20%'
											AND	btch_id NOT LIKE '9%'
											AND	new_pol_plan_cd NOT IN ('FDB01','PN001','BIC01','BIC02','BIC03','BIC04')
										 	AND new_cvg_stat IS NOT NULL
										 	AND new_cvg_stat NOT IN ('8','A','N','R','X')
										 	AND	((new_pol_cvg_typ='B' AND new_cvg_iss_dt BETWEEN ${start_date} AND ${end_date}) --AND ${end_date})
					  						  OR (new_pol_cvg_typ<>'B' AND new_cvg_eff_dt BETWEEN ${start_date} AND ${end_date}))
										) a
								) t
						 WHERE	t.rn=1
						) b
					ON	a.tgt_id = b.tgt_id --AND a.tgt_agt_regional_head=b.rh_name --[ver14] --a.tgt_agt_loc_code=b.loc_code	--[ver9]
				WHERE	a.cmpgn_id LIKE 'MIL-%-20%'
				GROUP BY		
						a.cmpgn_id,
				 		a.tgt_agt_regional_head,
						a.tgt_sub_rh_name,
						a.tgt_agt_loc_code,
						a.tgt_agt_code,
						b.new_pol_writing_agt,
						b.loc_code,
				 		b.rh_name,
						b.sub_rh_name,
						b.new_pol_num,
						b.new_pol_cust_id,
						b.new_cvg_stat,
				 		b.assign_agent_ind
				) c
			INNER JOIN
				(SELECT	pol_num,
						MIN(cnfrm_dt) cnfrm_dt,
				 		SUM(DISTINCT fyp_top_up*0.1) ape_topup,	--[ver10]
				 		SUM(DISTINCT fyp+(fyp_top_up*0.06)) new_fyp
				 FROM 	vn_published_reports_db.tabd_sbmt_pols
				 WHERE	layer_typ IN ('B','I')
				 GROUP BY
						pol_num
				) d
			ON	c.new_pol_num=d.pol_num
			/* Add FYC data */
			LEFT JOIN
				(SELECT	tb1.pol_num,
						SUM(tb1.fyc) fyc
				 FROM	(SELECT	pol_num,
								SUM(crr_valu) fyc
						 FROM	vn_published_ams_db.tams_crr_transactions
						 WHERE	reasn_cd='115'
							AND	crr_typ='COM'
						 GROUP BY
								pol_num
						 UNION ALL
						 SELECT	pol_num,
								SUM(crr_valu) fyc
						 FROM	vn_published_ams_bak_db.TAMS_CRR_TRANSACTIONS_BK
						 WHERE	reasn_cd='115'
							AND	crr_typ='COM'
						 GROUP BY
								pol_num
						) tb1
				 GROUP BY tb1.pol_num	-- ver 12
				) e
			 ON	c.new_pol_num=e.pol_num
			LEFT JOIN
				(SELECT	`pol num` pol_num,
				 		MAX(IF(`nb user`='EPOS','Y','N')) epos_ind
				 FROM	vn_published_reports_db.tpexnb02vn
				 WHERE	type='SUBMIT'
				 GROUP BY
						`pol num`
				) f
			 ON	c.new_pol_num=f.pol_num
			GROUP BY
				c.cmpgn_id,
				c.tgt_agt_regional_head,
				c.tgt_sub_rh_name,
				c.loc_cd,
				c.tgt_agt_code,
				c.new_pol_writing_agt,
				c.loc_code,
				c.rh_name,
				c.sub_rh_name,
				c.new_pol_num,
				c.new_pol_cust_id,
				c.new_cvg_stat,
		 		c.assign_agent_ind,
				c.assign_sm_ind,
				c.assign_sub_rh_ind,
				c.assign_rh_ind,
		 		c.sbmt_dt,
		 		c.pol_iss_dt,	
		 		d.cnfrm_dt,
		 		c.cc,
				c.new_ape,
		 		d.ape_topup,
				d.new_fyp,
				e.fyc,
				f.epos_ind
		) tgt		
	 ON	tgt.cmpgn_id = ape.cmpgn_id --AND tgt.new_rh_name = ape.rh_name -- [ver14] --tgt.loc_cd = ape.loc_cd
	LEFT JOIN
		vn_published_datamart_db.tagtdm_daily agt
	 ON	tgt.new_pol_writing_agt = agt.agt_code
	LEFT JOIN
		vn_published_reports_db.loc_to_sm_mapping_hist loc
	 ON	tgt.loc_cd = loc.loc_cd AND loc.image_date=${end_date}	
	INNER JOIN
		(SELECT	tgt_agt_code, COUNT(DISTINCT tgt_cust_id) no_leads	--[ver17]
		 FROM	vn_published_campaign_db.TARGETM_DM
		 WHERE	cmpgn_id LIKE 'MIL-%-20%'
            AND cmpgn_id NOT LIKE 'MIL-REC%'	-- exclude recycled leads
			AND cmpgn_id NOT LIKE '%RAND%'
			AND cmpgn_id NOT LIKE '%POC%'
			AND btch_id NOT LIKE '9%'
			AND target_channel='Agency'
		 GROUP BY
		 		tgt_agt_code
		) lead ON tgt.tgt_agt_code=lead.tgt_agt_code
;

SELECT	COUNT(new_pol_num) no_rows,
		COUNT(DISTINCT new_pol_num) no_pols
FROM 	vn_processing_datamart_temp_db.top_ape_by_agents_${rpt_mth}_v8
WHERE	cc>0
	AND	new_ape>0;