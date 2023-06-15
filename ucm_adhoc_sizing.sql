SET hivevar:me_dt=concat(year(add_months(current_date(), -1))
	,"-", case when month(add_months(current_date(), -1))<10 then '0' else '' end
	,month(add_months(current_date(), -1)));

SET hivevar:mnth_end = '2023-04-30';
SET hivevar:campaign_launch = '2023-06-31'; 

DROP TABLE IF EXISTS vn_lab_project_scratch_db.ucm_hist_actv_cvg;
CREATE TABLE vn_lab_project_scratch_db.ucm_hist_actv_cvg
AS
SELECT
    cvg.pol_num
    ,po_lnk.cli_num                                      AS po_num
   ,ins_lnk.cli_num                                     AS ins_num
   ,po.cli_nm                                           AS cli_name
   ,cvg.plan_code
   ,code.product_name
   ,cvg.vers_num
   ,cvg.cvg_stat_cd
   ,cvg.cvg_typ
   ,cvg.cvg_reasn
   ,cvg.bnft_dur
   ,cvg.cvg_prem
   ,pol.pmt_mode
   ,cvg.cvg_prem*12/pol.pmt_mode                        AS inforce_APE
   ,pol.crcy_code
   ,cvg.face_amt
   ,cvg.cvg_eff_dt
   ,cvg.cvg_iss_dt
   ,pol.pol_iss_dt
   ,cvg.xpry_dt
   ,pol.wa_cd_1                                         AS wagt_cd
   ,wa.comp_prvd_num                                    AS wagt_comp_prvd_num
   ,pol.agt_code                                        AS sagt_cd
   ,sa.comp_prvd_num                                    AS sagt_comp_prvd_num
   ,CASE 
        WHEN sa.trmn_dt IS NOT NULL 
            AND sa.comp_prvd_num IN ('01','04', '97', '98') THEN 'Orphaned'
        WHEN sa.comp_prvd_num = '01'  
            AND (
                pol.agt_code = pol.wa_cd_1 
                OR pol.agt_code = pol.wa_cd_2
                )                                           THEN 'Original Agent'
        WHEN sa.comp_prvd_num = '01'                        THEN 'Reassigned Agent'
        WHEN sa.comp_prvd_num = '04'                        THEN 'Orphaned-Collector'
        WHEN sa.comp_prvd_num IN ('97', '98')               THEN 'Orphaned-Agent is SM'
        ELSE 'Unknown' 
    END                                                 AS cus_agt_rltnshp


FROM vn_published_cas_db.tcoverages cvg 
INNER JOIN vn_published_cas_db.tpolicys pol
    ON cvg.pol_num = pol.pol_num
-- owner
INNER JOIN vn_published_cas_db.tclient_policy_links po_lnk 
    ON pol.pol_num = po_lnk.pol_num
        AND po_lnk.link_typ = 'O'
INNER JOIN vn_published_cas_db.tclient_details po 
    ON po_lnk.cli_num = po.cli_num

-- insured
INNER JOIN vn_published_cas_db.tclient_policy_links ins_lnk -- insured number
    ON pol.pol_num = ins_lnk.pol_num 
        AND ins_lnk.link_typ = 'I'
INNER JOIN vn_published_cas_db.tclient_details po_2 
    ON ins_lnk.cli_num = po_2.cli_num

INNER JOIN vn_published_ams_db.tams_agents wa 
    ON pol.wa_cd_1 = wa.agt_code
INNER JOIN vn_published_ams_db.tams_agents sa 
    ON pol.agt_code = sa.agt_code

inner join vn_published_campaign_db.vn_plan_code_map code
    on cvg.plan_code=code.plan_code

WHERE 
    cvg.cvg_typ = 'B'
    AND cvg.cvg_reasn = 'O'
    AND cvg.cvg_stat_cd IN ('1','3','5')
    -- agency sizing
    AND wa.comp_prvd_num in ('01','97','98')
    AND sa.comp_prvd_num in ('01','04','97','98')

    AND po.sex_code in ('F', 'M') --Individual policies

    ; 


--Step 3: Get all customers' issued (and still in-force) coverages from inception.
DROP TABLE IF EXISTS vn_lab_project_scratch_db.ucm_hist_allcvg;
CREATE TABLE vn_lab_project_scratch_db.ucm_hist_allcvg
AS
SELECT
    cvg.pol_num
   ,po_lnk.cli_num                                      AS po_num
   ,cvg.plan_code
   ,cvg.vers_num
   ,cvg.cvg_stat_cd
   ,cvg.cvg_typ
   ,cvg.cvg_reasn
   ,cvg.cvg_prem
   ,pol.pmt_mode
   ,cvg.cvg_prem*12/pmt_mode                            AS inforce_APE
   ,cvg.cvg_iss_dt
   ,pol.pol_iss_dt
   ,FLOOR(
        DATEDIFF(${mnth_end}, cvg.cvg_iss_dt)/365.25
    )                                                   AS inforce_year
   ,pol.wa_cd_1                                         AS wagt_cd
   ,wa.comp_prvd_num                                    AS wagt_comp_prvd_num
   ,pol.agt_code                                        AS sagt_cd
   ,sa.comp_prvd_num                                    AS sagt_comp_prvd_num
FROM vn_published_cas_db.tcoverages cvg 
INNER JOIN vn_published_cas_db.tpolicys pol
    ON cvg.pol_num = pol.pol_num
INNER JOIN vn_published_cas_db.tclient_policy_links po_lnk
    ON pol.pol_num = po_lnk.pol_num
        AND po_lnk.link_typ = 'O'
INNER JOIN vn_published_ams_db.tams_agents wa 
    ON pol.wa_cd_1 = wa.agt_code
INNER JOIN vn_published_ams_db.tams_agents sa 
    ON pol.agt_code = sa.agt_code
WHERE pol.pol_iss_dt <= ${mnth_end} --To calculate VIP/Near-VIP status on same period as propensity model scoring
    AND pol.pol_stat_cd in ('1','3','5')
    AND wa.comp_prvd_num in ('01','97','98')
    AND sa.comp_prvd_num in ('01','04','97','98')
    ;


--Step 4: Get Agent/Collector details
DROP TABLE IF EXISTS vn_lab_project_scratch_db.ucm_hist_agt;
CREATE TABLE vn_lab_project_scratch_db.ucm_hist_agt
AS
SELECT
    DISTINCT agt.agt_code
    ,agt.agt_nm
    ,agt.cntrct_eff_dt
    ,agt.trmn_dt
    ,CASE
            WHEN agt.stat_cd = '01' THEN 'Active'
            WHEN agt.stat_cd = '99' THEN 'Terminated'
            ELSE 'Unknown'
        END                                                 AS agt_status
    ,agt.comp_prvd_num
    ,agt.loc_code
    ,agtdm.id_num                                        AS sagt_id_num
    ,agtdm.sex_code                                      AS agt_sex_code
    ,agtdm.birth_dt                                      AS agt_birth_dt
    ,loc.city
    ,loc.region
    ,agt.mobl_phon_num
    ,can.email_addr
    ,smgr.loc_mrg                                        AS mgr_cd
    ,smgr.mgr_nm
    ,sc.agent_tier                                      AS agent_tier    
    ,sc.agent_cluster
    ,rh.rh_name                                         AS agent_rh
FROM vn_published_ams_db.tams_agents agt
LEFT JOIN vn_published_ams_db.tams_candidates can
    ON can.can_num = agt.can_num
LEFT JOIN vn_published_campaign_db.loc_code_mapping loc 
    ON loc.loc_code = agt.loc_code
LEFT JOIN vn_published_reports_db.loc_to_sm_mapping rh 
    ON rh.loc_cd = agt.loc_code

LEFT JOIN vn_published_datamart_db.tagtdm_daily agtdm
    ON agt.agt_code=agtdm.agt_code


LEFT JOIN vn_published_analytics_db.agent_scorecard sc 
    ON agt.agt_code = sc.agt_code
        AND DATE_FORMAT(monthend_dt, "yyyy-MM")=${me_dt}
LEFT JOIN (
    SELECT DISTINCT
        stru.loc_code
       ,stru.loc_mrg
       ,sm.agt_nm AS mgr_nm
    FROM vn_published_cas_db.tmis_stru_grp_sm stru
    INNER JOIN vn_published_ams_db.tams_agents sm
        ON stru.loc_mrg = sm.agt_code
    WHERE stru.loc_code IS NOT NULL
)smgr
    ON agt.loc_code = smgr.loc_code
WHERE agt.comp_prvd_num IN ('01', '04', '97', '98');

-- get leads here for scoring before proceeding with client details
-- create table for anniversary needs model
-- edit code to not extract from ucm needs model, or at least change the name of the table
