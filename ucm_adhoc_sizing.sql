SET hivevar:me_dt=concat(year(add_months(current_date(), -1))
	,"-", case when month(add_months(current_date(), -1))<10 then '0' else '' end
	,month(add_months(current_date(), -1)));

SET hivevar:mnth_end = '2023-04-30';
SET hivevar:campaign_launch = '2023-06-31'; 

DROP TABLE IF EXISTS vn_lab_project_scratch_db.ucm_hist_actv_cvg;
CREATE TABLE vn_lab_project_scratch_db.ucm_hist_actv_cvg
AS
SELECT DISTINCT
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
    -- still paying premium
    AND cvg.cvg_stat_cd IN ('1','3')
    -- agency sizing
    AND wa.comp_prvd_num in ('01','97','98')
    AND sa.comp_prvd_num in ('01','04','97','98')
    --Individual policies
    AND po.sex_code in ('F', 'M')
	AND	cvg.xpry_dt between '2023-07-01' and '2023-09-30'
; 


--Step 3: Get all customers' issued (and still in-force) coverages from inception.
DROP TABLE IF EXISTS vn_lab_project_scratch_db.ucm_hist_allcvg;
CREATE TABLE vn_lab_project_scratch_db.ucm_hist_allcvg
AS
SELECT DISTINCT
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
	AND	cvg.xpry_dt between '2023-07-01' and '2023-09-30'
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
drop table if exists vn_lab_project_scratch_db.ucm_hist_needs_model;
create table vn_lab_project_scratch_db.ucm_hist_needs_model as
select a.*
    , case
        when least(decile_inv, decile_ci, decile_lp, decile_lt, decile_acc, decile_med) in (1,2,3) then 'High'
        else 'Low'
        end as customer_propensity
    ,concat(concat((case when hml_inv='H' then 1 else 0 end ) + 
        (case when hml_ci='H' then 1 else 0 end ) + 
        (case when hml_lp='H' then 1 else 0 end ) + 
        (case when hml_lt='H' then 1 else 0 end ) + 
        (case when hml_acc='H' then 1 else 0 end ) + 
        (case when hml_med='H' then 1 else 0 end ),'H '),
        concat((case when hml_inv='M' then 1 else 0 end ) + 
        (case when hml_ci='M' then 1 else 0 end ) + 
        (case when hml_lp='M' then 1 else 0 end ) + 
        (case when hml_lt='M' then 1 else 0 end ) + 
        (case when hml_acc='M' then 1 else 0 end ) + 
        (case when hml_med='M' then 1 else 0 end ),'M '),
        concat((case when hml_inv='L' then 1 else 0 end ) + 
        (case when hml_ci='L' then 1 else 0 end ) + 
        (case when hml_lp='L' then 1 else 0 end ) + 
        (case when hml_lt='L' then 1 else 0 end ) + 
        (case when hml_acc='L' then 1 else 0 end ) + 
        (case when hml_med='L' then 1 else 0 end ),'L ')
        ) as propensity_group
    , 100*((case when hml_inv='H' then 1 else 0 end ) + 
        (case when hml_ci='H' then 1 else 0 end ) + 
        (case when hml_lp='H' then 1 else 0 end ) + 
        (case when hml_lt='H' then 1 else 0 end ) + 
        (case when hml_acc='H' then 1 else 0 end ) + 
        (case when hml_med='H' then 1 else 0 end )) +
                10*((case when hml_inv='M' then 1 else 0 end ) + 
        (case when hml_ci='M' then 1 else 0 end ) + 
        (case when hml_lp='M' then 1 else 0 end ) + 
        (case when hml_lt='M' then 1 else 0 end ) + 
        (case when hml_acc='M' then 1 else 0 end ) + 
        (case when hml_med='M' then 1 else 0 end )) +
                1*((case when hml_inv='L' then 1 else 0 end ) + 
        (case when hml_ci='L' then 1 else 0 end ) + 
        (case when hml_lp='L' then 1 else 0 end ) + 
        (case when hml_lt='L' then 1 else 0 end ) + 
        (case when hml_acc='L' then 1 else 0 end ) + 
        (case when hml_med='L' then 1 else 0 end )) as propensity_rank
from    vn_lab_project_campaign_module_db.vn_existing_customer_needs_model a
where   monthend_dt=${mnth_end}

--Step 5: Get client details
DROP TABLE IF EXISTS vn_lab_project_scratch_db.ucm_hist_cli_dtls;
CREATE TABLE vn_lab_project_scratch_db.ucm_hist_cli_dtls
AS
SELECT DISTINCT
    ucm_hist.po_num                                          AS po_num
   ,cli.cli_nm                                          AS po_name
   ,cli.id_num                                          AS po_id_num
   ,cli.sex_code                                        AS po_sex_code
   ,cli.birth_dt                                        AS po_birth_dt
   ,cli.cur_age                                         AS po_age_curr --age as of time of analysis
   ,CASE
        WHEN cli.cur_age < 18               THEN '1. <18'
        WHEN cli.cur_age BETWEEN 18 AND 25  THEN '2. 18-25'
        WHEN cli.cur_age BETWEEN 26 AND 35  THEN '3. 26-35'
        WHEN cli.cur_age BETWEEN 36 AND 45  THEN '4. 36-45'
        WHEN cli.cur_age BETWEEN 46 AND 55  THEN '5. 46-55'
        WHEN cli.cur_age BETWEEN 56 AND 65  THEN '6. 56-65'
        WHEN cli.cur_age > 65               THEN '7. >65'
        ELSE 'Unknown'
    END                                                 AS po_age_curr_grp
   ,cli.prim_phon_num                                   AS po_prim_phon_num
   ,cli.mobl_phon_num                                   AS po_mobl_phon_num
   ,cli.othr_phon_num                                   AS po_othr_phon_num
   ,IF(cli.mobl_phon_num IS NOT NULL, 'Y', 'N')         AS mobile_ind
   ,cli.sms_ind                                         AS opt_sms_ind
   ,cli.email_addr                                      AS po_email_addr
   ,IF(cli.email_addr IS NOT NULL, 'Y', 'N')            AS email_ind
   ,cli.sms_eservice_ind                                AS opt_email_ind
   ,cli.full_address                                    AS po_full_addr
   ,cli.city                                            AS customer_city
   ,CASE
        WHEN cli.city = 'Hà Nội'            THEN 'Hà Nội'
        WHEN cli.city = 'Hồ Chí Minh'       THEN 'Hồ Chí Minh'
        WHEN cli.city IS NULL               THEN 'Unknown'
        ELSE 'Others'
    END                                                 AS cust_city_cat
   ,NVL(cseg.CUSTOMER_SEGMENT, 'Undefined Segmentation')    AS lifestage_segment
--    ,cseg.max_no_dpnd
   ,cli.no_dpnd                               AS max_no_dpnd
   ,cli.occp_code
   ,cli.mthly_incm
   ,CASE
        WHEN cli.mthly_incm > 0 
            AND cli.mthly_incm < 20000      THEN '1) < 20M'
        WHEN cli.mthly_incm >= 20000 
            AND cli.mthly_incm < 40000      THEN '2) 20-40M'
        WHEN cli.mthly_incm >= 40000 
            AND cli.mthly_incm < 60000      THEN '3) 40-60M'
        WHEN cli.mthly_incm >= 60000 
            AND cli.mthly_incm < 80000      THEN '4) 60-80M'
        WHEN cli.mthly_incm >= 80000        THEN '5) 80M+'
        ELSE '6) Unknown'
    END                                                 AS mthly_incm_cat
   ,cli.mthly_incm*12                                   AS annual_incm
   ,iss.frst_join_dt
   ,IF(
        iss.frst_join_dt BETWEEN ADD_MONTHS(DATE_ADD(${mnth_end}, 1 - DAY(${mnth_end})),-12) AND ${mnth_end}
       ,'New'
       ,'Existing'
    )                                                   AS cus_type
   ,hld.pol_cnt
   ,hld.10yr_pol_cnt
   ,hld.total_APE
   ,CASE
         WHEN hld.pol_cnt = 1               THEN '1'
         WHEN hld.pol_cnt = 2               THEN '2'
         WHEN hld.pol_cnt BETWEEN 3 AND 5   THEN '3-5'
         WHEN hld.pol_cnt > 5               THEN '>5'
         ELSE 'NA' 
     END                                                AS pol_cnt_cat
   ,CASE
        WHEN hld.total_APE <  20000         THEN '1) < 20M'
        WHEN hld.total_APE >= 20000 
            AND hld.total_APE < 40000       THEN '2) 20-40M'
        WHEN hld.total_APE >= 40000
            AND hld.total_APE < 60000       THEN '3) 40-60M'
        WHEN hld.total_APE >= 60000
            AND hld.total_APE < 80000       THEN '4) 60-80M'
        WHEN hld.total_APE >= 80000         THEN '5) 80M+'
        ELSE '6) Unknown'
    END                                                 AS total_APE_cat
   ,CASE
        WHEN hld.total_APE >= 20000
            AND hld.total_APE < 65000
            AND hld.10yr_pol_cnt >= 1       THEN 'Silver'
        WHEN hld.total_APE >= 65000
            AND hld.total_APE < 150000       THEN 'Gold'
        WHEN hld.total_APE >= 150000
            AND hld.total_APE < 300000      THEN 'Platinum'
        WHEN hld.total_APE >= 300000        THEN 'Platinum Elite'
        ELSE 'Not VIP'
    END                                                 AS VIP_cat
   ,CASE
        WHEN hld.total_APE >= 15000
            AND hld.total_APE < 20000
            AND hld.10yr_pol_cnt >= 1       THEN 'Near Silver'
        WHEN hld.total_APE >= 55000
            AND hld.total_APE < 60000       THEN 'Near Gold1'
        WHEN hld.total_APE >= 60000
            AND hld.total_APE < 65000       THEN 'Near Gold2'
        ELSE 'Not Near VIP'
    END                                                 AS near_VIP_cat
   ,prop.propensity_group                               AS hml_propensity_grp
   ,SUBSTRING(prop.propensity_group,0,2)                AS no_high_prop -- Number of products that a customer is high propensity to buy
   ,NVL(prop.customer_propensity, 'No')                 AS hl_propensity_grp
   ,prop.propensity_rank
   ,prop.hml_inv
   ,prop.hml_ci
   ,prop.hml_lp
   ,prop.hml_lt
   ,prop.hml_acc
   ,prop.hml_med
   ,CASE
        WHEN SUBSTRING(prop.propensity_group,0,2) = '1H'
            AND prop.hml_acc = 'H'      THEN 'Acc'
        WHEN SUBSTRING(prop.propensity_group,0,2) = '1H'
            AND prop.hml_inv     = 'H'      THEN 'Investments'
        WHEN SUBSTRING(prop.propensity_group,0,2) = '1H'
            AND prop.hml_lt    = 'H'      THEN 'Term'
        WHEN SUBSTRING(prop.propensity_group,0,2) = '1H'        
            AND prop.hml_lp      = 'H'      THEN 'Life Protection'
        WHEN SUBSTRING(prop.propensity_group,0,2) = '1H'        
            AND prop.hml_med      = 'H'      THEN 'Med'
        WHEN SUBSTRING(prop.propensity_group,0,2) = '1H'        
            AND prop.hml_ci      = 'H'      THEN 'CI'
        ELSE 'Not 1H'
    END                                                 AS 1h_high_prop --Product where high propensity if 1H
    , prop.customer_propensity
    
FROM vn_lab_project_scratch_db.ucm_hist_actv_cvg ucm_hist
LEFT JOIN vn_published_datamart_db.tcustdm_daily cli
    ON ucm_hist.po_num = cli.cli_num

LEFT JOIN vn_published_analytics_db.cust_lifestage cseg
    ON ucm_hist.po_num = cseg.client_number
        AND cseg.monthend_dt = ${me_dt}
 -- put new needs model here
LEFT JOIN vn_lab_project_scratch_db.ucm_hist_needs_model prop
    ON ucm_hist.po_num = prop.cli_num
--         AND prop.monthend_dt = ${needs_mthend_dt}
LEFT JOIN (
    SELECT
        lnk.cli_num
       ,MIN(pol.frst_iss_dt)
       ,MIN(pol.pol_iss_dt)
       ,MIN(pol.pol_eff_dt)
       ,COALESCE(
            MIN(pol.frst_iss_dt)
           ,MIN(pol.pol_iss_dt)
           ,MIN(pol.pol_eff_dt)
        )                                               AS frst_join_dt
    FROM vn_published_cas_db.tpolicys pol
    INNER JOIN vn_published_cas_db.tclient_policy_links lnk
        ON pol.pol_num = lnk.pol_num
            AND lnk.link_typ = 'O'
    WHERE pol.pol_stat_cd IN ('1', '2', '3', '5')
    GROUP BY
        lnk.cli_num
)iss
    ON ucm_hist.po_num = iss.cli_num
LEFT JOIN (
    SELECT
        po_num
       ,SUM(inforce_APE)                                    AS total_APE
       ,COUNT(DISTINCT pol_num)                             AS pol_cnt
       ,COUNT(DISTINCT IF(inforce_year>=10,pol_num,NULL))   AS 10yr_pol_cnt
    FROM vn_lab_project_scratch_db.ucm_hist_allcvg
    GROUP BY po_num
) hld
    ON ucm_hist.po_num = hld.po_num;

-- get the list of dependent per policy
drop table if exists vn_lab_project_scratch_db.dpdnt_ids;
create table vn_lab_project_scratch_db.dpdnt_ids as
select cvg.pol_num
        , collect_set(dtl.id_num) as dpdnt_id_list
from vn_lab_project_scratch_db.ucm_hist_allcvg cvg
inner join vn_published_cas_db.tclient_policy_links lnk
    on cvg.pol_num=lnk.pol_num
inner join vn_published_cas_db.tclient_details dtl
    on cvg.po_num=dtl.cli_num
where lnk.link_typ IN ('O', 'I', 'T', 'B') --O: Owner, I: Insured, T: Other Insured, B: Beneficiary
group by 
    cvg.pol_num;

--Step 6: Final table, combines lead info, maturing policy info, and servicing agent info
DROP TABLE IF EXISTS vn_lab_project_scratch_db.ucm_hist_cli_final;
CREATE TABLE vn_lab_project_scratch_db.ucm_hist_cli_final
AS
SELECT DISTINCT
    cli.po_num
   ,cli.po_name
   ,cli.po_id_num
   ,cli.po_sex_code
   ,cli.po_birth_dt
   ,cli.po_age_curr
   ,cli.po_age_curr_grp
   ,cli.po_prim_phon_num
   ,cli.po_mobl_phon_num
   ,cli.po_othr_phon_num
   ,cli.mobile_ind
   ,cli.opt_sms_ind
   ,cli.po_email_addr
   ,cli.email_ind
   ,cli.opt_email_ind
   ,CASE
        WHEN cli.mobile_ind = 'Y'
            AND cli.opt_sms_ind = 'Y'
            AND cli.email_ind = 'Y'
            AND cli.opt_email_ind = 'Y'
            THEN 'Email and Mobile'
        WHEN cli.mobile_ind = 'Y'
            AND cli.opt_sms_ind = 'Y'
            THEN 'Mobile Only'
        WHEN cli.email_ind = 'Y'
            AND cli.opt_email_ind = 'Y'
            THEN 'Email Only'
        ELSE 'No Email or Mobile'
    END AS comm_method
   ,cli.po_full_addr
   ,cli.customer_city
   ,cli.cust_city_cat
   ,cli.lifestage_segment
   ,cli.max_no_dpnd
   ,cli.occp_code
   ,cli.mthly_incm
   ,cli.mthly_incm_cat
   ,cli.annual_incm
   ,cli.frst_join_dt
   ,cli.cus_type
   ,cli.pol_cnt
   ,cli.10yr_pol_cnt
   ,cli.total_APE
   ,cli.pol_cnt_cat
   ,cli.total_APE_cat
   ,cli.VIP_cat
   ,cli.near_VIP_cat
   ,CASE
        WHEN cli.near_VIP_cat <> 'Not Near VIP' 
            AND cli.VIP_cat <> 'Silver'             THEN 'Near VIP' --If both Silver and Near Gold1/Gold2 then classified as Silver
        WHEN cli.VIP_cat <> 'Not VIP'               THEN 'VIP'
        WHEN cli.near_VIP_cat = 'Not Near VIP'
            AND cli.vip_cat = 'Not VIP'
            AND cli.customer_propensity = 'High'         THEN 'High Propensity' --Existing customers that are high propensity (except 1H to Acc and Med only)
        ELSE 'Others'
    END AS lead_segment
   ,cli.hml_propensity_grp
   ,cli.hl_propensity_grp
   ,cli.no_high_prop
   ,cli.hml_inv
   ,cli.hml_ci
   ,cli.hml_lp
   ,cli.hml_lt
   ,cli.hml_acc
   ,cli.hml_med
   ,cli.1h_high_prop
   ,cli.customer_propensity
   ,IF(tgt.tgt_cust_id IS NULL, 'N', 'Y')               AS in_mat_po --if owner is present in other campaigns
   ,IF(ins.tgt_insrd_id IS NULL, 'N', 'Y')              AS in_mat_ins --if insured is present as lead in other campaigns
   ,CASE
        WHEN tgt.tgt_cust_id IS NULL
            AND ins.tgt_insrd_id IS NULL    THEN 'N'
        ELSE 'Y'
    END                                                 AS in_ONB --if either owner or insured is present as lead in other campaigns
   ,IF(bnc.po_num IS NULL, 'N', 'Y')                    AS in_banca --if owner is contacted in Banca
   ,IF(dmtm.po_num IS NULL, 'N', 'Y')                   AS in_dmtm --if owner is contacted in Banca
   ,IF(ARRAY_CONTAINS(dpd.dpdnt_id_list,CAST(agt.sagt_id_num AS VARCHAR(20)))
                OR  (cli.po_name=agt.agt_nm AND cli.po_birth_dt=agt.agt_birth_dt AND cli.po_sex_code=agt.agt_sex_code AND cli.po_email_addr=agt.email_addr)
                OR  (cli.po_name=agt.agt_nm AND cli.po_birth_dt=agt.agt_birth_dt AND cli.po_sex_code=agt.agt_sex_code AND cli.po_mobl_phon_num=agt.mobl_phon_num)
                ,'Y', 'N')   AS is_agt
   ,CASE
        WHEN tgt.tgt_cust_id IS NULL
            AND ins.tgt_insrd_id IS NULL
            AND bnc.po_num IS NULL
            AND dmtm.po_num IS NULL         
            AND IF(ARRAY_CONTAINS(dpd.dpdnt_id_list,CAST(agt.sagt_id_num AS VARCHAR(20)))
                OR  (cli.po_name=agt.agt_nm AND cli.po_birth_dt=agt.agt_birth_dt AND cli.po_sex_code=agt.agt_sex_code AND cli.po_email_addr=agt.email_addr)
                OR  (cli.po_name=agt.agt_nm AND cli.po_birth_dt=agt.agt_birth_dt AND cli.po_sex_code=agt.agt_sex_code AND cli.po_mobl_phon_num=agt.mobl_phon_num)
                , 1, 0)=0
                                            THEN 'N'
        ELSE 'Y'
    END                                                 AS excluded --if either owner or insured is present as lead in other campaigns or in Banca
   ,CASE
        WHEN bnc.po_num IS NOT NULL
            THEN 'Joint Banca'
        WHEN dmtm.po_num IS NOT NULL
            THEN 'Joint DMTM'
        WHEN tgt.tgt_cust_id IS NOT NULL
            OR ins.tgt_insrd_id IS NOT NULL
            THEN 'Past Campaigns'
        WHEN IF(ARRAY_CONTAINS(dpd.dpdnt_id_list,CAST(agt.sagt_id_num AS VARCHAR(20)))
                OR  (cli.po_name=agt.agt_nm AND cli.po_birth_dt=agt.agt_birth_dt AND cli.po_sex_code=agt.agt_sex_code AND cli.po_email_addr=agt.email_addr)
                OR  (cli.po_name=agt.agt_nm AND cli.po_birth_dt=agt.agt_birth_dt AND cli.po_sex_code=agt.agt_sex_code AND cli.po_mobl_phon_num=agt.mobl_phon_num)
                , 1, 0)=1
            THEN 'PO = active/terminated Agent'
        ELSE 'Not Excluded'
    END                                                 AS excluded_grp
   ,IF(ucm_hist.cus_agt_rltnshp IN ('Original Agent', 'Reassigned Agent'), 'N', 'Y')
                                                        AS unassigned
   ,ucm_hist.pol_num
   ,ucm_hist.plan_code
   ,code.product_name
   ,ucm_hist.vers_num
   ,ucm_hist.ins_num
   ,ucm_hist.cvg_stat_cd
   ,ucm_hist.cvg_typ
   ,ucm_hist.cvg_reasn
   ,ucm_hist.bnft_dur
   ,ucm_hist.inforce_APE                                     AS base_APE
--    ,bnf.base_face_amt
--    ,bnf.maturity_benefit_per_FA
--    ,bnf.acum_div_bal --accum dividend balance
--    ,bnf.csh_cpn_bal --cash coupon balance
--    ,bnf.lump_sum_bal --lump sum balance
--    ,bnf.pol_loan_bal --policy loan balance
--    ,bnf.vpo_face_amt --sum of VPO face amount
--    ,bnf.mat_value --maturity value  
--    ,bnf.crcy_code
   ,ucm_hist.cvg_eff_dt
   ,ucm_hist.cvg_iss_dt
   ,ucm_hist.pol_iss_dt
   ,ucm_hist.xpry_dt
   ,ucm_hist.sagt_cd
   ,agt.agt_nm                                          AS sagt_name
   ,agt.agt_status                                      AS sagt_status
   ,agt.mobl_phon_num                                   AS sagt_mobl_phon
   ,agt.email_addr                                      AS sagt_email_addr
   ,agt.comp_prvd_num                                   AS sagt_comp_prvd_num
   ,agt.city                                            AS sagt_city
   ,agt.loc_code                                        AS sagt_loc_code
   ,agt.region                                          AS sagt_region
   ,agt.mgr_cd                                          AS sagt_mgr_cd
   ,agt.mgr_nm                                          AS sagt_mgr_nm
   ,ucm_hist.cus_agt_rltnshp
   ,agt.agent_tier                                      AS sagt_tier
   ,agt.agent_cluster                                   AS sagt_cluster
   ,agt.agent_rh                                        AS sagt_rh
FROM vn_lab_project_scratch_db.ucm_hist_actv_cvg ucm_hist
-- INNER JOIN vn_lab_project_scratch_db.MAT_bnft_calc bnf
--    ON ucm_hist.pol_num = bnf.pol_num
INNER JOIN vn_lab_project_scratch_db.ucm_hist_cli_dtls cli
    ON ucm_hist.po_num = cli.po_num
INNER JOIN vn_lab_project_scratch_db.ucm_hist_agt agt
    ON ucm_hist.sagt_cd = agt.agt_code

INNER JOIN vn_published_campaign_db.vn_plan_code_map code
    ON ucm_hist.plan_code=code.plan_code

LEFT JOIN (
    SELECT DISTINCT
        t.tgt_cust_id
    FROM vn_published_campaign_db.targetm_dm t
    INNER JOIN vn_published_campaign_db.campm c
    -- INNER JOIN aa_lab_private_quichri_db.campm_210804 c
        ON t.cmpgn_id = c.cmpgn_id
    WHERE c.cmpgn_end_dt >= ${campaign_launch}
)tgt
    ON ucm_hist.po_num = tgt.tgt_cust_id  --exclude leads from past unfinished campaigns (as of this campaign's launch)
LEFT JOIN (
    SELECT DISTINCT
        t.tgt_insrd_id
    FROM vn_published_campaign_db.targetm_dm t
    INNER JOIN vn_published_campaign_db.campm c
    -- INNER JOIN aa_lab_private_quichri_db.campm_210804 c
        ON t.cmpgn_id = c.cmpgn_id
    WHERE c.cmpgn_end_dt >= ${campaign_launch}
)ins
    ON ucm_hist.po_num  = ins.tgt_insrd_id --exclude leads from past unfinished campaigns (as of this campaign's launch)
LEFT JOIN (
    SELECT DISTINCT 
        pol.po_num
    FROM vn_published_datamart_db.tpolidm_daily pol
    LEFT JOIN vn_published_datamart_db.tagtdm_daily cwa 
        ON pol.wa_code = cwa.agt_code
    LEFT JOIN vn_published_datamart_db.tagtdm_daily csa 
        ON pol.sa_code = csa.agt_code
    WHERE (cwa.channel = 'Banca'
        OR csa.channel = 'Banca')
        
)bnc
    ON ucm_hist.po_num = bnc.po_num --exclude owners that are joint Banca customers
LEFT JOIN (
    SELECT DISTINCT 
        pol.po_num
    FROM vn_published_datamart_db.tpolidm_daily pol
    LEFT JOIN vn_published_datamart_db.tagtdm_daily cwa 
        ON pol.wa_code = cwa.agt_code
    LEFT JOIN vn_published_datamart_db.tagtdm_daily csa 
        ON pol.sa_code = csa.agt_code
    WHERE  cwa.loc_cd LIKE 'DMO%'
        OR cwa.loc_cd LIKE 'FPC%'
        OR csa.loc_cd LIKE 'DMO%'
        OR csa.loc_cd LIKE 'FPC%'
)dmtm
    ON ucm_hist.po_num = dmtm.po_num --exclude owners that are joint DMTM customers

LEFT JOIN vn_lab_project_scratch_db.dpdnt_ids dpd
    on ucm_hist.pol_num=dpd.pol_num;
