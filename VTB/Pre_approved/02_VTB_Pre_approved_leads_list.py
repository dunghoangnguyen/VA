# Databricks notebook source
# MAGIC %md
# MAGIC ### Make sure the following files are updated and available:
# MAGIC <strong>1. /mnt/lab/vn/project/cpm/VTB/Pre_approved/VTB_preapproved_ư_yyyymmdd.csv<br>

# COMMAND ----------

# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data sizing checking
# MAGIC VTB customers<br>
# MAGIC Tenure (>1 year)<br>
# MAGIC Eligible for Pre-approved UW<br>
# MAGIC SA offer >= 500M<br>
# MAGIC Top3 decile (optional)

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

run_date = "20241029" #pd.Timestamp.now().strftime("%Y%m%d")

vtb_propostion = spark.read.csv(f"/mnt/lab/vn/project/cpm/VTB/Pre_approved/VTB_preapproved_uw_{run_date}.csv", inferSchema=True, header=True) \
    .filter((F.col("exclusion") == "N") &
            ((F.col("ilp_offr") >= 500000000) | (F.col("ul_offr") >= 500000000) | (F.col("trop_offr") >= 500000000)) &
            (F.col("client_tenure") >= 1) &
            (F.col("agt_status") == "INFORCE")
            )
vtb_propostion.createOrReplaceTempView("vtb_propostion")

vtb_lead_count = vtb_propostion.count()
vtb_top3decile_count = vtb_propostion.filter(F.col("decile").isin(1,2,3)).count()
print(f"Leads count: {vtb_lead_count}, top3 decile count: {vtb_top3decile_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refresh to retrieve latest customer and agent's information

# COMMAND ----------

# Get latest customer PII & contact information
cus_pii = spark.sql('''
  select distinct
          cus.CLI_NUM po_num
          , cus.CLI_TYP
          , cus.CLI_NM po_name
          , cus.SEX_CODE cus_gender
          , to_date(cus.BIRTH_DT) cus_birth_dt
          , floor(datediff('2025-12-31', cus.BIRTH_DT)/365.25) age
          , trim(cus.EMAIL_ADDR) cust_email
          , cus.MOBL_PHON_NUM cust_phone_number
          , cus.OCCP_CODE cust_occp_code
          , cus.OCCP_DESC cust_occp_desc
          , case when cus.sms_ind='Y' and cus.sms_stat_cd <> '02' then 'Y' else 'N' end sms_ind
          , case when trim(cus.EMAIL_ADDR) like '%@%.%' and len(trim(cus.EMAIL_ADDR))>6 then 'Y' else 'N' end email_ind
          , nvl(sms.OTT_CONSENT,'N') ott_consent
  from    vn_curated_datamart_db.tcustdm_daily cus left join
          vn_published_cics_db.tsms_clients sms on cus.CLI_NUM=sms.CLI_NUM                    
  where   cus.sex_code in ('F','M')
     and  cus.cli_typ = 'Individual' -- Exclude corporates
''')
cus_pii.filter(F.col("age") > 21)
cus_pii.createOrReplaceTempView("cus_pii")
#cus_pii.groupBy("cli_typ","cus_gender","age").agg(F.count("po_num").alias("client_count")).display()

# Get latest agent PII & contact information
agt_pii = spark.sql('''
select distinct
        agt.agt_code agt_code,
        agt.agt_nm agt_name,
        can.sex_code agt_gender,
        can.email_addr agt_email,
        case when lower(trim(can.email_addr)) like '%manulife.com%' then 'Y' else 'N' end valid_agt_manu_email,
        agt.mobl_phon_num agt_mobile,
        agt.STAT_CD agt_status,
        agt.loc_code,
        agt.br_code branch_code,
        drt.agt_cd sm_code,
        agt1.agt_nm sm_nm
from    vn_published_ams_db.tams_agents agt left join
        vn_published_ams_db.tams_candidates   can
on      agt.can_num = can.can_num and
        agt.chnl_cd = '03' and -- Only select Bancas
        agt.comp_prvd_num in ('52','53') left join -- Only VTB
        vn_published_ams_db.tams_agt_rpt_rels drt
on      agt.agt_code = drt.sub_agt_cd and
        drt.rpt_level = 1 left join
        vn_published_ams_db.tams_agents       agt1
on      drt.agt_cd = agt1.agt_code
where   1=1
  --and   agt.TRMN_DT is null
  --and   can.sex_code in ('F','M')
''')
agt_pii.createOrReplaceTempView("agt_pii")
#print(agt_pii.count())
#cus_pii.agg(F.count("po_num").alias("rows"), F.countDistinct("po_num").alias("records")).display()
#agt_pii.groupBy("mpro_title","mpro_rank").agg(F.count("agt_code").alias("rows"), F.countDistinct("agt_code").alias("records")).display()
#agt_pii.filter(F.col("agt_code").isin("DR391","2FQ94","7TC6F","DR336")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve policy and related rider information

# COMMAND ----------

# Identify the year first VTB policy was issued and is still inforce
frst_pol = spark.sql('''
select  distinct po_num, min(to_date(FRST_ISS_DT)) frst_pol_iss_yr, max(floor(months_between(to_date(pd_to_dt), to_date(pol_eff_dt)) / 12) + 1) payment_year
from    vn_curated_datamart_db.tpolidm_daily
where   pol_stat_cd in ('1','3','5')
    and (dist_chnl_cd in ('52','53') or sa_loc_code like 'VTI%')
group by po_num
''').dropDuplicates()
frst_pol.createOrReplaceTempView("frst_pol")

# Identify and rank last policies served
multiple_pol = spark.sql('''
with inf_pol as (
select  po_num, pol_num policy_number, pol_stat_desc, sa_code agt_code,
        row_number() over (partition by pol.po_num order by pol.POL_ISS_DT DESC) rn
from    vn_curated_datamart_db.tpolidm_daily pol inner join 
        agt_pii agt on pol.SA_CODE=agt.agt_code
where   (dist_chnl_cd in ('52','53') or sa_loc_code like 'VTI%')
    and pol_stat_cd in ('1','3','5')
    and pol_iss_dt < '2024-10-01'
qualify rn = 1
), multi_ilp as (
select  po_num, count(pol_num) no_ilp_pol
from    vn_curated_datamart_db.tpolidm_daily
where   pol_stat_cd in ('1','3','5')
    and PLAN_CODE='RUV04'
    and PO_NUM=INSRD_NUM
group by po_num    
)
select  distinct inf_pol.po_num, policy_number, agt_code,
        case when no_ilp_pol>1 then 'Y' else 'N' end multiple_ilp_ind
from    inf_pol left join
        multi_ilp on inf_pol.po_num=multi_ilp.po_num
''').dropDuplicates(["po_num"])
multiple_pol.createOrReplaceTempView("multiple_pol")

#multiple_pol.agg(F.count("po_num").alias("rows"),F.countDistinct("po_num").alias("records")).display()
#multiple_pol.filter(F.col("po_num").isin('2807568339','2806609958','2807138791','2807565959','2807292392','2806994161','2807566763')).display()

rider_dtl = spark.sql('''
with rider_typ as (
select  pol_num
        ,cvg.CLI_NUM insrd_num
        , cvg.PLAN_CODE plan_code
        , pln.MKT_PLAN_NM plan_name
        , case when pln.prod_typ='TP_RIDER' then 'Y' else 'N' end as tp_rider
        , case when pln.prod_typ='CI_RIDER' then 'Y' else 'N' end as ci_rider
        , case when pln.prod_typ='MC_RIDER' then 'Y' else 'N' end as mc_rider
        , case when pln.prod_typ='ADD_RIDER' then 'Y' else 'N' end as add_rider
        , case when pln.prod_typ='HC_RIDER' then 'Y' else 'N' end as hc_rider
        , case when pln.prod_typ='TPD_RIDER' then 'Y' else 'N' end as tpd_rider
        , case when pln.prod_typ not in ('TP_RIDER','CI_RIDER','MC_RIDER','ADD_RIDER','HC_RIDER','TPD_RIDER') then 'Y' else 'N' end as oth_rider
from    vn_published_cas_db.tcoverages cvg inner join
        vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code and cvg.vers_num=pln.vers_num
where   cvg.cvg_typ<>'B'
), po_insrd as (
select  po_num, 'Y' insrd_is_po
from    vn_curated_datamart_db.tpolidm_daily
where   pol_stat_cd in ('1','3','5')
    and PO_NUM=INSRD_NUM 
group by po_num
)
select  pol.po_num
        ,concat_ws(',',collect_set(rider_typ.plan_code)) rider_code
        ,concat_ws(',',collect_set(rider_typ.plan_name)) rider_name
        ,max(tp_rider) as tp_rider
        ,max(ci_rider) as ci_rider
        ,max(mc_rider) as mc_rider
        ,max(add_rider) as add_rider
        ,max(hc_rider) as hc_rider
        ,max(tpd_rider) as tpd_rider
        ,max(oth_rider) as oth_rider
        ,max(coalesce(po_insrd.insrd_is_po,'N')) insrd_is_po       
from    vn_curated_datamart_db.tpolidm_daily pol left join
        rider_typ on pol.pol_num=rider_typ.pol_num and pol.po_num=rider_typ.insrd_num left join
        po_insrd on pol.po_num=po_insrd.po_num
where   pol.pol_stat_cd in ('1','3','5')
and     rider_typ.pol_num is not null
group by pol.po_num
''')
rider_dtl.createOrReplaceTempView("rider_dtl")
#rider_dtl.filter((F.col("insrd_is_po")=="N") &
#                 (F.col("po_num")=="2800029630")
#                 ).display()
#rider_dtl.limit(5).display()
#rider_dtl.groupBy("po_no_rider").agg(F.count("po_num").alias("rows"), F.countDistinct("po_num").alias("records")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reload SA offer and/or HCR/MC benefits if needed

# COMMAND ----------

mc_benefit = spark.read.format("csv").load("/mnt/lab/vn/project/cpm/VTB/Pre_approved/TAAR/vtb_taar.csv", inferSchema=True, header=True) \
    .select("cli_num","mc_benefit") \
    .withColumnRenamed("cli_num", "po_num")
mc_benefit.createOrReplaceTempView("mc_benefit")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge with the new tables for the additional features

# COMMAND ----------


full_cols = [
  "po_num", "po_name", "cus_gender", "cus_birth_dt", "cust_email", "cust_phone_number", "cust_occp_code", "cust_occp_desc", "frst_pol_iss_yr", "pol_yrs", "ilp_min_prem", "agt_name", "agt_code", "mpro_title", "agt_gender", "agt_email", "valid_agt_man_email", "agt_mobile", "loc_code", "branch_code", "sm_code", "sm_name", "rh_name", "policy_number", "rider_plan_code", "rider_plan_nm", "maturity_date", "email_ind", "sms_ind", "ott_consent", "owner_more_than_1_ilp", "borderline_age", "birthday_from_nov_dec", "po_rider_only", "protection_income_grp", "mat_ind", "trop_offer", "age", "client_tenure_cat", "months_since_lst_pur", "lst_prod_typ", "decile_cat", "ilp_offr", "ul_offr"
]

new_vtb_propostion = spark.sql(f'''
with base_sa as (
    select  pol_num policy_number, sum(face_amt) as base_sa
    from    vn_published_cas_db.tcoverages
    where   cvg_typ='B'
    group by pol_num
)                    
select  l2.po_num,
        cp.po_name,
        concat('***',substr(cp.po_num,4,7)) masked_po_num,
        cp.cus_gender,
        cp.cus_birth_dt,
        cp.cust_email,
        cp.cust_phone_number,
        cp.cust_occp_code,
        cp.cust_occp_desc,
        fp.frst_pol_iss_yr,
        fp.payment_year pmt_yr,
        cast(floor(datediff(current_date, frst_pol_iss_yr)/365.25) as int) pol_yrs,
        ap.agt_code,
        ap.agt_status,
        ap.agt_name,
        ap.agt_gender,
        ap.agt_email,
        ap.valid_agt_manu_email,
        ap.agt_mobile,
        ap.loc_code,
        ap.branch_code,
        ap.sm_code,
        ap.sm_nm sm_name,
        mp.policy_number,
        concat('***',substr(mp.policy_number,4,7)) masked_policy_number,
        rd.rider_code,
        rd.rider_name,
        case when l2.maturity_date between '2024-12-01' and '2025-12-31' then l2.maturity_date end maturity_date,
        cp.email_ind,
        cp.sms_ind,
        cp.ott_consent,
        l2.hcr_ind,
        l2.mc_ind,
        nvl(mc.mc_benefit,0) mc_benefit,
        case when cp.age in (30,40,50,60) /*and month(cp.cus_birth_dt) > 10*/ then 'Y' else 'N' end borderline_age,
        case when month(cp.cus_birth_dt) > 10 then 'Y' else 'N' end birthday_from_nov_dec,
        case when rd.po_num is not null and rd.insrd_is_po='N' then 'Y' else 'N' end po_rider_only,
        case when l2.protection_income_grp in ('3. 20% Income Protection','4. below 20% Income Protection','5. No dependent/Protection Pols') then 'Y' else 'N' end
        protection_gap,
        case when l2.maturity_date between '2024-12-01' and '2025-02-28' then 'Y' else 'N' end near_maturity,
        l2.trop_offr,
        case when l2.trop_offr/1000000000 >= 1 then concat(cast(l2.trop_offr/1000000000 as decimal(4,1)),"B") 
            else concat(cast(l2.trop_offr/1000000 as int),"M")
        end trop_offr_text,
        cp.age cus_age,
        l2.decile_cat,
        sa.base_sa basic_sa,
        l2.ilp_offr,
        l2.ul_offr
from    vtb_propostion l2 inner join
        cus_pii cp      on l2.po_num=cp.po_num left join
        multiple_pol mp on l2.po_num=mp.po_num left join
        agt_pii ap      on mp.agt_code=ap.agt_code left join
        frst_pol fp     on l2.po_num=fp.po_num left join
        rider_dtl rd    on l2.po_num=rd.po_num left join
        mc_benefit mc   on l2.po_num=mc.po_num left join
        base_sa sa      on mp.policy_number=sa.policy_number  
--where 1=1
''').dropDuplicates()

# Add filter for agent serving more than 10 customers
lego2_grp_df = new_vtb_propostion.groupBy("agt_code")\
                .agg(
                F.count(F.col("po_num")).alias("serving_cus_count")
              )

new_vtb_propostion = new_vtb_propostion.join(lego2_grp_df, on="agt_code", how="left")
#new_vtb_propostion.filter(F.col("po_num").isin('2807568339','2806609958','2807138791','2807565959','2807292392','2806994161','2807566763')).display()
#new_vtb_propostion.groupBy("loc_code").agg(F.countDistinct("po_num").alias("leads"),F.countDistinct("agt_code").alias("agents")).display()

# COMMAND ----------

# Remove customers without a VTB serving agent
new_vtb = new_vtb_propostion.filter(F.col("frst_pol_iss_yr").isNotNull()).toPandas()

# Keep a group for top3 decile customers
new_vtb_top3 = new_vtb[new_vtb["decile_cat"].isin(["Top1-HP","Top2-HP","Top3-HP"])]

# Save the DataFrames to CSV files
new_vtb.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/VTB/Leads_list/VTB_Preapproved_all_{run_date}.csv", index=False, header=True, encoding="utf-8-sig")
new_vtb_top3.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/VTB/Leads_list/VTB_Preapproved_top3decile_{run_date}.csv", index=False, header=True, encoding="utf-8-sig")
print("Total customers:", new_vtb.shape[0])
print("Total customers in top3 decile:", new_vtb_top3.shape[0])

# COMMAND ----------

grouped = new_vtb.groupby(["pol_yrs","pmt_yr"])["po_num"].nunique().reset_index(name="po_count")

#grouped.display()
