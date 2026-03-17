# Databricks notebook source
# MAGIC %md
# MAGIC ### Make sure the following files are updated and available:
# MAGIC <strong>1. /mnt/lab/vn/project/cpm/LEGO/Preapproved_UW_sizing_rev_yyyymmdd_Agents.csv<br>

# COMMAND ----------

# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data sizing checking

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

run_date = "20240829" #pd.Timestamp.now().strftime("%Y%m%d")

lego2_propostion = spark.read.csv(f"/mnt/lab/vn/project/cpm/LEGO/Preapproved_UW_sizing_rev_{run_date}.csv", inferSchema=True, header=True)
lego2_propostion.createOrReplaceTempView("lego2_proposition")

mpro_title = spark.read.csv("/mnt/lab/vn/project/cpm/LEGO/mpro_title.csv", inferSchema=True, header=True)
mpro_title.createOrReplaceTempView("mpro_title")
print(lego2_propostion.count()," ", mpro_title.count())
#lego2_propostion.groupBy("age_cat").agg(F.count("po_num").alias("leads")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refresh to retrieve latest customer and agent's information

# COMMAND ----------

# Get latest customer PII & contact information
cus_pii = spark.sql('''
  select distinct
          cus.cli_num po_num
          , cus.CLI_NM po_name
          , cus.SEX_CODE cus_gender
          , to_date(cus.BIRTH_DT) cus_birth_dt
          , trim(cus.EMAIL_ADDR) cust_email
          , cus.MOBL_PHON_NUM cust_phone_number
          , cus.OCCP_CODE cust_occp_code
          , cus.OCCP_DESC cust_occp_desc
          , case when cus.sms_ind='Y' and cus.sms_stat_cd <> '02' then 'Y' else 'N' end sms_ind
          , case when trim(cus.EMAIL_ADDR) like '%@%.%' and len(trim(cus.EMAIL_ADDR))>6 then 'Y' else 'N' end email_ind
          , nvl(sms.OTT_CONSENT,'N') ott_consent
  from    vn_curated_datamart_db.tcustdm_daily cus left join
          vn_published_cics_db.tsms_clients sms on cus.CLI_NUM=sms.CLI_NUM                    
                    
''')
cus_pii.createOrReplaceTempView("cus_pii")

# Get latest agent PII & contact information
agt_pii = spark.sql('''
  select distinct 
          agt.agt_nm agt_name
          , agt.agt_code
          , coalesce(mpro.mpro_title,'Normal') mpro_title
          , case when mpro.mpro_title='TOT' then 1
                 when mpro.mpro_title='COT' then 2
                 when mpro.mpro_title='MDRT' then 3
                 when mpro.mpro_title='P' then 4
                 when mpro.mpro_title='G' then 5
                 when mpro.mpro_title='S' then 6
                 else 7
            end as mpro_rank 
          , can.SEX_CODE agt_gender
          , can.EMAIL_ADDR agt_email
          , case when email_addr like 'Withdraw%@manulife.%' then 'W'
                 when email_addr like '%@manulife.%' then 'Y' 
                 else 'N' 
            end as valid_agt_manu_email
          , agt.MOBL_PHON_NUM agt_mobile
          , agt.LOC_CODE loc_code
          , agt.BR_CODE branch_code
          , coalesce(manager_code_0, manager_code_1, manager_code_2, manager_code_3, manager_code_4, manager_code_5, manager_code_6, 'Open') sm_code
          , coalesce(manager_name_0, manager_name_1, manager_name_2, manager_name_3, manager_name_4, manager_name_5, manager_name_6, 'Open') sm_name
          , rh_name
  from    vn_published_ams_db.tams_agents agt inner join
          vn_published_ams_db.tams_candidates can on agt.CAN_NUM=can.CAN_NUM left join
          vn_curated_reports_db.loc_to_sm_mapping loc on agt.LOC_CODE=loc.loc_cd left join
          mpro_title mpro on agt.agt_code=mpro.agt_code                
  where   comp_prvd_num in ('01','08','97','98')
      or  agt.CHNL_CD='01'
''')
agt_pii.createOrReplaceTempView("agt_pii")

#cus_pii.agg(F.count("po_num").alias("rows"), F.countDistinct("po_num").alias("records")).display()
#agt_pii.groupBy("mpro_title","mpro_rank").agg(F.count("agt_code").alias("rows"), F.countDistinct("agt_code").alias("records")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Retrieve policy and related rider information

# COMMAND ----------

# Identify Bancas customer for exclusion
banca_pol = spark.sql('''
with frst_pol_cus as (
  select  po_num, dist_chnl_cd, pol_num, wa_code, sa_code, to_date(pol_eff_dt) pol_eff_dt,
          row_number() over (partition by po_num order by pol_eff_dt desc) rn
  from    vn_curated_datamart_db.tpolidm_daily pol
  where   dist_chnl_cd not in ('*','01','02','08','50') -- Exclude all policies onboarded via Agency channel
    and   pol_stat_cd in ('1','3','5')              -- Policies must still be inforce
),
tcb_onb as (
    select  po_num
    from    frst_pol_cus a inner join
            vn_curated_datamart_db.tcustdm_daily d on a.po_num=d.cli_num left join
            vn_published_ams_db.tams_agents  b on a.wa_code=b.agt_code left join
            vn_published_ams_db.tams_agents  c on a.sa_code=c.agt_code 
    where   1=1
        and (b.loc_code like 'TCB%' or
             c.loc_code like 'TCB%' or
             a.dist_chnl_cd in ('10','24','49')
             )
        and  rn=1
),
vtb_onb as (
    select  po_num
    from    frst_pol_cus a inner join
            vn_curated_datamart_db.tcustdm_daily d on a.po_num=d.cli_num left join
            vn_published_ams_db.tams_agents  b on a.wa_code=b.agt_code left join
            vn_published_ams_db.tams_agents  c on a.sa_code=c.agt_code 
    where   1=1
        and (b.loc_code like 'VTI%' or
             c.loc_code like 'VTI%' or
             a.dist_chnl_cd in ('52','53')
             )
        and  rn=1
)
select  *
from    tcb_onb
union   
select  *
from    vtb_onb
''').dropDuplicates()
banca_pol.createOrReplaceTempView("banca_pol")
#banca_pol.agg(F.count("po_num").alias("rows"), F.countDistinct("po_num").alias("records")).display()

# Identify date and year where first (non-taken) policy was issued
frst_pol = spark.sql('''
select  distinct po_num, min(to_date(FRST_ISS_DT)) frst_pol_iss_yr
from    vn_curated_datamart_db.tpolidm_daily
where   pol_stat_cd not in ('A','N','R','X')
group by po_num
''').dropDuplicates()
frst_pol.createOrReplaceTempView("frst_pol")

# Identify and rank last policies served
multiple_pol = spark.sql('''
with inf_pol as (
select  po_num, pol_num policy_number, sa_code agt_code,
        dense_rank() over (partition by pol.po_num order by agt.mpro_rank ASC, pol.POL_ISS_DT DESC) rank
from    vn_curated_datamart_db.tpolidm_daily pol inner join 
        agt_pii agt on pol.SA_CODE=agt.agt_code
where   dist_chnl_cd in ('*','01','02','08','50')        
    and pol_stat_cd in ('1','3','5')
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
where   rank=1
''').dropDuplicates(["po_num"])
multiple_pol.createOrReplaceTempView("multiple_pol")

#multiple_pol.agg(F.count("po_num").alias("rows"),F.countDistinct("po_num").alias("records")).display()
#multiple_pol.filter(F.col("po_num")=="2803558129").display() #.agg(F.count("po_num").alias("rows"),F.countDistinct("po_num").alias("records")).display()

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
# MAGIC ### Check one last time against the eligible list

# COMMAND ----------

# Load latest NB UW exclusion list
excl_cus = spark.read.parquet("/mnt/lab/vn/project/cpm/LEGO/excl_cus/")

excl_cus.createOrReplaceTempView("excl_cus")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge lego2 with the new tables for the additional features

# COMMAND ----------


full_cols = [
  "po_num", "po_name", "cus_gender", "cus_birth_dt", "cust_email", "cust_phone_number", "cust_occp_code", "cust_occp_desc", "frst_pol_iss_yr", "pol_yrs", "ilp_sa_offer", "ilp_min_prem", "agt_name", "agt_code", "mpro_title", "agt_gender", "agt_email", "valid_agt_man_email", "agt_mobile", "loc_code", "branch_code", "sm_code", "sm_name", "rh_name", "policy_number", "rider_plan_code", "rider_plan_nm", "maturity_date", "email_ind", "sms_ind", "ott_consent", "owner_more_than_1_ilp", "borderline_age", "birthday_from_sep_dec", "po_rider_only", "protection_income_grp", "mat_ind", "rank_code", "tier", "last_pol_cat", "age_cat", "client_tenure_cat", "lst_eff_dt", "months_since_lst_pur", "lst_prod_typ", "decile_cat", "ilp_taar_cat"
]

new_lego2_proposition = spark.sql(f'''
select  l2.po_num,
        cp.po_name,
        cp.cus_gender,
        cp.cus_birth_dt,
        cp.cust_email,
        cp.cust_phone_number,
        cp.cust_occp_code,
        cp.cust_occp_desc,
        fp.frst_pol_iss_yr,
        year(current_date) - year(frst_pol_iss_yr) pol_yrs,
        l2.ilp_taar ilp_sa_offer,
        '' ilp_min_prem,
        ap.agt_name,
        ap.agt_code,
        ap.mpro_title,
        --ap.mpro_rank,
        ap.agt_gender,
        ap.agt_email,
        ap.valid_agt_manu_email,
        ap.agt_mobile,
        ap.loc_code,
        ap.branch_code,
        ap.sm_code,
        ap.sm_name,
        ap.rh_name,
        mp.policy_number,
        rd.rider_code,
        rd.rider_name,
        case when l2.maturity_date between '2024-09-01' and '2024-12-31' then l2.maturity_date end maturity_date,
        cp.email_ind,
        cp.sms_ind,
        cp.ott_consent,
        mp.multiple_ilp_ind owner_more_than_1_ilp,
        case when l2.proposition='1. Borderline age' then 'Y' else 'N' end borderline_age,
        case when l2.bday_month > 8 then 'Y' else 'N' end birthday_from_sep_dec,
        case when rd.po_num is not null and rd.insrd_is_po='N' then 'Y' else 'N' end po_rider_only,
        case when l2.protection_income_grp in ('3. 20% Income Protection','4. below 20% Income Protection','5. No dependent/Protection Pols') then 'Y' else 'N' end
        protection_gap,
        case when l2.maturity_date between '2024-09-01' and '2024-12-31' then 'Y' else 'N' end near_maturity,
        l2.ilp_taar_cat,
        l2.decile,
        l2.tier,
        round(date_diff(current_date,cp.cus_birth_dt)/365.25,0) cus_age,
        l2.proposition
from    lego2_proposition l2 left join
        cus_pii cp      on l2.po_num=cp.po_num left join
        multiple_pol mp on l2.po_num=mp.po_num left join
        agt_pii ap      on mp.agt_code=ap.agt_code left join
        banca_pol bp    on l2.po_num=bp.po_num left join
        frst_pol fp     on l2.po_num=fp.po_num left join
        rider_dtl rd    on l2.po_num=rd.po_num left join
        excl_cus ec     on l2.po_num=ec.po_num
where   bp.po_num is null
  and   ec.exclusion_ind <> 'Y'
  and   mp.po_num is not null
''').dropDuplicates()

# Add filter for agent serving more than 10 customers
lego2_grp_df = new_lego2_proposition.groupBy("agt_code")\
                .agg(
                F.count(F.col("po_num")).alias("serving_cus_count")
              )

new_lego2_proposition = new_lego2_proposition.join(lego2_grp_df, on="agt_code", how="left") \
            .withColumn("serving_cus_cat", F.when(F.col("serving_cus_count") > 10, ">10").otherwise("<=10"))
#new_lego2_proposition.groupBy("proposition").agg(F.count("po_num").alias("rows"),F.countDistinct("po_num").alias("records")).orderBy("proposition").display()
#new_lego2_proposition.groupBy("mpro_title").agg(F.countDistinct("po_num").alias("leads"),F.countDistinct("agt_code").alias("agents")).display()

# COMMAND ----------

new_lego2_pd = new_lego2_proposition.toPandas()
#new_lego2_pd.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/Leads_list/Phase2/LEGO_Phase2_list_ALL.csv", index=False, header=True, encoding="utf-8-sig")

# Exclude non-MPRO agents
filtered_df = new_lego2_pd[new_lego2_pd["mpro_title"] != "Normal"]

# Sort the DataFrame based on cus_age (ascending) and ilp_sa_offer (descending)
sorted_df = filtered_df.sort_values(by=["cus_age", "ilp_sa_offer"], ascending=[True, False])

# Initialize empty DataFrames for leads and remain
new_lego2_leads = pd.DataFrame()
new_lego2_remain = pd.DataFrame()

# Group by agt_code and apply the criteria
for agt_code, group in sorted_df.groupby("agt_code"):
    # Split the group based on serving_cus_cat and the first 10 po_num
    group_leads = group[group["serving_cus_cat"] == "<=10"]
    group_remain = group[group["serving_cus_cat"] == ">10"]
    
    if not group_remain.empty:
        first_10 = group_remain.head(10)
        group_leads = pd.concat([group_leads, first_10])
        group_remain = group_remain.iloc[10:]
    
    # Append to the respective DataFrames
    new_lego2_leads = pd.concat([new_lego2_leads, group_leads])
    new_lego2_remain = pd.concat([new_lego2_remain, group_remain])

# Save the DataFrames to CSV files
new_lego2_leads.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/Leads_list/Phase2/LEGO_Phase2_leads_{run_date}.csv", index=False, header=True, encoding="utf-8-sig")
new_lego2_remain.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/Leads_list/Phase2/LEGO_Phase2_remain_{run_date}.csv", index=False, header=True, encoding="utf-8-sig")
filtered_df.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/Leads_list/Phase2/LEGO_Phase2_list_{run_date}.csv", index=False, header=True, encoding="utf-8-sig")
print("Total customers:", filtered_df.shape[0])
print("No opportunities:",new_lego2_leads.shape[0], "no remain:", new_lego2_remain.shape[0])
