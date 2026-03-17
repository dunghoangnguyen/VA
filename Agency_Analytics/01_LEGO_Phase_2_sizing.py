# Databricks notebook source
# MAGIC %md
# MAGIC ### Check and/or run the following Notebooks:
# MAGIC
# MAGIC 1. https://adb-2294815648411921.1.azuredatabricks.net/?o=2294815648411921#notebook/762980468658015/command/762980468658016
# MAGIC 2. https://adb-2294815648411921.1.azuredatabricks.net/editor/notebooks/1408503140409838?o=2294815648411921#command/1408503140409988
# MAGIC 3. https://adb-2294815648411921.1.azuredatabricks.net/?o=2294815648411921#notebook/472336847788005/command/472336847788006
# MAGIC 4. https://adb-2294815648411921.1.azuredatabricks.net/?o=2294815648411921#notebook/4372496396309830/command/4372496396309838
# MAGIC 5. Upload ilp_taar.csv from "\Data Analytics\02. Use Cases\2024\LEGO\Sizing\TAAR\" to "/lab/vn/project/cpm/LEGO/TAAR/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Make sure the following files are updated and available:
# MAGIC <strong>1. /mnt/lab/vn/project/cpm/LEGO/preapproved_uw_yyyymmdd.parquet<br>
# MAGIC <strong>2. /mnt/lab/vn/project/cpm/M25/<br>
# MAGIC <strong>3. /mnt/lab/vn/project/scratch/agent_activation/merged_target_activation.parquet<br>
# MAGIC <strong>4. /mnt/lab/vn/project/cpm/Adhoc/maturity_policies_2024_dtl_w_excl.csv<br>
# MAGIC <strong>5. /mnt/lab/vn/project/cpm/LEGO/TAAR/ilp_taar.csv<br>

# COMMAND ----------

# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge all additional data points

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

run_date = '20241010'
lst_mthend='2024-09-30'
mthend_sht=lst_mthend[:4]+lst_mthend[5:7]

cus_dtl = spark.read.format("parquet").load("/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TCUSTDM_DAILY/")
cus_dtl = cus_dtl.select(*[col.lower() for col in cus_dtl.columns])

agt_dtl = spark.read.format("parquet").load("/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TAGTDM_DAILY/")
agt_dtl = agt_dtl.select(*[col.lower() for col in agt_dtl.columns])

agency_preapproved_uw_df = spark.read.format("parquet").load(f"/mnt/lab/vn/project/cpm/LEGO/preapproved_uw_{run_date}.parquet")
existing_leads_score = spark.read.format("parquet").load("/mnt/prod/Curated/VN/Master/VN_CURATED_CUSTOMER_ANALYTICS_DB/EXISTING_CUSTOMER_SCORE/")
fully_paid = spark.read.format("parquet").load("/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/")

merged_cols = [
"po_num", "mar_stat_cat", "clm_6m_amt", "clm_6m_cnt_cat", "clm_6m_ratio_cat"
]
merged_activation = spark.read.format("parquet").load("/mnt/lab/vn/project/scratch/agent_activation/merged_target_activation.parquet")\
                     .select(*merged_cols)

#agency_preapproved_uw_df = agency_preapproved_uw_df.filter(F.col("exclusion")=="N")
#agency_preapproved_uw_df.createOrReplaceTempView("agency_preapprv")
mat_cols = [
"po_num", "maturing_policy", "maturing_product_type", "maturing_product", "policy_status", "maturity_month",
"maturity_date", "maturity_ape", "maturity_serving_agent", "loc_code", "agt_status", "agt_rltnshp", "tier",
"sm_code", "sm_name", "rh_name"
]
maturity_list = spark.read.format("parquet").load(f"/mnt/lab/vn/project/cpm/Adhoc/Maturity/maturity_policies_dtl_w_excl_{mthend_sht}.parquet")\
                     .select(*mat_cols)\
                     .withColumnRenamed("agt_status", "mat_agt_status")\
                     .withColumnRenamed("agt_rltnshp", "mat_agt_rltnshp")\
                     .withColumnRenamed("tier", "mat_agt_tier")

existing_leads_score = existing_leads_score.filter(F.col("image_date") == lst_mthend)\
                            .select("po_num", "decile")\
                            .withColumn("decile_cat", 
                                        F.when(F.col("decile").isin(1,2,3), "HP-Top3").otherwise("Others"))
#print(existing_leads_score.count())

fully_paid = fully_paid.filter(F.col("image_date") == lst_mthend)\
                .select("po_num", "pol_num", "pol_stat_cd", "pol_stat_desc", "tot_ape", "base_ape", "rid_ape")

# Add a new column named "fully_paid_ape"
fully_paid = fully_paid.withColumn("fully_paid_ape", 
                                   F.when((F.col("pol_stat_cd") == '3') & (F.col("tot_ape") > 0), F.col("tot_ape"))
                                    .otherwise(F.col("base_ape") + F.col("rid_ape")))

# Group the dataframe by "po_num" and sum the "fully_paid_ape"
fully_paid_grouped = fully_paid.groupBy("po_num")\
        .agg(   
             F.count("pol_num").alias("no_fully_paid_pols"),
             F.sum("fully_paid_ape").alias("fully_paid_ape")
        )

# Add a new column "fully_paid_ind" and set the value to 1 if "fully_paid_ape" > 0
fully_paid_grouped = fully_paid_grouped.withColumn("fully_paid_ind", 
                                                   F.when(F.col("fully_paid_ape") > 0, 1).otherwise(0))

m25_cols = ["po_num", "cus_gender", "birth_dt", "bday_ind", "bday_month", 
            "avy3_ind", "avy5_ind", "avy7_ind", "avy10_ind", "avyo_ind", "avy_ind", "avy_month", 
            "cashcoupon_ind", "cashcoupon_value", "mat_ind", "mat_month", "mat_value", "income_decile", 
            "wallet_rem", "rep_decile", "rep_rank"
]
m25_list = spark.read.format("parquet").load(f"/mnt/lab/vn/project/cpm/M25/image_date={lst_mthend}/")\
              .select(*m25_cols)
#print(m25_list.count())

income_based_decile = spark.read.format("parquet").load(f"/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_AGENCY/image_date={lst_mthend}/")
protection_gap = income_based_decile.filter((F.col('adj_mthly_incm')>0))\
                        .withColumn('protection_income%', F.col('f_with_dependent')*100*(F.col('protection_fa_all_imp') / (120*F.least(F.col('adj_mthly_incm'),F.lit(20000)))))\
                        .withColumn('protection_income_grp', F.when(F.col('f_with_dependent') != 1 , '5. No Dependent')\
                            .otherwise(F.when(F.col('protection_income%')>75, '1. 75% Income Protection')\
                            .otherwise(F.when(F.col('protection_income%')>50, '2. 50% Income Protection')\
                                .otherwise(F.when(F.col('protection_income%')>20, '3. 20% Income Protection')\
                                    .otherwise(F.when(F.col('protection_income%')>0, '4. below 20% Income Protection')\
                                    .otherwise('5. No dependent/Protection Pols'))))))\
                        .select('po_num','protection_income%', 'protection_income_grp')


ilp_taar = spark.read.format("csv").option("header", "true").load("/mnt/lab/vn/project/cpm/LEGO/TAAR/ilp_taar.csv")\
                .select(F.col("cli_num").cast("string").alias("po_num"), 
                        F.col("ilp_taar"),
                        #F.col("ilp_taar_cat")
                        )\
                .withColumn("ilp_taar_cat", 
                        F.when((F.col("ilp_taar").isNull()) | (F.col("ilp_taar") <= 0), "0. No SA")
                         .when((F.col("ilp_taar")/1000000 <= 500), "1. <=500M")
                         .when((F.col("ilp_taar")/1000000 <= 1000), "2. 500M-1B")
                         .when((F.col("ilp_taar")/1000000 <= 2000), "3. 1-2B")
                         .when((F.col("ilp_taar")/1000000 <= 3000), "4. 2-3B")
                         .when((F.col("ilp_taar")/1000000 <= 5000), "5. 3-5B")
                         .when((F.col("ilp_taar")/1000000 <= 7000), "6. 5-7B")
                         .when((F.col("ilp_taar")/1000000 <= 10000), "7. 7-10B")
                         .when((F.col("ilp_taar")/1000000 > 10000), "8. >10B")
                         .otherwise("9. Exceeded")
                )

rider_holding_string = '''
with rider_typ as (
select  pol_num
        , case when pln.prod_typ='TP_RIDER' then 'Y' else 'N' end as tp_rider
        , case when pln.prod_typ='CI_RIDER' then 'Y' else 'N' end as ci_rider
        , case when pln.prod_typ='MC_RIDER' then 'Y' else 'N' end as mc_rider
        , case when pln.prod_typ='ADD_RIDER' then 'Y' else 'N' end as add_rider
        , case when pln.prod_typ='HC_RIDER' then 'Y' else 'N' end as hc_rider
        , case when pln.prod_typ='TPD_RIDER' then 'Y' else 'N' end as tpd_rider
from    vn_published_casm_cas_snapshot_db.tcoverages cvg inner join
        vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code and cvg.vers_num=pln.vers_num
where   cvg.cvg_typ<>'B'
    and cvg.image_date='{lst_mthend}'
    and (pln.prod_typ is null or
         pln.prod_typ not in ('WOP_RIDER','WOD_RIDER','PW_RIDER','WOC_RIDER')) -- Exclude waiver riders
), riders as (
select  cvg.pol_num
        ,cvg.ins_typ
        ,max(tp_rider) as tp_rider
        ,max(ci_rider) as ci_rider
        ,max(mc_rider) as mc_rider
        ,max(add_rider) as add_rider
        ,max(hc_rider) as hc_rider
        ,max(tpd_rider) as tpd_rider
from    vn_published_casm_cas_snapshot_db.tcoverages cvg left join
        rider_typ on cvg.pol_num=rider_typ.pol_num
where   cvg.cvg_typ='B'
    and cvg.image_date='{lst_mthend}'
group by cvg.pol_num, cvg.ins_typ
)
select  po_num
        --, pol.pol_num
        , max(nvl(tp_rider,'N')) tp_rider_ind
        , max(nvl(ci_rider,'N')) ci_rider_ind
        , max(nvl(mc_rider,'N')) mc_rider_ind
        , max(nvl(add_rider,'N')) add_rider_ind
        , max(nvl(hc_rider,'N')) hc_rider_ind
        , max(nvl(tpd_rider,'N')) tpd_rider_ind
from    hive_metastore.vn_curated_datamart_db.tpolidm_daily pol inner join
        riders cvg on pol.pol_num=cvg.pol_num inner join
        vn_published_cas_db.tfield_values fld on cvg.ins_typ=fld.fld_valu and fld.fld_nm='INS_TYP'
where   1=1
    and pol.PLAN_CODE not like 'EV%'    -- Exclude exchange rate conversion products
    and pol.pol_stat_cd in ('1','2','3','5','7','9') -- Only premium paying or holiday
group by
        pol.po_num
'''
rider_holding = sql_to_df(rider_holding_string, 1 , spark)

po_base_for_string = '''
with pol_base_for as (
  select distinct cpl.cli_num po_num, case when fld.fld_valu_desc_eng in ('Owner','payor') then 'Self' else fld.fld_valu_desc_eng end as rel_to_insrd
  from    vn_published_casm_cas_snapshot_db.tclient_policy_links cpl inner join
          vn_published_casm_cas_snapshot_db.tpolicys pol on pol.pol_num=cpl.pol_num and pol.image_date=cpl.image_date and cpl.link_typ='O' and cpl.rec_status='A' inner join
          vn_published_casm_cas_snapshot_db.tfield_values fld on cpl.rel_to_insrd=fld.fld_valu and cpl.image_date=fld.image_date and fld.fld_nm='REL_TO_INSRD'
  where   rel_to_insrd is not null
    and   pol_stat_cd in ('1','2','3','5','7')
    and   to_date(cpl.image_date)='{lst_mthend}'
)
select po_num
        , sum(case when rel_to_insrd='Self' then 1 else 0 end) base_self_ins_ind
        , sum(case when rel_to_insrd='parent' then 1 else 0 end) base_parent_ins_ind
        , sum(case when rel_to_insrd='Spouse' then 1 else 0 end) base_spouse_ins_ind
        , sum(case when rel_to_insrd='Child' then 1 else 0 end) base_child_ins_ind
        , sum(case when rel_to_insrd not in ('Self','parent','Spouse','Child') then 1 else 0 end) base_oth_ins_ind
from   pol_base_for   
group by po_num
'''
po_base_for = sql_to_df(po_base_for_string, 1, spark)

po_rider_for_string = '''
with rider_for as (
  select distinct cpl.cli_num po_num, case when fld.fld_valu_desc_eng in ('Owner','payor') then 'Self' else fld.fld_valu_desc_eng end as rel_to_insrd
  from    vn_published_casm_cas_snapshot_db.tclient_policy_links cpl inner join
          vn_published_casm_cas_snapshot_db.tpolicys pol on pol.pol_num=cpl.pol_num and pol.image_date=cpl.image_date and cpl.link_typ='O' and cpl.rec_status='A' inner join
          vn_published_casm_cas_snapshot_db.tcoverages cvg on pol.pol_num=cvg.pol_num and pol.image_date=cvg.image_date inner join
          vn_published_casm_cas_snapshot_db.tfield_values fld on cvg.rel_to_insrd=fld.fld_valu and cvg.image_date=fld.image_date and fld.fld_nm='REL_TO_INSRD'
  where   cvg.rel_to_insrd is not null
    and   pol_stat_cd in ('1','2','3','5','7')
    and   cvg.cvg_typ='R'    
    and   to_date(cpl.image_date)='{lst_mthend}'
)
select po_num
        , sum(case when rel_to_insrd='Self' then 1 else 0 end) rid_self_ins_ind
        , sum(case when rel_to_insrd='parent' then 1 else 0 end) rid_parent_ins_ind
        , sum(case when rel_to_insrd='Spouse' then 1 else 0 end) rid_spouse_ins_ind
        , sum(case when rel_to_insrd='Child' then 1 else 0 end) rid_child_ins_ind
        , sum(case when rel_to_insrd not in ('Self','parent','Spouse','Child') then 1 else 0 end) rid_oth_ins_ind
from   rider_for
group by po_num
'''
po_rider_for = sql_to_df(po_rider_for_string, 1, spark)

merged_df = agency_preapproved_uw_df.join(existing_leads_score, on="po_num", how="left")\
                .join(merged_activation,  on="po_num", how="left")\
                .join(m25_list,           on="po_num", how="left")\
                .join(protection_gap,     on="po_num", how="left")\
                .join(maturity_list,      on="po_num", how="left")\
                .join(ilp_taar,           on="po_num", how="left")\
                .join(fully_paid_grouped, on="po_num", how="left")\
                .join(po_base_for,        on="po_num", how="left")\
                .join(po_rider_for,       on="po_num", how="left")\
                .join(rider_holding,      on="po_num", how="left")\
                .withColumn("hcr_ind", F.col("hc_rider_ind"))\
                .withColumn("mc_ind", F.col("mc_rider_ind"))

merged_df = merged_df\
            .withColumn("last_pol_cat",
                        F.when((F.col("agt_status")=="TERMINATED") |
                               ((F.col("agt_rltnshp").isin("2.Collector","3.SM","4.Unknown")) &
                                (F.col("tier").isin("TOT","COT","MDRT","Platinum","Gold","Silver")==False)
                               ), F.lit("Unassigned")).otherwise(F.col("last_pol_cat")))\
            .withColumn("months_since_lst_pur",
                        F.when(F.col("month_since_lst_eff_dt")<3, "1.1-3mth")\
                        .when(F.col("month_since_lst_eff_dt")<6, "2.3-6mth")\
                        .when(F.col("month_since_lst_eff_dt")<9, "3.6-9mth")\
                        .when(F.col("month_since_lst_eff_dt")<12, "4.<1yr")\
                        .when(F.col("month_since_lst_eff_dt")<24, "5.1-2yr").otherwise("6.>2yr")
                        )\
            .withColumn("decile_cat", F.when(F.col("decile").isNull(), "Others").otherwise(F.col("decile_cat")))

merged_df = merged_df.fillna({'ilp_taar_cat':'0. No SA', 'tier':'Unranked', 'protection_income_grp': '5. No dependent/Protection Pols'})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data sizing checking

# COMMAND ----------

#.filter(F.col("last_pol_cat").isin("Active","Unassigned"))\
merged_df\
  .filter((F.col("exclusion") == "N")
          &(F.col("cus_gender").isin("F","M"))
          &(F.col("tier").isin("TOT","COT","MDRT","Platinum","Gold","Silver")) 
          #&(F.col("last_pol_cat")).isin("Active")          
          &(F.col("ilp_taar_cat").isin("","0. No SA","1. <=500M","9. Exceeded")==False)
          &(F.col("decile_cat") == "HP-Top3")
          &(F.col("birth_dt") > F.lit('1955-12-31'))
          )\
  .groupBy("exclusion"
      #"decile", "ilp_taar_cat", "last_pol_cat", "tier", "decile_cat"
      )\
  .agg(
  F.countDistinct(F.col("po_num")).alias("lead_count"),
  F.countDistinct(F.col("agt_code")).alias("agent_count")
)\
.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter by the following exclusion rules:
# MAGIC <strong>Pass UW eligibility<br>
# MAGIC <strong>Select only individual customers<br>
# MAGIC <strong>Active MPro+ agents only<br>
# MAGIC <strong>ILP TAAR >= 500m VND<br>
# MAGIC <strong>Age is below 69<br>
# MAGIC <strong>Top3 decile<br>
# MAGIC <strong>Agent serving 10 customers or less

# COMMAND ----------

filtered_cols = [
  "po_num", "cus_gender", "birth_dt", "age", "age_cat", "ilp_ind", "ul_ind", "trop1_ind", "trop2_ind", "mc_ind", "hcr_ind", "agt_code",
  "agt_status", "agt_rltnshp", "rank_code", "tier", "last_pol_cat", "ins_typ_count", "inforce_pol", "inforce_pol_cat", "investment_pol", "term_pol", "endow_pol", "whole_pol", "health_indem_pol", "total_ape", "total_ape_cat", "rider_cnt", "rider_cnt_cat", "rider_ape", "rider_ape_cat", "client_tenure", "client_tenure_cat", "adj_mthly_incm", "adj_mthly_incm_cat", "protection_income%", "protection_income_grp", "wallet_rem", "lst_eff_dt", "month_since_lst_eff_dt", "months_since_lst_pur", "lst_prod_typ", "inforce_ind", "decile", "decile_cat", "mar_stat_cat", "bday_ind", "bday_month", "avy3_ind", "avy5_ind", "avy7_ind", "avy10_ind", "avyo_ind", "avy_ind", "avy_month", "mat_ind", "mat_month", "maturing_policy", "maturing_product_type", "maturing_product", "policy_status", "maturity_month", "maturity_date", "maturity_ape", "maturity_serving_agent", "loc_code", "mat_agt_status", "mat_agt_rltnshp", "mat_agt_tier", "sm_code", "sm_name", "rh_name", "no_fully_paid_pols", "fully_paid_ape", "fully_paid_ind", "base_self_ins_ind", "base_parent_ins_ind", "base_spouse_ins_ind", "base_child_ins_ind", "base_oth_ins_ind", "rid_self_ins_ind", "rid_parent_ins_ind", "rid_spouse_ins_ind", "rid_child_ins_ind", "rid_oth_ins_ind", "tp_rider_ind", "ci_rider_ind", "add_rider_ind", "tpd_rider_ind", "ilp_taar", "ilp_taar_cat"
]
#.filter(F.col("last_pol_cat").isin("Active","Unassigned"))\
filtered_df = merged_df.filter(
                  (F.col("exclusion") == "N") &
                  (F.col("cus_gender") != "G") &
                  (F.col("tier").isin("TOT","COT","MDRT","Platinum","Gold","Silver")) &
                  (F.col("ilp_taar_cat").isin("","0. No SA","1. <=500M","9. Exceeded")==False) &
                  #(F.col("last_pol_cat").isin("Active")) &
                  (F.col("decile_cat") == "HP-Top3") &
                  (F.col("birth_dt") > F.lit('1955-12-31'))
                  )\
  .select(*filtered_cols)

#filtered_df.createOrReplaceTempView("filtered_df")
#filtered_df.toPandas().to_csv(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/Preapproved_UW_sizing_rev_{run_date}_ALL.csv", index=False, header=True, encoding="utf-8-sig")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Proposition building for LEGO Phase 2
# MAGIC <strong>Borderline Age<br>
# MAGIC <strong>Maturing<br>
# MAGIC <strong>Protection gap<br>
# MAGIC <strong>Anniversary<br>
# MAGIC <strong>Birthday<br>
# MAGIC <strong>PO have only riders

# COMMAND ----------

lego2_propostion = filtered_df.withColumn("proposition",
    F.when((F.col("age").isin(25,30,34,39,49,69)) &
           (F.col("bday_month") > 8), "1. Borderline age")
    .when(F.col("mat_month").isin("09","10","11","12"), "2. Maturing")
    .when(F.col("protection_income_grp").isin("3. 20% Income Protection","4. below 20% Income Protection","5. No dependent/Protection Pols"), "3. Protection Gap > 75%")
    .when(F.col("avy_month").isin("09","10","11","12"), "4. Anniversary")
    .when(F.col("bday_month") > 8, "5. Birthday")
    .when(F.col("base_self_ins_ind") == 0, "6. PO have only riders")
    .otherwise("7. Others")
)

lego2_final = lego2_propostion.toPandas()
lego2_final.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/Preapproved_UW_sizing_rev_{run_date}.csv", index=False, header=True, encoding="utf-8-sig")
print(lego2_final.shape)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Put Lego2 Proposition data into PII template

# COMMAND ----------

lego2_propostion_pii = lego2_propostion.alias("lego2") \
        .join(cus_dtl.alias("cus"), F.col("lego2.po_num") == F.col("cus.cli_num"), "left") \
        .join(agt_dtl.alias("agt"), F.col("lego2.agt_code") == F.col("agt.agt_code"), "left") \
        .select(F.col("lego2.agt_code").alias("agent_code"),
            F.col("agt.agt_nm").alias("agent_name"),
            F.col("lego2.tier").alias("MPRO_TITLE"),
            F.col("cus.cli_nm").alias("customer_name"),
            F.col("cus.sex_code").alias("cus_sex_code"),
            F.to_date(F.col("cus.birth_dt")).alias("cus_birth_dt"),
            F.col("cus.email_addr").alias("cus_email"),
            F.col("cus.mobl_phon_num").alias("cust_phone_number"),
            F.col("cus.occp_code").alias("cust_occp_code"),
            F.col("cus.occp_desc").alias("cust_occp_desc"),
            F.col("cus.sms_ind").alias("sms_ind"),
            F.col("cus.sms_eservice_ind").alias("sms_eservice_ind")
            )

# COMMAND ----------

#lego2_propostion_pii.toPandas().to_csv("/dbfs/mnt/lab/vn/project/cpm/LEGO/lego2_proposition_pii.csv", index=False, header=True, encoding="utf-8-sig")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Agent's individual information

# COMMAND ----------

size_cols = [
  "po_num", "cus_gender", "age", "age_cat", "ilp_ind", "ul_ind", "trop1_ind", "trop2_ind", "mc_ind", "hcr_ind", "ins_typ_count", "inforce_pol", "inforce_pol_cat", "investment_pol", "term_pol", "endow_pol", "whole_pol", "health_indem_pol", "total_ape_cat", "rider_cnt_cat", "rider_ape_cat", "client_tenure_cat", "adj_mthly_incm_cat", "protection_income_grp", "lst_eff_dt", "months_since_lst_pur", "lst_prod_typ", "decile_cat", "mar_stat_cat", "bday_ind", "bday_month", "avyo_ind", "avy_ind", "mat_ind", "fully_paid_ind", "base_self_ins_ind", "base_parent_ins_ind", "base_spouse_ins_ind", "base_child_ins_ind", "base_oth_ins_ind", "rid_self_ins_ind", "rid_parent_ins_ind", "rid_spouse_ins_ind", "rid_child_ins_ind", "rid_oth_ins_ind", "tp_rider_ind", "ci_rider_ind", "add_rider_ind", "tpd_rider_ind", "ilp_taar_cat", "agt_code", "rank_code", "tier", "last_pol_cat"
]

# Retrieve agents loc_code, first available sm, RH and email address
agt_dtl = spark.sql('''
  select distinct 
          agt_code,
          agt_nm,
          email_addr agt_email_addr,
          case when email_addr like 'Withdraw%@manulife.%' then 'W'
               when email_addr like '%@manulife.%' then 'Y' 
               else 'N' end as valid_manu_email,
          coalesce(manager_code_0, manager_code_1, manager_code_2, manager_code_3, manager_code_4, manager_code_5, manager_code_6, 'Open') sm_code,
          coalesce(manager_name_0, manager_name_1, manager_name_2, manager_name_3, manager_name_4, manager_name_5, manager_name_6, 'Open') sm_name,
          rh_name
  from    vn_curated_datamart_db.tagtdm_daily agt inner join
          vn_curated_reports_db.loc_to_sm_mapping loc on agt.loc_cd=loc.loc_cd                 
  where   comp_prvd_num in ('01','08','97','98')
      or  channel='Agency'
''')

lego2_propostion_dtl = lego2_propostion.select(*size_cols).join(agt_dtl, on="agt_code", how="left").dropDuplicates()

#lego2_propostion_dtl.toPandas().to_csv(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/Preapproved_UW_sizing_rev_{run_date}_Agents.csv", index=False, header=True, encoding="utf-8-sig")
