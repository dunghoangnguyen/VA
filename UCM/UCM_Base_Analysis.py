# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window
import pandas as pd

run_date = 20240926
lst_mthend = '2024-08-31'
sht_mthend = lst_mthend[:4]+lst_mthend[5:7]

FILE_PATH = f'/mnt/lab/vn/project/cpm/LEGO/preapproved_uw_{run_date}.parquet'
MAT_PATH = f'/mnt/lab/vn/project/cpm/Adhoc/Maturity/maturity_policies_dtl_w_excl_{sht_mthend}.parquet'
ORP1_PATH = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TORPHAN_POLICIES/'
ORP2_PATH = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TORPHAN_HISTORIES/'
CUS_PATH = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_AGENCY/image_date={lst_mthend}'
POL_PATH = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_DAILY/'
VIP_PATH = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/TWRK_CLIENT_APE/'
HP_PATH = f'/mnt/prod/Curated/VN/Master/VN_CURATED_CUSTOMER_ANALYTICS_DB/EXISTING_CUSTOMER_SCORE/image_date={lst_mthend}'
LPS_PATH = '/mnt/lab/vn/project/lapse/pre_lapse_deployment/lapse_mthly/lapse_score.parquet/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load tables

# COMMAND ----------

pre_df      = spark.read.format("parquet").load(FILE_PATH)
mat_df      = spark.read.format("parquet").load(MAT_PATH).select("po_num", "maturing_policy", "maturing_product", "policy_status", "maturity_date")
orp1_df     = spark.read.format("parquet").load(ORP1_PATH).select("pol_num", "trxn_dt")
orp2_df     = spark.read.format("parquet").load(ORP2_PATH).select("pol_num", "trxn_dt")
cus_df      = spark.read.format("parquet").load(CUS_PATH).filter(F.col("unassigned_ind") == 1).select("po_num", "unassigned_ind")
pol_df      = spark.read.format("parquet").load(POL_PATH).select("po_num", "pol_num", "pol_stat_cd", "pol_stat_desc", "tot_ape", "base_ape", "rid_ape")
vip_df      = spark.read.format("parquet").load(VIP_PATH).select(F.col("cli_num").alias("po_num"), "vip_typ", "vip_typ_desc")
hp_df       = spark.read.format("parquet").load(HP_PATH).select("po_num", F.col("decile").alias("hp_decile"))
lps_df      = spark.read.format("parquet").load(LPS_PATH).filter(F.col("month_snapshot") == sht_mthend).select("pol_num", F.col("decile").alias("lapse_decile"))

# COMMAND ----------

# Add additional unassigned/orphan date
query = '''
select  POL_NUM, max(to_date(TRXN_DT)) lst_ter_date
from    vn_published_cas_db.ttrxn_histories
where   TRXN_CD = 'ORPAGN'
  and   TRXN_DESC like 'Servicing agent changed%'
  and   TRXN_DT <= '{lst_mthend}'
group by POL_NUM
union
select  POL_NUM, max(to_date(TRXN_DT)) lst_ter_date
from    vn_published_cas_db.ttrxn_histories_partial
where   TRXN_CD = 'ORPAGN'
  and   TRXN_DESC like 'Servicing agent changed%'
  and   TRXN_DT <= '{lst_mthend}'
group by POL_NUM
'''

# Execute SQL query
orp3_df = sql_to_df(query, 1, spark)
#orp2_df.filter(F.col("pol_num")=="2880551393").display()

# Union and Aggregation to find the last orphan date
orp_df = orp1_df.union(orp2_df).union(orp3_df).groupBy("pol_num").agg(F.max("trxn_dt").alias("trxn_dt"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Immediate tables

# COMMAND ----------

# Window Specification to retrieve the last orphan date
window_spec = Window.partitionBy("pol_num").orderBy(F.desc("trxn_dt"))
frst_orp_df = orp_df.withColumn("rn", F.row_number().over(window_spec))

# Orphan Data Processing
frst_orp_grouped_df = frst_orp_df.filter(F.col("rn") == 1).select("pol_num", F.to_date(F.col("trxn_dt")).alias("lst_ter_date")) \
                                .join(pol_df, "pol_num", "inner") \
                                .groupBy("po_num") \
                                .agg(F.max("lst_ter_date").alias("lst_ter_date"))

frst_orp_grouped_df = frst_orp_grouped_df.withColumn("ucm_tenure", F.months_between(F.lit(lst_mthend), F.col("lst_ter_date"))) \
                                        .withColumn("ucm_tenure_cat", 
                                                    F.when(F.col("ucm_tenure") < 6, "0-6 months")
                                                    .when(F.col("ucm_tenure") < 12, "6-12 months")
                                                    .when(F.col("ucm_tenure") < 18, "12-18 months")
                                                    .when(F.col("ucm_tenure") < 24, "18-24 months")
                                                    .otherwise("24+ months")
                                                    )
#frst_orp_grouped_df.filter(F.col("po_num")=="2800377053").display()
hp_df = hp_df.withColumn("hp_decile_cat", F.when(F.col("hp_decile").isin(1,2,3), "HP-Top3").otherwise("Others"))

lps_grouped_df = pol_df.join(F.broadcast(lps_df), "pol_num", "inner") \
                .groupBy("po_num") \
                .agg(
                    F.min("lapse_decile").alias("lapse_decile")
                )
lps_grouped_df = lps_grouped_df.withColumn("lapse_decile_cat", F.when(F.col("lapse_decile").isin(1,2,3), "Lapse-Top3").otherwise("Others"))

fully_paid = pol_df.withColumn("fully_paid_ape", 
                               F.when((F.col("pol_stat_cd") == '3') & (F.col("tot_ape") > 0), F.col("tot_ape"))
                                .otherwise(F.col("base_ape") + F.col("rid_ape")))

# Group the dataframe by "po_num" and sum the "fully_paid_ape"
fully_paid_grouped = fully_paid.groupBy("po_num")\
        .agg(   
             F.count("pol_num").alias("no_fully_paid_pols"),
             F.sum("fully_paid_ape").alias("fully_paid_ape")
        )

# Derive fully paid indicator
fully_paid_grouped = fully_paid_grouped.withColumn("fully_paid_ind", F.when(F.col("fully_paid_ape") > 0, 1).otherwise(0))

rider_holding_string = '''
with rider_typ as (
select  pol_num
        , case when pln.prod_typ='TP_RIDER' then 'Y' else 'N' end as tp_rider
        , case when pln.prod_typ='CI_RIDER' then 'Y' else 'N' end as ci_rider
        , case when pln.prod_typ='MC_RIDER' then 'Y' else 'N' end as mc_rider
        , case when pln.prod_typ='ADD_RIDER' then 'Y' else 'N' end as add_rider
        , case when pln.prod_typ='HC_RIDER' then 'Y' else 'N' end as hc_rider
        , case when pln.prod_typ='TPD_RIDER' then 'Y' else 'N' end as tpd_rider
from    vn_published_cas_db.tcoverages cvg inner join
        vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code and cvg.vers_num=pln.vers_num
where   cvg.cvg_typ<>'B'
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
from    vn_published_cas_db.tcoverages cvg left join
        rider_typ on cvg.pol_num=rider_typ.pol_num
where   cvg.cvg_typ='B'
group by cvg.pol_num, cvg.ins_typ
)
select  po_num
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

# Maturity Data Processing
mat_grouped_df = mat_df.groupBy("po_num") \
            .agg(F.min("maturity_date").alias("maturity_date"))

mat_grouped_df = mat_grouped_df.withColumn("maturity_period", F.months_between(F.col("maturity_date"), F.lit(lst_mthend))) \
                                .withColumn("maturity_period_cat",
                                            F.when(F.col("maturity_period") < 3, "0-3 months")
                                            .when(F.col("maturity_period") < 6, "3-6 months")
                                            .otherwise("6+ months"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge everything

# COMMAND ----------

# Master Dataframe
master_df = pre_df.join(F.broadcast(cus_df), "po_num", "left") \
                  .join(F.broadcast(vip_df), "po_num", "left") \
                  .join(frst_orp_grouped_df, "po_num", "left") \
                  .join(F.broadcast(hp_df), "po_num", "left") \
                  .join(lps_grouped_df, "po_num", "left") \
                  .join(F.broadcast(fully_paid_grouped), "po_num", "left") \
                  .join(F.broadcast(mat_grouped_df), "po_num", "left") \
                  .join(rider_holding, "po_num", "left")

master_ucm_df = master_df.filter((F.col("unassigned_ind") == 1) &
                                 (F.col("agt_rltnshp") != "1.Inforce"))

master_ucm_df = master_ucm_df.fillna({"hp_decile_cat": "Others", "lapse_decile_cat": "Others", "vip_typ_desc": "Non-VIP", 
                                      "ucm_tenure_cat": "24+ months", "adj_mthly_incm_cat": "missing", "maturity_period_cat": "more than 12 months"})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analysis

# COMMAND ----------

# Aggregation
master_ucm_df.groupBy("hp_decile_cat").agg(F.count("po_num").alias("number_customers")).show()
master_ucm_df.groupBy("exclusion").agg(F.count("po_num").alias("number_customers")).show()
master_ucm_df.groupBy("adj_mthly_incm_cat").agg(F.count("po_num").alias("number_customers")).show()
master_ucm_df.groupBy("total_ape_cat").agg(F.count("po_num").alias("number_customers")).show()
master_ucm_df.groupBy("vip_typ_desc").agg(F.count("po_num").alias("number_customers")).show()
master_ucm_df.groupBy("ucm_tenure_cat").agg(F.count("po_num").alias("number_customers")).show()
master_ucm_df.groupBy("rider_typ", "prod_type").agg(F.count("po_num").alias("number_customers")).show()
master_ucm_df.groupBy("lapse_decile_cat").agg(F.count("po_num").alias("number_customers")).show()
master_ucm_df.groupBy("maturity_period_cat").agg(F.count("po_num").alias("number_customers")).show()

# COMMAND ----------

master_ucm_pd = master_ucm_df.toPandas()

master_ucm_pd.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/Adhoc/UCM/Agency_UCM_base_analysis_{sht_mthend}.csv", index=False, header=True)
#master_ucm_df.filter(F.col("po_num")=="2800377053").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Investigation and stuffs

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select pol_num, sa_code, POL_STAT_DESC, agt.COMP_PRVD_NUM, agt.AGT_STAT_CODE, agt.TRMN_DT
# MAGIC from   vn_curated_datamart_db.tpolidm_daily pol inner join
# MAGIC         vn_published_ams_db.tams_agents agt on pol.sa_code=agt.agt_code
# MAGIC where  po_num='2800229910'
