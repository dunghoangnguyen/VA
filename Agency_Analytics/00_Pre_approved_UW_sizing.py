# Databricks notebook source
# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, timedelta
import pandas as pd

sys_date = (pd.Timestamp.now() - timedelta(days=1))
run_date = sys_date.strftime('%Y%m%d')
vdo_date = 20241023
#lst_mthend = (sys_date.replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
lst_mthend = '2025-01-31'
mthend_sht = lst_mthend[:7]

cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cseg_ag_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_AGENCY/image_date={lst_mthend}'
cseg_pd_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_PD/image_date={lst_mthend}'
life_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/CUST_LIFESTAGE/monthend_dt={mthend_sht}'

clm_file = 'TCLAIM_DETAILS/'
med_cli_file = 'TMED_CLIENT_DETAILS/'
med_dtl_file = 'TMED_DTL_EXAM/'
sub_file = 'TPT_SUB_DTL/'
hdr_file = 'TNB_LETTER_HDR/'
doc_cli_file = 'TNT_DOC_CLI/'
doc_req_file = 'TNT_DOC_REQ/'
cov_file = 'TCOVERAGES/'
pol_file = 'TPOLICYS/'
fld_file = 'TFIELD_VALUES/'
con_file = 'TCLIENT_CONSOLIDATIONS/'

table_paths = [cas_path]
table_files = [clm_file,med_cli_file,med_dtl_file,sub_file,hdr_file,doc_cli_file,doc_req_file,cov_file,pol_file,fld_file,con_file]
print(f"run_date: {run_date}\nvdo_date: {vdo_date}\nlst_mthend: {lst_mthend}\nmthend_sht: {mthend_sht}")

# COMMAND ----------

# Define schema
schema = StructType([
    StructField("agt_code", StringType(), True),
    StructField("mpro_title", StringType(), True)
])

# Load adhoc agent MPRO list provided by ABM team (if available)
#new_mpro_df = spark.read.csv('/mnt/lab/vn/project/cpm/LEGO/mpro_title.csv', schema=schema, header=True)
new_mpro_df = spark.read.format('parquet').load('/mnt/prod/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS/') \
    .filter((F.col('CHNL_CD') == '01') &
            (F.col('COMP_PRVD_NUM').isin(['01','02','08','96','98'])) &
            (F.col('TRMN_DT').isNull())) \
    .select('agt_code', F.when(F.col('MPRO_TITLE').isin('P','G','S'),
                               F.when(F.coalesce(F.col('MDRT_DESC')).isNotNull(), F.col('MDRT_DESC'))
                               .otherwise(F.col('MPRO_TITLE')))
                        .otherwise(F.lit('Unranked')).alias('mpro_title'))



# Rename and transform the mpro_title column
new_mpro_df = new_mpro_df.withColumn("mpro_title", 
                                     F.when(F.col("mpro_title") == "TOT", F.lit("1.TOT"))
                                     .when(F.col("mpro_title") == "COT", F.lit("2.COT"))
                                     .when(F.col("mpro_title") == "MDRT", F.lit("3.MDRT"))
                                     .when(F.col("mpro_title") == "P", F.lit("4.Platinum"))
                                     .when(F.col("mpro_title") == "G", F.lit("5.Gold"))
                                     .when(F.col("mpro_title") == "S", F.lit("6.Silver"))
                                     .otherwise("Unranked"))

# Rename the column agt_code to agt_cd
new_mpro_df = new_mpro_df.withColumnRenamed("agt_code", "agt_cd")
new_mpro_df.createOrReplaceTempView("mpro")
#print(new_mpro_df.count())
# new_mpro_df.groupBy('mpro_title').agg(F.count(F.col('agt_cd')).alias('agent_count')).display()

tpolidm_df = spark.read.parquet(f'{dm_path}TPOLIDM_DAILY/')  #MTHEND/image_date={lst_mthend}')
tagtdm_df = spark.read.parquet(f'{dm_path}TAGTDM_DAILY/')    #MTHEND/image_date={lst_mthend}')
tcustdm_df = spark.read.parquet(f'{dm_path}TCUSTDM_DAILY/')  #MTHEND/image_date={lst_mthend}')

# Retrieve the latest Agent segmentation data
tpardm_df = spark.read.parquet(f'/mnt/lab/vn/project/cpm/datamarts/TPARDM_MTHEND/')
tpardm_df = tpardm_df.filter(F.col('image_date')==lst_mthend).select('agt_cd','rank_code','tier','last_9m_pol').dropDuplicates()
tpardm_df = tpardm_df.join(F.broadcast(new_mpro_df), on="agt_cd", how="left") \
                     .withColumn("tier", F.coalesce(F.col("mpro_title"),F.lit("Unranked")))
tpardm_df = tpardm_df.drop("mpro_title")

cseg_cols = ['po_num','ins_typ_count','inforce_pol','investment_pol','term_pol','endow_pol','whole_pol',
             'health_indem_pol','total_ape','rider_cnt','rider_ape','client_tenure','adj_mthly_incm',
             'income_decile','inforce_ind','inforce_pol']
cseg_ag_df = spark.read.parquet(cseg_ag_path).select(*cseg_cols)
cseg_pd_df = spark.read.parquet(cseg_pd_path).select(*cseg_cols)
csegdm_df = cseg_ag_df.union(cseg_pd_df).dropDuplicates(['po_num'])
#csegdm_df.filter(F.col('po_num').isin('2801283119','2800643689','2801097699','2800963591')).display()
clife_df = spark.read.parquet(life_path)

df_list = load_parquet_files(table_paths, table_files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add consolidated client numbers

# COMMAND ----------

'''tconso_df = df_list['TCLIENT_CONSOLIDATIONS'].select('cli_num', 'cli_cnsldt_num')
tpolidm_tmp = spark.read.parquet(f'{dm_path}TPOLIDM_DAILY/')

tpolidm_df = tpolidm_tmp.alias("pol").join(
    tconso_df.alias("con"),
    F.col("pol.PO_NUM") == F.col("con.cli_cnsldt_num"),
    "left"
).withColumn(
    "new_PO_NUM",
    F.when(F.col("con.cli_num").isNull(), F.col("pol.PO_NUM")).otherwise(F.col("con.cli_num"))
).drop(F.col("pol.PO_NUM")) \
.withColumnRenamed("new_PO_NUM", "PO_NUM")

# Move "PO_NUM" to the beginning
columns = tpolidm_df.columns
columns.insert(0, columns.pop(columns.index("PO_NUM")))

tpolidm_df = tpolidm_df.select(columns)'''
#tpolidm_df.filter(F.col('po_num').isin('2800856996','2801361234','2802192328','2802356740')).display()

# COMMAND ----------

generate_temp_view(df_list)
tpolidm_df.createOrReplaceTempView("tpolidm")
tagtdm_df.createOrReplaceTempView("tagtdm")
tcustdm_df.createOrReplaceTempView("tcustdm")
csegdm_df.createOrReplaceTempView("csegdm_mthend")
clife_df.createOrReplaceTempView("cust_lifestage")
tpardm_df.createOrReplaceTempView("tpardm_mthend")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PREPARE PRE-APPROVED CONDITIONS AND CUSTOMERS DATA

# COMMAND ----------

# Customers with ILP
ilp_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' ilp_ind
from    tpolicys
where   substr(plan_code_base,1,3) in ('RUV') -- exclude ILP
    and pol_stat_cd in ('1','2','3','5','7')
""")

# Customers with UL
ul_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' ul_ind
from    tpolicys
where   substr(plan_code_base,1,3) in ('UL0') -- exclude UL
    and pol_stat_cd in ('1','2','3','5','7')
""")

# Customers with TROP 1.0
trop1_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' trop1_ind
from    tpolicys
where   substr(plan_code_base,1,3) in ('RPA', 'RPB') -- exclude TROP1
    and pol_stat_cd in ('1','2','3','5','7')
""")
# Customers with TROP 2.0
trop2_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' trop2_ind
from    tpolicys
where   substr(plan_code_base,1,3) in ('RPC', 'RPD') -- exclude TROP2
    and pol_stat_cd in ('1','2','3','5','7')
""")

# Customers with HCR
hcr_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' hcr_ind
from    tcoverages cov inner join
        vn_published_cas_db.tplans pln on cov.plan_code=pln.plan_code
where   1=1
    and cvg_stat_cd in ('1','2','3','5','7')
    and prod_typ='HC_RIDER'
""")

# Customers with MC
mc_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' mc_ind
from    tcoverages cov inner join
        vn_published_cas_db.tplans pln on cov.plan_code=pln.plan_code
where   1=1
    and cvg_stat_cd in ('1','2','3','5','7')
    and prod_typ='MC_RIDER'
""")
#print(mc_his_dtl.count())

# Customers with claim histories (policy level)
clm_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' clm_ind
from    tclaim_details
where   1=1
    --and clm_code in (3, 7, 8, 11, 27, 28, 29)
    and clm_stat_code in ('A','D')
""")

# Customers with medical history
med_his_dtl = spark.sql(f"""
select  distinct 
        cli.pol_num, 'Y' med_his_ind
from    tmed_client_details cli inner join
        tmed_dtl_exam dtl on cli.pol_num=dtl.pol_num and cli.med_seq=dtl.med_seq
where   1=1
""")

# Customers with exclusions/pre-existing conditions
excl_dtl = spark.sql(f"""
select distinct 
        doc.pol_num, 'Y' excl_ind
from    tnt_doc_cli doc inner join 
        tnt_doc_req req on doc.pol_num=req.pol_num and doc.trxn_id=req.trxn_id left join
        tpt_sub_dtl sub on doc.pol_num=sub.pol_num left join
        tnb_letter_hdr hdr on sub.pos_num=hdr.pos_num and sub.letter_id=hdr.letter_id
where   uwg_rmk is not null or
        health_risk='Y' or
        hdr.status in ('A','R')
""")
#excl_dtl.limit(10).display()

# Customers with rating/loading history
rate_his = spark.sql(f"""
select distinct
        pol_num, 'Y' rating_his_ind
from    tcoverages
where   1=1
    and (nvl(FACE_RATG,0) > 100
    or  nvl(PERM_MORT_RATG,0) > 100
    or  nvl(TEMP_MORT_RATG,0) > 100)
""")

# Customers with Occupation class different from 1, 2, 3
occp_cls = spark.sql(f"""
select distinct
        pol_num, 'Y' occp_cls_ind
from    tcoverages
where   1=1
    and occp_clas in ('C','D')
""") #.display()

# Customers with Decline/Rejection history
decl_his = spark.sql("""
select distinct
        pol_num, 'Y' decl_ind
from    tpolicys
where   pol_stat_cd in ('A','X','R','N')
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ad NB Admin Rules AND TAAR fields

# COMMAND ----------

#nb_cols = ["CLI_NUM","POL_NUM","CLM_IND","MEDIC_IND","F856_IND","TAAR_LIFE","TAAR_ADD","TAAR_WV",
#           "TAAR_ME","TAAR_CI","TAAR_MC","TAAR_TPD","TAAR_ME_LIFE","TAAR_ME_CI","TAAR_ME_TPD","TAAR_ME_ADB","TAAR_ME_MED","TAAR_ME_HC","TAAR_LIFE_N"]
#nb_grp_cols = []
#nb_admin_rule_dtl = spark.read.format("csv").load(f"/mnt/lab/vn/project/cpm/LEGO/TAAR/TWRK_RASAL201_TAAR_{vdo_date}.csv", header=True, inferSchema=True) \
#    .select(*[F.col(name.lower()) for name in nb_cols])

#nb_admin_rules = nb_admin_rule_dtl.groupBy("cli_num","pol_num") \
#    .agg(F.max("clm_ind").alias("clm_ind"))

#row_count = nb_admin_rules.count()
#dedup_count = nb_admin_rules.select("POL_NUM").distinct().count()

#print(f"row_count: {row_count} and policy count: {dedup_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add extra step to keep only PO who is also in INSRD on traditional products

# COMMAND ----------

po_insrd = spark.sql("""
with po_pair as (
select  CLI_NUM po_num, pol_num
from    vn_published_cas_db.tclient_policy_links cpl
where   1=1
    and link_typ in ('O')
), dtl as (
select  distinct 
        po_pair.po_num, cvg.cli_num insrd_num, 
        case when po_pair.po_num = cvg.cli_num then
            case when cvg.cvg_typ='B' then 'Bought Base for Self'
                 else 'Bought Rider for Self' end
             else 'Bought nothing for Self'
        end as po_product
        , cvg.pol_num, cvg_typ, plan_code
        , cvg.rel_to_insrd
from    vn_published_cas_db.tcoverages cvg inner join
        vn_published_cas_db.tpolicys pol on cvg.pol_num = pol.pol_num inner join
        po_pair on po_pair.pol_num = pol.POL_NUM
where   pol.pol_stat_cd in ('1','3')
    and substr(pol.PLAN_CODE_BASE,1,3) not in ('FDB','BIC','CA3','CI3','CX3','PN0')
)
select  distinct pol_num, 1 po_not_insrd_ind
from    dtl
where   po_product <> 'Bought nothing for Self'
""")

#non_insrd.filter(F.col("pol_num").isin("2907017650","2906302129","2807663739")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge all rules to identify the PO exclusion list

# COMMAND ----------

pol_cols = ["po_num","pol_num","pol_stat_cd","dist_chnl_cd","dist_chnl_desc","sa_code","po_not_insrd_ind"]

tpolidm_filtered_df = tpolidm_df.join(po_insrd, "pol_num", "left").fillna({'po_not_insrd_ind': 0})

tpolidm_filtered_df.createOrReplaceTempView("tpolidm")
#tpolidm_filtered_df.filter(F.col('po_num').isin('2801283119','2800643689','2801097699','2800963591')).display()
# Map all exclusion rules and filter out the ones that hit
# Apply exclusion rules and filter out ineligible customers
excl_cus = (
    tpolidm_filtered_df.select(*pol_cols)
    .alias("pol")
    .join(ilp_his_dtl.alias("ilp"), on="pol_num", how="left")
    .join(ul_his_dtl.alias("ul"), on="pol_num", how="left")
    .join(trop1_his_dtl.alias("trop1"), on="pol_num", how="left")
    .join(trop2_his_dtl.alias("trop2"), on="pol_num", how="left")
    .join(mc_his_dtl.alias("mc"), on="pol_num", how="left")
    .join(hcr_his_dtl.alias("hcr"), on="pol_num", how="left")
    .join(clm_his_dtl.alias("clm"), on="pol_num", how="left")
    .join(med_his_dtl.alias("med"), on="pol_num", how="left")
    .join(excl_dtl.alias("excl"), on="pol_num", how="left")
    .join(rate_his.alias("rate"), on="pol_num", how="left")
    .join(occp_cls.alias("occp"), on="pol_num", how="left")
    .join(decl_his.alias("decl"), on="pol_num", how="left")
    .withColumn(
        "exclusion_ind",
        F.when(
            (F.col("clm.clm_ind") == "Y") |
            (F.col("med.med_his_ind") == "Y") |
            (F.col("excl.excl_ind") == "Y") |
            (F.col("rate.rating_his_ind") == "Y") |
            (F.col("occp.occp_cls_ind") == "Y") |
            (F.col("decl.decl_ind") == "Y"), "Y"
        ).otherwise("N")
    )
    .groupby("po_num")
    .agg(
        F.max(F.col("ilp_ind")).alias("ilp_ind"),
        F.max(F.col("ul_ind")).alias("ul_ind"),
        F.max(F.col("trop1_ind")).alias("trop1_ind"),
        F.max(F.col("trop2_ind")).alias("trop2_ind"),
        F.max(F.col("mc.mc_ind")).alias("mc_ind"),
        F.max(F.col("hcr.hcr_ind")).alias("hcr_ind"),
        F.max(F.col("clm.clm_ind")).alias("clm_ind"),
        F.max(F.col("med.med_his_ind")).alias("med_his_ind"),
        F.max(F.col("excl.excl_ind")).alias("excl_ind"),
        F.max(F.col("rate.rating_his_ind")).alias("rating_his_ind"),
        F.max(F.col("occp.occp_cls_ind")).alias("occp_cls_ind"),
        F.max(F.col("decl.decl_ind")).alias("decl_ind"),
        F.min(F.col("po_not_insrd_ind")).alias("po_not_insrd_ind"),
        F.max(F.col("exclusion_ind")).alias("exclusion_ind")
    )
    .withColumn(
        "exclusion_ind",
        F.when(F.col("po_not_insrd_ind") == 1, "Y").otherwise(F.col("exclusion_ind"))
    )
)
excl_cus.cache()
#excl_cus = excl_cus.filter(F.col("exclusion_ind") == "Y")

excl_cus.createOrReplaceTempView("excl_cus")
excluded_count = excl_cus.count()
distinct_po_num_count = excl_cus.select('po_num').distinct().count()

if excluded_count == distinct_po_num_count:
    print("No. of excluded cus:", excluded_count, "No. of distinct po_num:", distinct_po_num_count, "==> OK")
else:
    print("No. of excluded cus:", excluded_count, "No. of distinct po_num:", distinct_po_num_count, " ==> RECHECK")
excl_cus.write.mode("overwrite").parquet("/mnt/lab/vn/project/cpm/LEGO/excl_cus/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sizing on Agency customers

# COMMAND ----------

# Derive base policys from Agency
pol_base = spark.sql(f"""
with pol_onb as (
    select  po_num,
            case when c.trmn_dt is null then
                case
                    when c.chnl_cd = '03' and c.comp_prvd_num in ('52','53') then '1.Banca'
                    when c.chnl_cd = '01' and c.comp_prvd_num in ('01','02','08','96','97','98') then '2.Agency'
                    else '3.Closed channels'
                end
            else '4.UCM'
        end as agency_cat
    from    tpolidm a                         left join
            tagtdm  c on a.sa_code=c.agt_code 
    where   a.pol_stat_cd in ('1','3','5')
),
po_cat as (
    select  po_num, min(agency_cat) agency_cat
    from    pol_onb
    group by po_num
),
rider as (
    select  po_num, count(pol.pol_num) rider_cnt, cast(sum(cvg_prem)/23.145 as float) rider_ape
    from    vn_curated_datamart_db.tpolidm_daily pol inner join
            vn_published_cas_db.tcoverages cov on pol.pol_num=cov.pol_num
    where   1=1 
      and   pol_stat_cd in ('1','2','3','5','7')
      and   cvg_typ='R'
    group by po_num
),
inf_pol as (
    select  po_num, count(pol_num) inforce_pol, sum((base_ape_usd+rid_ape_usd)) as total_ape, 1 inforce_ind
    from    tpolidm
    where   pol_stat_cd in ('1','3','5')
    group by po_num
)
select  t.po_num po_num, inforce_ind, inforce_pol, total_ape, nvl(rider_cnt,0) rider_cnt, nvl(rider_ape,0) rider_ape, 
        agt_code, agt_status, nvl(agency_cat,'4. UCM') agency_cat,
        coalesce(ilp_ind,'N') ilp_ind, coalesce(ul_ind,'N') ul_ind,  
        coalesce(trop1_ind,'N') trop1_ind, coalesce(trop2_ind,'N') trop2_ind, 
        coalesce(mc_ind,'N') mc_ind, coalesce(hcr_ind,'N') hcr_ind, 
        coalesce(clm_ind,'N') clm_ind,
        coalesce(med_his_ind,'N') med_his_ind,
        coalesce(excl_ind,'N') excl_ind,
        coalesce(rating_his_ind,'N') rating_his_ind,
        coalesce(occp_cls_ind,'N') occp_cls_ind,
        coalesce(decl_ind,'N') decl_ind,
        coalesce(po_not_insrd_ind,0) po_not_insrd_ind,
        coalesce(exclusion_ind,'N') exclusion_ind
from    (
        select  po_num, sa_code agt_code, agt_stat agt_status,
                row_number() over (partition by po_num order by man.mpro_title, pol_eff_dt desc) as rn
        from    tpolidm pol inner join
                tagtdm  agt on pol.sa_code = agt.agt_code inner join
                mpro    man on agt.agt_code = man.agt_cd
        where   man.mpro_title <> 'Unranked'
        qualify rn = 1
        ) t inner join
        po_cat      b on t.po_num=b.po_num  left join
        excl_cus    c on t.po_num=c.po_num  left join
        inf_pol     e on t.po_num=e.po_num  left join
        rider       d on t.po_num=d.po_num
""").dropDuplicates()
#pol_base.filter(F.col('po_num').isin('2801283119','2800643689','2801097699','2800963591')).display()
#print(pol_base.count(),pol_base.select('po_num').distinct().count())

lst_prod = spark.sql("""
select  po_num, lst_eff_dt, lst_prod_typ
from    (
        select  pol.po_num, pol.pol_num, to_date(pol.pol_eff_dt) lst_eff_dt, fld.fld_valu_desc_eng lst_prod_typ,
                row_number() over (partition by po_num order by pol.pol_eff_dt desc, pol.pol_num) rn
        from    tpolidm pol inner join
                tpolicys cpl on pol.pol_num=cpl.pol_num inner join
                tfield_values fld on cpl.ins_typ_base=fld.fld_valu and fld.fld_nm='INS_TYP'
        where   pol.pol_stat_cd in ('1','2','3','5','7','9','F','H')
        qualify rn = 1
        ) t
""").dropDuplicates()

agt_stat = spark.sql("""
select  distinct
        agt_code,
        CASE WHEN trmn_dt IS NOT NULL THEN '4.Terminated'
             WHEN trmn_dt IS NULL THEN
             CASE
                WHEN comp_prvd_num in ('01','02','08','96')     THEN '1.Inforce'
                WHEN comp_prvd_num = '04'                       THEN '2.Collector'
                WHEN comp_prvd_num IN ('97','98')               THEN '3.SM'
                ELSE '5.Not-Agency'
            END
        END AS agt_rltnshp
from    tagtdm                   
""")

# Group by customer level and latest serving agent
cus_base = pol_base.groupBy(['po_num', 'agt_code', 'agt_status'])\
                    .agg(
                        F.min('agency_cat').alias('agency_cat'),
                        F.max('inforce_ind').alias('inforce_ind'),
                        F.sum('inforce_pol').alias('inforce_pol'),
                        F.sum('total_ape').alias('total_ape'),
                        F.sum('rider_cnt').alias('rider_cnt'),
                        F.sum('rider_ape').alias('rider_ape'),
                        F.max('ilp_ind').alias('ilp_ind'),
                        F.max('ul_ind').alias('ul_ind'),
                        F.max('trop1_ind').alias('trop1_ind'),
                        F.max('trop2_ind').alias('trop2_ind'),
                        F.max('mc_ind').alias('mc_ind'),
                        F.max('hcr_ind').alias('hcr_ind'),
                        F.max('clm_ind').alias('clm_ind'),
                        F.max('med_his_ind').alias('med_his_ind'),
                        F.max('excl_ind').alias('excl_ind'),
                        F.max('rating_his_ind').alias('rating_his_ind'),
                        F.max('occp_cls_ind').alias('occp_cls_ind'),
                        F.max('decl_ind').alias('decl_ind'),
                        F.min('po_not_insrd_ind').alias('po_not_insrd_ind'),
                        F.max('exclusion_ind').alias('exclusion_ind')
                    )
#print(cus_base.count(), cus_base.select('po_num').distinct().count())

# Exclusion group
excl_cus = excl_cus.filter(F.col('exclusion_ind')=="Y")

# Add exclusion / PII / contact and/or demographic information
cus_base_xtra = cus_base \
    .alias("cb") \
    .join(tcustdm_df.alias("tc"), F.col("cb.po_num") == F.col("tc.cli_num"), how="left") \
    .join(lst_prod.alias("lp"), F.col("cb.po_num") == F.col("lp.po_num"), how="left") \
    .join(excl_cus.alias("ec"), F.col("cb.po_num") == F.col("ec.po_num"), how="left") \
    .join(csegdm_df.alias("cs"), F.col("cb.po_num") == F.col("cs.po_num"), how="left") \
    .join(tpardm_df.alias("tp"), F.col("cb.agt_code") == F.col("tp.agt_cd"), how="left") \
    .join(agt_stat.alias("as"), F.col("cb.agt_code") == F.col("as.agt_code"), how="left") \
    .select(
        F.col("cb.po_num").alias("po_num"),
        F.col("cb.agency_cat"),
        F.col("tc.sms_ind"),
        F.col("tc.sms_eservice_ind"),
        F.col("cb.clm_ind"),
        F.col("cb.med_his_ind"),
        F.col("cb.excl_ind"),
        F.col("cb.rating_his_ind"),
        F.col("cb.occp_cls_ind"),
        F.col("cb.decl_ind"),
        F.col("cb.po_not_insrd_ind"),
        F.col("cb.ilp_ind"),
        F.col("cb.ul_ind"),
        F.col("cb.trop1_ind"),
        F.col("cb.trop2_ind"),
        F.col("cb.mc_ind"),
        F.col("cb.hcr_ind"),
        F.col("cb.exclusion_ind").alias("exclusion"),
        F.col("cb.agt_code").alias("agt_code"),
        F.col("cb.agt_status"),
        F.col("as.agt_rltnshp"),
        F.col("tp.rank_code"),
        F.col("tp.tier"),
        F.when(F.col("tp.last_9m_pol") > 0, "Active").otherwise("Inactive").alias("last_pol_cat"),
        F.col("cs.ins_typ_count"),
        F.coalesce(F.col("cb.inforce_pol"),F.col("cs.inforce_pol")).alias("inforce_pol"),
        F.col("cs.investment_pol"),
        F.col("cs.term_pol"),
        F.col("cs.endow_pol"),
        F.col("cs.whole_pol"),
        F.col("cs.health_indem_pol"),
        F.coalesce(F.col("cb.total_ape"),F.col("cs.total_ape")).alias("total_ape"),
        F.coalesce(F.col("cb.rider_cnt"), F.col("cs.rider_cnt")).alias("rider_cnt"),
        F.coalesce(F.col("cb.rider_ape"), F.col("cs.rider_ape")).alias("rider_ape"),
        F.col("cs.client_tenure"),
        F.col("cs.adj_mthly_incm"),
        F.col("cs.income_decile"),
        F.col("lp.lst_eff_dt"),
        F.months_between(F.lit(lst_mthend), F.col("lp.lst_eff_dt")).alias("month_since_lst_eff_dt"),
        F.col("lp.lst_prod_typ"),
        F.coalesce(F.col("cb.inforce_ind"), F.col("cs.inforce_ind")).alias("inforce_ind"),
        (F.datediff(F.lit(lst_mthend), F.col("tc.birth_dt")) / 365.25).cast("int").alias("age")
    ) \
    .withColumn("ins_typ_count", F.when(F.col("inforce_pol") <= 1, F.lit(1)).otherwise(F.col("cs.ins_typ_count"))) \
    .withColumn("rider_typ", F.when(F.col("rider_cnt") > 0, F.lit("2. Customer with Rider")).otherwise("1. Customer without Rider")) \
    .withColumn("prod_type",
        F.when(
            (F.col("ins_typ_count") == 1) &
            (
                (F.col("cs.investment_pol") > 0) |
                (F.col("cs.term_pol") > 0) |
                (F.col("cs.endow_pol") > 0) |
                (F.col("cs.whole_pol") > 0) |
                (F.col("cs.health_indem_pol") > 0)
            ),
            F.when(F.col("cs.investment_pol") > 0, F.lit("1 Investment Only"))
            .when(F.col("cs.term_pol") > 0, F.lit("2 Term Only"))
            .when(F.col("cs.endow_pol") > 0, F.lit("3 Endowment Only"))
            .when(F.col("cs.whole_pol") > 0, F.lit("4 Whole Life Only"))
            .when(F.col("cs.health_indem_pol") > 0, F.lit("5 Health Only"))
        )
        .otherwise("6 Multiple")
    )

#total_count = cus_base_xtra.count()
#distinct_po_count = cus_base_xtra.select('po_num').distinct().count()

#if total_count == distinct_po_count:
#    print("No. of total cus:", total_count, "=> OK")
#else:
#    print("No. of total cus:", total_count, "No. of distinct cus:", distinct_po_count, "=> DIFFERENCE")

#cus_base_xtra.filter(F.col('po_num').isin('2801283119','2800643689','2801097699','2800963591')).display()

# COMMAND ----------

# Double check the numbers before proceed
# filter(F.col('inforce_ind')==1)
#cus_sum = cus_base_xtra.filter(F.col('inforce_ind')==1) \
#    .groupBy(['exclusion', #'agency_cat' ,'trop2_ind','rider_typ']) \
#    .agg(
#        F.count('po_num').alias('no_of_rows'),
#        F.countDistinct('po_num').alias('no_of_pos'),
#        F.countDistinct('agt_code').alias('no_of_agts'),
#        F.sum('inforce_pol').alias('no_of_pols'),
#        F.sum('total_ape').alias('total_ape')
#    ).orderBy('exclusion') #,'agency_cat'])'''

#cus_sum.display()

# COMMAND ----------

cus_base_xtra.filter(F.col('inforce_ind')==1).write.mode("overwrite").parquet(f"/mnt/lab/vn/project/cpm/LEGO/preapproved_uw_temp/image_date={lst_mthend}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorize all metrics

# COMMAND ----------

cus_pd = pd.read_parquet(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/preapproved_uw_temp/image_date={lst_mthend}")
#cus_base_xtra.filter(F.col('inforce_ind')==1).toPandas()
print("No. of total inforce customers: ", cus_pd.shape[0])
print("No. of customers eligible for Pre-approved: ", cus_pd[cus_pd['exclusion'] == 'N'].shape[0])
#cus_pd[cus_pd["trop2_ind"]=="N"].head(2)

# COMMAND ----------

# Define bins and labels for each column to be binned
bins_labels = [
    # Add new features
    ('age', [0, 24, 30, 35, 40, 45, 50, 55, float('inf')],
     ['<25', '25-30', '31-35', '36-40', '41-45', '46-50', '51-55', '55+']),
    ('inforce_pol', [0, 0.1, 1, 2, float('inf')],
     ['0', '1', '2', '3+']),
    ('total_ape', [0, 1000, 2000, 3000, 5000, 7000, 10000, float('inf')], 
     ['1. <=1k', '2. 1-2k', '3. 2-3k', '4. 3-5k', '5. 5-7k', '6. 7-10k', '7. >10k']),
    ('rider_cnt', [-float('inf'), 0, 1, 2, float('inf')],
    ['0', '1', '2', '3+']),
    ('rider_ape', [-float('inf'), 0, 1000, 2000, 3000, 5000, float('inf')], 
     ['0', '1. <=1k', '2. 1-2k', '3. 2-3k', '4. 3-5k', '5. 5k+']),
    ('adj_mthly_incm', [0, 500, 1000, 1500, 2000, 3000, 5000, float('inf')],
     ['1. <500', '2. 500-1k', '3. 1-1.5k', '4. 1.5-2k', '5. 2-3k', '6. 3-5k', '7. >5k']),    
    ('client_tenure', [0, 1, 2, 3, 4, float('inf')],
     ['<1', '1-2', '2-3', '3-4', '4+']),
]

# Apply the function to each feature
for column, bins, labels in bins_labels:
    create_categorical(cus_pd, column, bins, labels)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exclusion funnel

# COMMAND ----------

# Initialize an empty list to store the results
exclusion_data = []

# Calculate the count and percentage of each exclusion flag
exclusion_flags = ["clm_ind", "med_his_ind", "excl_ind", "rating_his_ind", "occp_cls_ind", "decl_ind", "po_not_insrd_ind"]

total_customers = cus_pd["po_num"].nunique()

for flag in exclusion_flags:
    flag_count = cus_pd[cus_pd[flag] == "Y"]["po_num"].nunique()
    flag_ratio = flag_count / total_customers * 100
    exclusion_data.append({"exclusion": flag, "count": flag_count, "ratio": flag_ratio})

# Create the Pandas dataframe from the list of dictionaries using pd.concat()
exclusion_pd = pd.concat([pd.DataFrame([data]) for data in exclusion_data], ignore_index=True)

# Display the resulting Pandas dataframe
print(exclusion_pd)

# COMMAND ----------

# 1. Calculate the number of unique po_num and agt_code per tier from the eligible pool
summary = cus_pd[(cus_pd["exclusion"] == "N")
                 & (cus_pd["tier"] != "7.Unranked") #& (cus_pd["ilp_taar"] <= 500000000)
                 ].groupby('tier').agg(
    unique_po_num=('po_num', 'nunique'),
    unique_agt_code=('agt_code', 'nunique')
).reset_index()

# 2. Calculate the percentage distribution of po_num and agt_code per tier
total_po_num = cus_pd['po_num'].nunique()
total_agt_code = cus_pd['agt_code'].nunique()

summary['po_num_percentage'] = (summary['unique_po_num'] / total_po_num) * 100
summary['agt_code_percentage'] = (summary['unique_agt_code'] / total_agt_code) * 100

# 3. Identify agt_code that are linked to more than 10 po_num
agent_po_counts = cus_pd.groupby('agt_code')['po_num'].nunique().reset_index()
agents_more_than_10_po = agent_po_counts[agent_po_counts['po_num'] > 10]['agt_code']

# 4. Count these agents per tier
agents_with_more_than_10_po = cus_pd[cus_pd['agt_code'].isin(agents_more_than_10_po)]
agents_with_more_than_10_po_per_tier = agents_with_more_than_10_po.groupby('tier')['agt_code'].nunique().reset_index(name='agt_code_more_than_10_po')

# Combine all the calculated values into a summary dataframe
summary = summary.merge(agents_with_more_than_10_po_per_tier, on='tier', how='left')
summary['agt_code_more_than_10_po'] = summary['agt_code_more_than_10_po'].fillna(0).astype(int)

display(summary)

# COMMAND ----------

cus_sum = cus_pd.groupby(["agency_cat"]).agg(unique_po_num=('po_num', 'count')).reset_index()

display(cus_sum)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store as partitioned data

# COMMAND ----------

cus_pd.to_parquet(f'/dbfs/mnt/lab/vn/project/cpm/LEGO/preapproved_uw_{run_date}.parquet', index=False)
cus_pd.head(2)

# COMMAND ----------

cus_pd.to_csv(f'/dbfs/mnt/lab/vn/project/cpm/LEGO/preapproved_uw_{run_date}.csv', index=False, header=True, encoding='utf-8-sig')
