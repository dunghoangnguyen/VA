# Databricks notebook source
# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime, timedelta
import pandas as pd

run_date = pd.Timestamp.now().strftime('%Y%m%d')
lst_mthend = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
mthend_sht=lst_mthend[:4]+lst_mthend[5:7]

cas_path =  '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
dm_path =   '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cseg_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_PD/image_date={lst_mthend}'

clm_file =      'TCLAIM_DETAILS/'
med_cli_file = 'TMED_CLIENT_DETAILS/'
med_dtl_file = 'TMED_DTL_EXAM/'
sub_file =      'TPT_SUB_DTL/'
hdr_file =      'TNB_LETTER_HDR/'
doc_cli_file = 'TNT_DOC_CLI/'
doc_req_file = 'TNT_DOC_REQ/'
cov_file =      'TCOVERAGES/'
pol_file =      'TPOLICYS/'
fld_file =      'TFIELD_VALUES/'
con_file =      'TCLIENT_CONSOLIDATIONS/'
vip_file =      'TWRK_CLIENT_APE/'

table_paths = [cas_path]
table_files = [clm_file,med_cli_file,med_dtl_file,sub_file,hdr_file,doc_cli_file,doc_req_file,cov_file,pol_file,fld_file,con_file,vip_file]
print(run_date, lst_mthend, mthend_sht)

# COMMAND ----------

tagtdm_df = spark.read.parquet(f'{dm_path}TAGTDM_MTHEND/image_date={lst_mthend}')
tcustdm_df = spark.read.parquet(f'{dm_path}TCUSTDM_MTHEND/image_date={lst_mthend}')
csegdm_df = spark.read.parquet(cseg_path)

df_list = load_parquet_files(table_paths, table_files)
tconso_df = df_list['TCLIENT_CONSOLIDATIONS'].select('cli_num', 'cli_cnsldt_num')
tpolidm_tmp = spark.read.parquet(f'{dm_path}TPOLIDM_MTHEND/image_date={lst_mthend}')

tpolidm_df = tpolidm_tmp.alias("pol").join(
    tconso_df.alias("con"),
    F.col("pol.PO_NUM") == F.col("con.cli_cnsldt_num"),
    "left"
).withColumn(
    "conso_PO_NUM",
    F.when(F.col("con.cli_num").isNull(), F.col("pol.PO_NUM")).otherwise(F.col("con.cli_num")) #.drop(F.col("pol.PO_NUM")).withColumnRenamed("new_PO_NUM", "PO_NUM") \
) \
.join(df_list["TPOLICYS"].select("POL_NUM", "OWN_INS_REL"), "POL_NUM", "inner")

# Move "PO_NUM" to the beginning
columns = tpolidm_df.columns
columns.insert(0, columns.pop(columns.index("PO_NUM")))

tpolidm_df = tpolidm_df.select(columns)

# Lower case column headers
tpolidm_df = tpolidm_df.toDF(*[col.lower() for col in tpolidm_df.columns])

#tpolidm_df.filter(F.col('PO_NUM').isin('2805687402', '2807427062')).display()

# COMMAND ----------

generate_temp_view(df_list)
#tpolidm_df.createOrReplaceTempView("tpolidm")
tagtdm_df.createOrReplaceTempView("tagtdm")
tcustdm_df.createOrReplaceTempView("tcustdm")
csegdm_df.createOrReplaceTempView("csegdm_mthend")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PREPARE PRE-APPROVED INDICATORS AND VTB CUSTOMERS DATA

# COMMAND ----------

# Customers with ILP
ilp_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' ilp_ind
from    tpolicys
where   substr(plan_code_base,1,3) in ('RUV') -- exclude ILP
    and pol_stat_cd in ('1','2','3','5','7')
""")

# Customers with UL19
ul19_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' ul19_ind
from    tcoverages
where   plan_code in ('UL038', 'UL039', 'UL040', 'UL041') -- exclude UL19
    and cvg_stat_cd not in ('A','N','R','X','L')
""")

# Customers with HCR
hcr_his_dtl = spark.sql(f"""
select  distinct pol.pol_num
        , 'Y' hcr_ind
from    tpolicys pol inner join 
        tcoverages cov on pol.pol_num=cov.pol_num inner join
        vn_published_cas_db.tplans pln on cov.plan_code=pln.plan_code
where   1=1
    and pol_stat_cd in ('1','2','3','5','7')
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

# Customers with health claim histories (policy level)
clm_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' clm_ind
from    tclaim_details
where   1=1
""")

# Customers with medical history
med_his_dtl = spark.sql(f"""
select  distinct 
        cli.pol_num, 'Y' med_his_ind
from    tmed_client_details cli inner join
        tmed_dtl_exam dtl on cli.pol_num=dtl.pol_num and cli.med_seq=dtl.med_seq
where   1=1
""")

#med_his_dtl.limit(10).display()

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
""")

# Customers with Decline/Rejection history
decl_his = spark.sql("""
select distinct
        pol_num, 'Y' decl_ind
from    tpolicys
where   pol_stat_cd in ('X','R','N','A')                  
""")

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
    and substr(pol.PLAN_CODE_BASE,1,3) not in ('FDB','BIC','CA3','CX3','PN0')
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

# Map all exclusion rules and filter out the ones that hit
excl_cus = tpolidm_filtered_df.select(pol_cols).alias("pol") \
                    .join(ilp_his_dtl.alias("ilp"), on="pol_num", how="left") \
                    .join(ul19_his_dtl.alias("ul19"), on="pol_num", how="left") \
                    .join(hcr_his_dtl.alias("hcr"), on="pol_num", how="left") \
                    .join(mc_his_dtl.alias("mc"), on="pol_num", how="left") \
                    .join(clm_his_dtl.alias("clm"), on="pol_num", how="left") \
                    .join(med_his_dtl.alias("med"), on="pol_num", how="left") \
                    .join(excl_dtl.alias("excl"), on="pol_num", how="left") \
                    .join(rate_his.alias("rate"), on ="pol_num", how="left") \
                    .join(occp_cls.alias("occp"), on="pol_num", how="left") \
                    .join(decl_his.alias("decl"), on="pol_num", how="left") \
                    .withColumn("exclusion_ind",
                         F.when((F.col("clm.clm_ind") == "Y") |
                                (F.col("med.med_his_ind") == "Y") | 
                                (F.col("excl.excl_ind") == "Y") | 
                                (F.col("rate.rating_his_ind") == "Y") |
                                (F.col("occp.occp_cls_ind") == "Y") |
                                (F.col("decl.decl_ind") == "Y"), "Y")
                         .otherwise("N")) \
                    .groupby("po_num").agg(
                        F.max(F.col("ilp.ilp_ind")).alias("ilp_ind"),
                        F.max(F.col("ul19.ul19_ind")).alias("ul19_ind"),
                        F.max(F.col("hcr.hcr_ind")).alias("hcr_ind"),
                        F.max(F.col("mc.mc_ind")).alias("mc_ind"),
                        F.max(F.col("clm.clm_ind")).alias("clm_ind"),
                        F.max(F.col("med.med_his_ind")).alias("med_his_ind"),                        
                        F.max(F.col("excl.excl_ind")).alias("excl_ind"),
                        F.max(F.col("rate.rating_his_ind")).alias("rating_his_ind"),
                        F.max(F.col("occp.occp_cls_ind")).alias("occp_cls_ind"),
                        F.max(F.col("decl.decl_ind")).alias("decl_ind"),
                        F.min(F.col("po_not_insrd_ind")).alias("po_not_insrd_ind"),
                        F.max(F.col("exclusion_ind")).alias("exclusion_ind")) \
                    .withColumn(
                        "exclusion_ind",
                        F.when(F.col("po_not_insrd_ind") == 1, "Y").otherwise(F.col("exclusion_ind"))
                    )
excl_cus.cache()

excl_cus.createOrReplaceTempView("excl_cus")
print("No. of excluded cus:", excl_cus.count())
#print("No. of HCR cus:", excl_cus.filter(F.col("hcr_ind")=="Y").count())
#excl_pols.limit(10).display()
excl_cus.filter(F.col("exclusion_ind")=="Y").write.mode("overwrite").parquet("/mnt/lab/vn/project/cpm/VTB/Pre_approved/excl_cus/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sizing on VTB customers

# COMMAND ----------

# Derive base policys from TCB (or any Banca locations)
pol_base = spark.sql(f"""
with pol_onb as (
    select  po_num,
            case when
            a.dist_chnl_cd in ('52','53') then '1.Onboarded and served by VTB'
                                          else '2.Served by VTB'
            end as vtb_cat
    from    tpolidm a                         left join
            tagtdm  b on a.sa_code=b.agt_code
    where   1=1
        and pol_stat_cd in ('1','3','5','7')
        and (b.loc_cd like 'VTI%' or
             b.comp_prvd_num in ('52', '53'))
),
po_cat as (
    select  po_num, min(vtb_cat) vtb_cat
    from    pol_onb
    group by po_num
),
rider as (
    select  po_num, count(pol.pol_num) rider_cnt, cast(sum(cvg_prem)/23.145 as float) rider_ape
    from    vn_curated_datamart_db.tpolidm_daily pol inner join
            vn_published_cas_db.tcoverages cov on pol.pol_num=cov.pol_num
    where   1=1 
      and   cvg_stat_cd in ('1','3','5','7')
      and   cvg_typ='R'
    group by po_num
),
inf_pol as (
    select  po_num, count(pol_num) inforce_pol, sum((base_ape_usd+rid_ape_usd)) as total_ape, 1 inforce_ind
    from    tpolidm
    where   pol_stat_cd in ('1','3','5','7')
    group by po_num
),
vip_cus as (
    select  distinct cli_num po_num, case when vip_typ_desc = 'SUPER VIP' then 'VIP Elite' else vip_typ_desc end as vip_status
    from    twrk_client_ape
)
select  t.po_num po_num, conso_po_num, inforce_ind, inforce_pol, total_ape, nvl(rider_cnt,0) rider_cnt, nvl(rider_ape,0) rider_ape, 
        agt_code, agt_status, vtb_cat, 
        coalesce(ilp_ind,'N') ilp_ind, coalesce(ul19_ind,'N') ul19_ind,
        coalesce(mc_ind,'N') mc_ind, coalesce(hcr_ind,'N') hcr_ind, 
        coalesce(clm_ind,'N') clm_ind,
        coalesce(med_his_ind,'N') med_his_ind,
        coalesce(excl_ind,'N') excl_ind,
        coalesce(rating_his_ind,'N') rating_his_ind,
        coalesce(occp_cls_ind,'N') occp_cls_ind,
        coalesce(decl_ind,'N') decl_ind,
        coalesce(po_not_insrd_ind,0) po_not_insrd_ind,
        coalesce(exclusion_ind,'N') exclusion_ind,
        coalesce(f.vip_status,'Non VIP') vip_status
from    (
        select  po_num, nvl(conso_po_num,po_num) conso_po_num, sa_code agt_code, agt_stat agt_status,
                row_number() over (partition by po_num order by pol_eff_dt desc) as rn
        from    tpolidm pol inner join
                tagtdm  agt on pol.sa_code = agt.agt_code
        where   1=1
        ) t inner join
        po_cat      b on t.po_num=b.po_num  left join
        excl_cus    c on t.po_num=c.po_num  left join
        rider       d on t.po_num=d.po_num  left join
        inf_pol     e on t.po_num=e.po_num  left join
        vip_cus     f on t.po_num=f.po_num
where rn = 1
""").dropDuplicates()

lst_prod = spark.sql("""
select  po_num, lst_eff_dt, lst_prod_typ
from    (
        select  pol.po_num as po_num, pol.pol_num, to_date(pol.pol_eff_dt) lst_eff_dt, fld.fld_valu_desc_eng lst_prod_typ,
                row_number() over (partition by po_num order by pol.pol_eff_dt desc, pol.pol_num) rn
        from    tpolidm pol inner join
                tpolicys cpl on pol.pol_num=cpl.pol_num inner join
                tfield_values fld on cpl.ins_typ_base=fld.fld_valu and fld.fld_nm='INS_TYP'
        where   pol.pol_stat_cd not in ('8','A','C','L','N','R','X')
        ) t
where   rn=1
""")

# Group by customer level and latest serving agent
cus_base =  pol_base.groupBy(['po_num', 'conso_po_num', 'vip_status', 'agt_code', 'agt_status'])\
                    .agg(
                        F.max('inforce_ind').alias('inforce_ind'),
                        F.sum('inforce_pol').alias('inforce_pol'),
                        F.sum('total_ape').alias('total_ape'),
                        F.sum('rider_cnt').alias('rider_cnt'),
                        F.sum('rider_ape').alias('rider_ape'),
                        F.max('ilp_ind').alias('ilp_ind'),
                        F.min('vtb_cat').alias('vtb_cat'),
                        F.max('ul19_ind').alias('ul19_ind'),
                        F.max('hcr_ind').alias('hcr_ind'),
                        F.max('mc_ind').alias('mc_ind'),
                        F.max('clm_ind').alias('clm_ind'),
                        F.max('med_his_ind').alias('med_his_ind'),
                        F.max('excl_ind').alias('excl_ind'),
                        F.max('rating_his_ind').alias('rating_his_ind'),
                        F.max('occp_cls_ind').alias('occp_cls_ind'),
                        F.max('decl_ind').alias('decl_ind'),
                        F.min('po_not_insrd_ind').alias('po_not_insrd_ind'),
                        F.max('exclusion_ind').alias('exclusion_ind')
                    )

# Exclusion customers group
excl_cus = excl_cus.filter(F.col('exclusion_ind')=="Y")

# Add exclusion / PII / contact and/or demographic information
cus_base_xtra = cus_base \
                    .join(excl_cus, cus_base["po_num"]==excl_cus["po_num"], how="left"
                    ) \
                    .join(tcustdm_df, cus_base["po_num"]==tcustdm_df["cli_num"], how="inner"     
                    ) \
                    .join(lst_prod, cus_base["po_num"]==lst_prod["po_num"], how="inner"     
                    ) \
                    .join(csegdm_df, cus_base["po_num"]==csegdm_df["po_num"], how="left") \
                    .select(
                        cus_base["po_num"],
                        "conso_po_num",
                        "cli_nm",
                        "vtb_cat",
                        "mobl_phon_num",
                        "email_addr",
                        "sms_ind",
                        "sms_eservice_ind",
                        cus_base["ilp_ind"],
                        cus_base["ul19_ind"],
                        cus_base["hcr_ind"],
                        cus_base["mc_ind"],
                        cus_base["clm_ind"],
                        cus_base["med_his_ind"],
                        cus_base["excl_ind"],
                        cus_base["rating_his_ind"],
                        cus_base["occp_cls_ind"],
                        cus_base["decl_ind"],
                        cus_base["po_not_insrd_ind"],
                        cus_base["exclusion_ind"].alias("exclusion"),
                        "vip_status",
                        "agt_code",
                        "agt_status",
                        "ins_typ_count",
                        F.coalesce(cus_base["inforce_pol"],csegdm_df["inforce_pol"]).alias("inforce_pol"),
                        "investment_pol",
                        "term_pol",
                        "endow_pol",
                        "whole_pol",
                        "health_indem_pol",
                        F.coalesce(cus_base["total_ape"],csegdm_df["total_ape"]).alias("total_ape"),
                        F.coalesce(cus_base["rider_cnt"],csegdm_df["rider_cnt"]).alias("rider_cnt"),
                        F.coalesce(cus_base["rider_ape"],csegdm_df["rider_ape"]).alias("rider_ape"),
                        "client_tenure",
                        "adj_mthly_incm",
                        "income_decile",
                        lst_prod["lst_eff_dt"],
                        F.months_between(F.lit(lst_mthend), lst_prod["lst_eff_dt"]).alias("month_since_lst_eff_dt"),
                        "lst_prod_typ",
                        F.coalesce(cus_base["inforce_ind"],csegdm_df["inforce_ind"]).alias("inforce_ind"),
                        (F.datediff(F.lit(lst_mthend), F.col("birth_dt"))/365.25).cast("int").alias("age")) \
                        .withColumn("ins_typ_count", F.when(F.col("inforce_pol")<=1, F.lit(1)).otherwise(F.col("ins_typ_count"))) \
                        .withColumn("rider_typ", F.when((F.col("rider_ape")>0),
                                F.lit("2. Customer with Rider")).otherwise("1. Customer without Rider")) \
                        .withColumn("prod_type", 
                                F.when(
                                    (F.col("rider_typ") == "1. Customer without Rider") &
                                    (F.col("ins_typ_count") == 1) &
                                    (
                                        (F.col("investment_pol") > 0) |
                                        (F.col("term_pol") > 0) |
                                        (F.col("endow_pol") > 0) |
                                        (F.col("whole_pol") > 0) |
                                        (F.col("health_indem_pol") > 0)
                                    ),
                                    F.when(F.col("investment_pol") > 0, F.lit("1.1 Investment Only"))
                                    .when(F.col("term_pol") > 0, F.lit("1.2 Term Only"))
                                    .when(F.col("endow_pol") > 0, F.lit("1.3 Endowment Only"))
                                    .when(F.col("whole_pol") > 0, F.lit("1.4 Whole Life Only"))
                                    .when(F.col("health_indem_pol") > 0, F.lit("1.5 Health Only"))
                                )
                                .when(
                                    (F.col("rider_typ") == "2. Customer with Rider") &
                                    (F.col("ins_typ_count") == 1) &
                                    (
                                        (F.col("investment_pol") > 0) |
                                        (F.col("term_pol") > 0) |
                                        (F.col("endow_pol") > 0) |
                                        (F.col("whole_pol") > 0) |
                                        (F.col("health_indem_pol") > 0)
                                    ),
                                    F.when(F.col("investment_pol") > 0, F.lit("2.1 Investment Only"))
                                    .when(F.col("term_pol") > 0, F.lit("2.2 Term Only"))
                                    .when(F.col("endow_pol") > 0, F.lit("2.3 Endowment Only"))
                                    .when(F.col("whole_pol") > 0, F.lit("2.4 Whole Life Only"))
                                    .when(F.col("health_indem_pol") > 0, F.lit("2.5 Health Only"))
                                )
                                .otherwise("3 Others")
                                )
                    #.withColumn("exclusion", F.coalesce(F.col("exclusion"), F.lit("N")))

# Auto-exclude those without HCR
#cus_base_xtra = cus_base_xtra.withColumn("exclusion_hcr", 
#    F.when((F.col("hcr_ind") == "Y"), "Y")
#    .otherwise(F.col("exclusion")))

# Double check the numbers before proceed
vtb_sum = cus_base_xtra.filter(F.col('inforce_ind')==1) \
    .groupBy(['exclusion','vtb_cat','rider_typ'
              ]) \
    .agg(
        F.count('po_num').alias('row_count'),
        F.countDistinct('po_num').alias('po_count'),
        F.countDistinct('agt_code').alias('agt_count'),
        F.sum('inforce_pol').alias('pol_count'),
        F.sum('total_ape').cast("int").alias('total_ape')
    ).orderBy(['exclusion'])

vtb_sum.display()
#tcb_cus_base_xtra.filter(F.col('po_num').isin('2805687402','2807427062')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load top 2 deciles from Banca model

# COMMAND ----------

vtb_leads = spark.read.parquet(f"/mnt/lab/vn/project/scratch/banca_cpm_2024/vtb/prod_4_alldec_{mthend_sht}") #prod_4_alldec_{mthend_sht}
vtb_leads = vtb_leads.withColumn("decile_cat", 
                                F.when(F.col("decile") == 1, "Top1-HP")
                                 .when(F.col("decile") == 2, "Top2-HP")
                                 .when(F.col("decile") == 3, "Top3-HP")
                                 .otherwise("Bottom"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load additional indicators (fully-paid, protection gap and riders...)

# COMMAND ----------

merged_cols = ["po_num", "clm_6m_amt", "clm_6m_cnt_cat", "clm_6m_ratio_cat"]
merged_activation = spark.read.format("parquet").load("/mnt/lab/vn/project/scratch/agent_activation/merged_target_activation.parquet").select(*merged_cols)

mat_cols = [
"po_num", "maturing_policy", "maturing_product_type", "maturing_product", "policy_status", "maturity_month",
"maturity_date", "maturity_value", "maturity_ape", "maturity_serving_agent", "loc_code", "agt_status"]
maturity_list = spark.read.format("parquet").load(f"/mnt/lab/vn/project/cpm/Adhoc/Maturity/maturity_policies_dtl_w_excl_{mthend_sht}.parquet")\
                .select(*mat_cols)\
                .withColumnRenamed("agt_status", "mat_agt_status")\
                .orderBy("po_num", "maturity_date", F.desc("maturing_policy"))\
                .dropDuplicates(["po_num"])

fully_paid = spark.read.format("parquet").load("/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/")
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

m25_cols = ["po_num", "cus_gender", "birth_dt", "bday_ind", "bday_month"]
m25_list = spark.read.format("parquet").load(f"/mnt/lab/vn/project/cpm/M25/image_date={lst_mthend}/")\
              .select(*m25_cols)

income_based_decile = spark.read.format("parquet").load(f"/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_PD/image_date={lst_mthend}/")
protection_gap = income_based_decile.filter((F.col('adj_mthly_incm')>0))\
                        .withColumn('protection_income%', F.col('f_with_dependent')*100*(F.col('protection_fa_all_imp') / (120*F.least(F.col('adj_mthly_incm'),F.lit(20000)))))\
                        .withColumn('protection_sa', F.when(F.col('protection_fa_all_imp') < (30*F.least(F.col('total_ape'),F.lit(20000))), 1).otherwise(0))\
                        .withColumn('protection_income_grp', F.when(F.col('f_with_dependent') != 1 , '5. No Dependent')\
                            .otherwise(F.when(F.col('protection_income%')>75, '1. 75% Income Protection')\
                            .otherwise(F.when(F.col('protection_income%')>50, '2. 50% Income Protection')\
                                .otherwise(F.when(F.col('protection_income%')>20, '3. 20% Income Protection')\
                                    .otherwise(F.when(F.col('protection_income%')>0, '4. below 20% Income Protection')\
                                    .otherwise('5. No dependent/Protection Pols'))))))\
                        .select('po_num',F.col('protection_fa_all_imp').alias('protection_fa'),'protection_income%','protection_income_grp','protection_sa')


vtb_taar = spark.read.format("csv").option("header", "true").load("/mnt/lab/vn/project/cpm/VTB/Pre_approved/TAAR/vtb_taar.csv", inferSchema=True)\
                .select(F.col("cli_num").cast("string").alias("po_num"), 
                        "ilp_offr",
                        "ul_offr",
                        "trop_offr",
                        "eligibility"
                        )\
                .withColumn("ilp_offr_cat", 
                        F.when((F.col("ilp_offr").isNull()) | (F.col("ilp_offr") <= 0), "0. No SA")
                         .when((F.col("ilp_offr")/1000000 <= 500), "1. <=500M")
                         .when((F.col("ilp_offr")/1000000 <= 1000), "2. 500M-1B")
                         .when((F.col("ilp_offr")/1000000 <= 2000), "3. 1-2B")
                         .when((F.col("ilp_offr")/1000000 <= 3000), "4. 2-3B")
                         .when((F.col("ilp_offr")/1000000 <= 5000), "5. 3-5B")
                         .when((F.col("ilp_offr")/1000000 <= 7000), "6. 5-7B")
                         .when((F.col("ilp_offr")/1000000 <= 10000), "7. 7-10B")
                         .when((F.col("ilp_offr")/1000000  > 10000), "8. >10B")
                         .otherwise("9. Exceeded")) \
                .withColumn("ul_offr_cat", 
                        F.when((F.col("ul_offr").isNull()) | (F.col("ul_offr") <= 0), "0. No SA")
                         .when((F.col("ul_offr")/1000000 <= 500), "1. <=500M")
                         .when((F.col("ul_offr")/1000000 <= 1000), "2. 500M-1B")
                         .when((F.col("ul_offr")/1000000 <= 2000), "3. 1-2B")
                         .when((F.col("ul_offr")/1000000 <= 3000), "4. 2-3B")
                         .when((F.col("ul_offr")/1000000 <= 5000), "5. 3-5B")
                         .when((F.col("ul_offr")/1000000 <= 7000), "6. 5-7B")
                         .when((F.col("ul_offr")/1000000 <= 10000), "7. 7-10B")
                         .when((F.col("ul_offr")/1000000  > 10000), "8. >10B")
                         .otherwise("9. Exceeded")) \
                .withColumn("trop_offr_cat", 
                        F.when((F.col("trop_offr").isNull()) | (F.col("trop_offr") <= 0), "0. No SA")
                         .when((F.col("trop_offr")/1000000 <= 500), "1. <=500M")
                         .when((F.col("trop_offr")/1000000 <= 1000), "2. 500M-1B")
                         .when((F.col("trop_offr")/1000000 <= 2000), "3. 1-2B")
                         .when((F.col("trop_offr")/1000000 <= 3000), "4. 2-3B")
                         .when((F.col("trop_offr")/1000000 <= 5000), "5. 3-5B")
                         .when((F.col("trop_offr")/1000000 <= 7000), "6. 5-7B")
                         .when((F.col("trop_offr")/1000000 <= 10000), "7. 7-10B")
                         .when((F.col("trop_offr")/1000000  > 10000), "8. >10B")
                         .otherwise("9. Exceeded"))


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
    and pol.PLAN_CODE not like 'EV%'                    -- Exclude exchange rate conversion products
    and pol.pol_stat_cd in ('1','2','3','5','7','9')    -- Only premium paying or holiday
group by
        pol.po_num
'''
rider_holding = sql_to_df(rider_holding_string, 1 , spark)

po_base_for_string = '''
with pol_base_for as (
  select distinct cpl.cli_num po_num, case when fld.fld_valu_desc_eng in ('Owner','payor') then 'Self' else fld.fld_valu_desc_eng end as rel_to_insrd
  from    vn_published_cas_db.tclient_policy_links cpl inner join
          vn_published_cas_db.tpolicys pol on pol.pol_num=cpl.pol_num and cpl.link_typ='O' and cpl.rec_status='A' inner join
          vn_published_cas_db.tfield_values fld on cpl.rel_to_insrd=fld.fld_valu and fld.fld_nm='REL_TO_INSRD'
  where   rel_to_insrd is not null
    and   pol_stat_cd in ('1','2','3','5','7')
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
  from    vn_published_cas_db.tclient_policy_links cpl inner join
          vn_published_cas_db.tpolicys pol on pol.pol_num=cpl.pol_num and cpl.link_typ='O' and cpl.rec_status='A' inner join
          vn_published_cas_db.tcoverages cvg on pol.pol_num=cvg.pol_num inner join
          vn_published_cas_db.tfield_values fld on cvg.rel_to_insrd=fld.fld_valu and fld.fld_nm='REL_TO_INSRD'
  where   cvg.rel_to_insrd is not null
    and   pol_stat_cd in ('1','2','3','5','7')
    and   cvg.cvg_typ='R'
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consolidate all metrics al cus_base_extra

# COMMAND ----------

#maturity_list.agg(F.count("po_num").alias("row_count"), F.countDistinct("po_num").alias("po_count")).display()

# COMMAND ----------

#.withColumn("hcr_ind",      F.col("hc_rider_ind")) \
vtb_cus_df = cus_base_xtra\
        .join(vtb_leads,            on="po_num", how="left") \
        .join(merged_activation,    on="po_num", how="left") \
        .join(m25_list,             on="po_num", how="left") \
        .join(protection_gap,       on="po_num", how="left") \
        .join(maturity_list,        on="po_num", how="left") \
        .join(fully_paid_grouped,   on="po_num", how="left") \
        .join(po_base_for,          on="po_num", how="left") \
        .join(po_rider_for,         on="po_num", how="left") \
        .join(rider_holding,        on="po_num", how="left") \
        .join(vtb_taar,             on="po_num", how="left") \
        .withColumn("exclusion",    F.when(F.col("eligibility") == "Eligible", "N").otherwise("Y")) \
        .withColumn("sms_ind",      F.when((F.col("mobl_phon_num").isNotNull()) & 
                                           (F.length(F.col("mobl_phon_num")) == 10), "Y").otherwise("N"))

# Define the condition for when the email address matches the pattern and has length > 6
email_condition = (F.col("email_addr").rlike(r'.+@.+\..+') & (F.length(F.col("email_addr")) > 6))

# Update the 'sms_eservice_ind' column based on the condition
vtb_cus_df = vtb_cus_df.withColumn(
    "sms_eservice_ind",
    F.when(email_condition, "Y").otherwise(F.col("sms_eservice_ind")))

vtb_cus_df = vtb_cus_df.drop(
    "vtb_cat",
    "cli_nm",
    "mobl_phon_num",
    "email_addr",
    "fully_paid_ape",
    "base_parent_ins_ind",
    "base_spouse_ins_ind",
    "base_child_ins_ind",
    "base_oth_ins_ind",
    "rid_parent_ins_ind",
    "rid_spouse_ins_ind",
    "rid_child_ins_ind",
    "rid_oth_ins_ind",
    "hc_rider_ind",
    "mc_rider_ind"
)

vtb_cus_df.cache()

# COMMAND ----------

# Double check data before proceed
vtb_sum = vtb_cus_df \
    .groupBy(['exclusion','eligibility','vip_status','hcr_ind'
              ]) \
    .agg(
        F.count('po_num').alias('row_count'),
        F.countDistinct('po_num').alias('po_count'),
        F.countDistinct('agt_code').alias('agt_count'),
        F.sum('inforce_pol').alias('pol_count'),
        F.sum('total_ape').cast("int").alias('total_ape')
    ).orderBy(['exclusion','eligibility'])

vtb_sum.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only Inforce VTB customers

# COMMAND ----------

vtb_cus_pd = vtb_cus_df.toPandas()
vtb_final = vtb_cus_pd.copy()

total_count = vtb_final.shape[0]
distinct_po_count = vtb_final['po_num'].nunique()
diff_count = total_count - distinct_po_count

if total_count == distinct_po_count:
    print(f"Total record count is THE SAME as PO count: {total_count} ==> [OK]")
else:
    print(f"Total count is {diff_count} rows MORE THAN PO count ==> [RE-CHECK]")
#vtb_cus_pd[vtb_cus_pd['po_num'].isin(['2805687402','2807427062'])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorize all metrics

# COMMAND ----------

# Define bins and labels for each column to be binned
bins_labels = [
    # Add new features
    ('age', [0, 24, 30, 35, 40, 45, 50, 55, float('inf')],
     ['<25', '25-30', '31-35', '36-40', '41-45', '46-50', '51-55', '55+']),
    ('inforce_pol', [0, 0.1, 1, 2, float('inf')],
     ['0', '1', '2', '3+']),
    ('total_ape', [-float('inf'), 0, 1000, 2000, 3000, 5000, 7000, 10000, float('inf')], 
     ['0', '1. <=1k', '2. 1-2k', '3. 2-3k', '4. 3-5k', '5. 5-7k', '6. 7-10k', '7. >10k']),
    ('rider_cnt', [0, 0.1, 1, 2, float('inf')],
     ['0', '1', '2', '3+']),
    ('rider_ape', [-float('inf'), 0, 1000, 2000, 3000, 5000, float('inf')], 
     ['0', '1. <=1k', '2. 1-2k', '3. 2-3k', '4. 3-5k', '5. 5k+']),
    ('adj_mthly_incm', [0, 500, 1000, 1500, 2000, 3000, 5000, float('inf')],
     ['1. <500', '2. 500-1k', '3. 1-1.5k', '4. 1.5-2k', '5. 2-3k', '6. 3-5k', '7. >5k']),    
    ('client_tenure', [-1, 1, 2, 3, 4, float('inf')],
     ['<1', '1-2', '2-3', '3-4', '4+']),
    ('month_since_lst_eff_dt', [-np.inf, 3, 6, 9, 12, 24, np.inf],
     ['1.1-3mth', '2.3-6mth', '3.6-9mth', '4.<1yr', '5.1-2yr', '6.>2yr'])
]

# Apply the function to each feature
for column, bins, labels in bins_labels:
    create_categorical(vtb_final, column, bins, labels)

# Add 'Unknown' to the categories
vtb_final['adj_mthly_incm_cat'] = vtb_final['adj_mthly_incm_cat'].cat.add_categories('Unknown')

# Fill NaN values with 'Unknown'
vtb_final['adj_mthly_incm_cat'] = vtb_final['adj_mthly_incm_cat'].fillna('Unknown')

# COMMAND ----------

# Initialize an empty list to store the results
exclusion_data = []

# Calculate the count and percentage of each exclusion flag
exclusion_flags = ["clm_ind", "med_his_ind", "excl_ind", "rating_his_ind", "occp_cls_ind", "decl_ind", "po_not_insrd_ind"#, "ul19_ind", "ilp_ind"
                   ]

total_customers = vtb_final["po_num"].nunique()
remaining_count = total_customers

for flag in exclusion_flags:
    flag_count = 0
    flag_count = vtb_cus_pd[vtb_cus_pd[flag] == "Y"]["po_num"].nunique()
    
    remaining_count = total_customers - flag_count
    flag_ratio = flag_count / total_customers * 100
    exclusion_data.append({"exclusion": flag, "count": flag_count, "remaining": remaining_count, "ratio": flag_ratio})

# Create the Pandas dataframe from the list of dictionaries using pd.concat()
exclusion_pd = pd.concat([pd.DataFrame([data]) for data in exclusion_data], ignore_index=True)

# Display the resulting Pandas dataframe
print(exclusion_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add proposition

# COMMAND ----------

# Propositions for Pandas DataFrame
# Convert 'bday_month' to integer
vtb_final['bday_month'] = vtb_final['bday_month'].fillna(0)
vtb_final['bday_month'] = vtb_final['bday_month'].astype(int)

# Create the 'proposition' column using np.select
conditions = [
    vtb_final['eligibility'] == "Eligible",
    vtb_final['protection_income_grp'].isin(["3. 20% Income Protection", "4. below 20% Income Protection", "5. No dependent/Protection Pols"]),
    (vtb_final['age'].isin([25, 30, 34, 39, 49, 69])) & (vtb_final['bday_month'] > 10),
    vtb_final['base_self_ins_ind'] == 0
]
choices = [
    "1. Pre-approved",
    "2. Protection Gap > 75%",
    "3. Borderline age",
    "4. PO have only riders"
]

vtb_final['proposition'] = np.select(conditions, choices, default="5. Others")

# Fill NaN values in specific columns with specific values
vtb_final.fillna({'base_self_ins_ind' : 0, 'rid_self_ins_ind' : 0, 'decile_cat' : 'Bottom',
                  'protection_income_grp': '5. No dependent/Protection Pols', 'protection_sa' : 0,
                  'ilp_offr' : 0, 'ilp_offr_cat' : '0. No SA', 'ul_offr' : 0, 'ul_offr_cat' : '0. No SA',
                  'trop_offr' : 0, 'trop_offr_cat' : '0. No SA', 'eligibility' : 'Ineligible'}, inplace=True)

vtb_final['months_since_lst_pur'] = vtb_final['month_since_lst_eff_dt_cat']
vtb_final = vtb_final.drop(columns=['month_since_lst_eff_dt_cat'])
#vtb_cus_dedup = vtb_cus_final.dropDuplicates(["po_num"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store as partitioned data

# COMMAND ----------

vtb_final.to_parquet(f'/dbfs/mnt/lab/vn/project/cpm/VTB/Pre_approved/VTB_preapproved_uw_{run_date}.parquet', index=False)
vtb_final.to_csv(f'/dbfs/mnt/lab/vn/project/cpm/VTB/Pre_approved/VTB_preapproved_uw_{run_date}.csv', header=True, index=False, encoding='utf-8-sig')
print(vtb_final.shape)
vtb_final.head(2)

# COMMAND ----------

vtb_final[vtb_final["po_num"]=="2807856067"]

# COMMAND ----------

# MAGIC %md
# MAGIC # Validate result

# COMMAND ----------

#import pyspark.sql.functions as F

# Load the data frame
                        #.filter(F.col("decile").isin(1,2,3)) \
vtb_cus_df = spark.read.format("parquet").load(f"/mnt/lab/vn/project/cpm/VTB/Pre_approved/VTB_preapproved_uw_{run_date}.parquet") \
                        .select("po_num", "exclusion", "eligibility", "vip_status", "decile_cat", "ilp_ind", "hcr_ind", "decile",
                                "total_ape",  "protection_income%", "protection_income_grp",
                                "base_self_ins_ind", "rid_self_ins_ind"
                                )
#vtb_cus_df.createOrReplaceTempView("vtb_leads")

vtb_cus_grp = vtb_cus_df.groupBy("exclusion","eligibility","decile_cat","vip_status").agg(
    F.count(F.col("po_num")).alias("no_leads")
).sort("eligibility")

#vtb_cus_grp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checking status of the customers

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /*select  po_num, pol_num, plan_code, plan_nm, TOT_APE_USD, to_date(pol_iss_dt) issue_date, cast(cus.MTHLY_INCM/23.1345 as decimal(12,2)) adj_mthly_incm
# MAGIC from    vn_curated_datamart_db.tpolidm_daily pol inner join
# MAGIC         vn_curated_datamart_db.tcustdm_daily cus on pol.po_num = cus.CLI_NUM
# MAGIC where   po_num='2807856067'*/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate detail lead list

# COMMAND ----------

#lead_list = vtb_final[vtb_final['exclusion'] == 'N'].drop(columns=['exclusion'])
#lead_list.to_csv(f'/dbfs/mnt/lab/vn/project/cpm/VTB/Lead_lists/VTB_Leads_List_{run_date}.csv', header=True, index=False)
#lead_list.head(2)
