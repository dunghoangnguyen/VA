# Databricks notebook source
# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime, timedelta
import pandas as pd

run_date = pd.Timestamp.now().strftime('%Y%m%d')
lst_mthend = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')

cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cseg_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_PD/image_date={lst_mthend}'

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
print(run_date, lst_mthend)

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
    "new_PO_NUM",
    F.when(F.col("con.cli_num").isNull(), F.col("pol.PO_NUM")).otherwise(F.col("con.cli_num"))
).drop(F.col("pol.PO_NUM")) \
.withColumnRenamed("new_PO_NUM", "PO_NUM")

# Move "PO_NUM" to the beginning
columns = tpolidm_df.columns
columns.insert(0, columns.pop(columns.index("PO_NUM")))

tpolidm_df = tpolidm_df.select(columns)
tpolidm_df.filter(F.col('PO_NUM').isin('2805687402', '2807427062')).display()
tpolidm_df.createOrReplaceTempView("tpolidm")

# COMMAND ----------

generate_temp_view(df_list)
#tpolidm_df.createOrReplaceTempView("tpolidm")
tagtdm_df.createOrReplaceTempView("tagtdm")
tcustdm_df.createOrReplaceTempView("tcustdm")
csegdm_df.createOrReplaceTempView("csegdm_mthend")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PREPARE PRE-APPROVED INDICATORS AND TCB CUSTOMERS DATA

# COMMAND ----------

# MAGIC %sql
# MAGIC /*desc tnt_doc_cli
# MAGIC select  req.pol_num,
# MAGIC         doc.cli_num,
# MAGIC         doc.remark,
# MAGIC         doc.uwg_rmk,
# MAGIC         doc.health_risk,
# MAGIC         req.medic_item,
# MAGIC         to_date(req.recv_dt) recv_dt,
# MAGIC         to_date(req.req_dt) req_dt,
# MAGIC         to_date(req.exam_dt) exam_dt,
# MAGIC         req.user_id,
# MAGIC         req.rslt,
# MAGIC         sub.sub_desc,
# MAGIC         sub.item_code,
# MAGIC         sub.letter_id,
# MAGIC         hdr.request_id,
# MAGIC         hdr.send_aws,
# MAGIC         to_date(hdr.signed_dt) signed_dt,
# MAGIC         hdr.status
# MAGIC from    tnt_doc_cli doc inner join 
# MAGIC         tnt_doc_req req on doc.pol_num=req.pol_num and doc.trxn_id=req.trxn_id left join
# MAGIC         tpt_sub_dtl sub on doc.pol_num=sub.pol_num left join
# MAGIC         tnb_letter_hdr hdr on sub.pos_num=hdr.pos_num and sub.letter_id=hdr.letter_id
# MAGIC         
# MAGIC where   1=1
# MAGIC     and req.pol_num='2907655038'*/
# MAGIC
# MAGIC     select  req.pol_num,
# MAGIC         doc.cli_num,
# MAGIC         doc.remark,
# MAGIC         doc.uwg_rmk,
# MAGIC         doc.health_risk,
# MAGIC         req.medic_item,
# MAGIC         to_date(req.recv_dt) recv_dt,
# MAGIC         to_date(req.req_dt) req_dt,
# MAGIC         to_date(req.exam_dt) exam_dt,
# MAGIC         req.user_id,
# MAGIC         req.rslt,
# MAGIC         sub.sub_desc,
# MAGIC         sub.item_code,
# MAGIC         sub.letter_id,
# MAGIC         hdr.request_id,
# MAGIC         hdr.send_aws,
# MAGIC         to_date(hdr.signed_dt) signed_dt,
# MAGIC         hdr.status
# MAGIC from    tnt_doc_cli doc inner join 
# MAGIC         tnt_doc_req req on doc.pol_num=req.pol_num and doc.trxn_id=req.trxn_id left join
# MAGIC         tpt_sub_dtl sub on doc.pol_num=sub.pol_num left join
# MAGIC         tnb_letter_hdr hdr on sub.pos_num=hdr.pos_num and sub.letter_id=hdr.letter_id
# MAGIC         
# MAGIC where   1=1
# MAGIC     and hdr.status ="A" or health_risk = "Y"
# MAGIC limit   5    
# MAGIC

# COMMAND ----------

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
#print(hcr_his_dtl.count())

# Customers with health claim histories (policy level)
clm_his_dtl = spark.sql(f"""
select  distinct pol_num
        , 'Y' clm_ind
from    tclaim_details
where   1=1
    and clm_code in (27, 28, 29)
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
#rate_his.limit(10).display()

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
where   pol_stat_cd in ('X','L','R','N')                  
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge all rules to identify the PO exclusion list

# COMMAND ----------

pol_cols = ["po_num","pol_num","pol_stat_cd","dist_chnl_cd","dist_chnl_desc","sa_code"]

# Map all exclusion rules and filter out the ones that hit
excl_cus = tpolidm_df.select(pol_cols).alias("pol") \
                    .join(ul19_his_dtl.alias("ul19"), on="pol_num", how="left") \
                    .join(hcr_his_dtl.alias("hcr"), on="pol_num", how="left") \
                    .join(clm_his_dtl.alias("clm"), on="pol_num", how="left") \
                    .join(med_his_dtl.alias("med"), on="pol_num", how="left") \
                    .join(excl_dtl.alias("excl"), on="pol_num", how="left") \
                    .join(rate_his.alias("rate"), on ="pol_num", how="left") \
                    .join(occp_cls.alias("occp"), on="pol_num", how="left") \
                    .join(decl_his.alias("decl"), on="pol_num", how="left") \
                    .withColumn("exclusion_ind",
                         F.when((F.col("clm.clm_ind") == "Y") | 
                                (F.col("ul19.ul19_ind") == "Y") |
                                (F.col("hcr.hcr_ind") == "Y") | 
                                (F.col("med.med_his_ind") == "Y") | 
                                (F.col("excl.excl_ind") == "Y") | 
                                (F.col("rate.rating_his_ind") == "Y") |
                                (F.col("occp.occp_cls_ind") == "Y") |
                                (F.col("decl.decl_ind") == "Y"), "Y")
                         .otherwise("N")) \
                    .groupby("po_num").agg(
                        F.max(F.col("ul19.ul19_ind")).alias("ul19_ind"),
                        F.max(F.col("hcr.hcr_ind")).alias("hcr_ind"),
                        F.max(F.col("clm.clm_ind")).alias("clm_ind"),
                        F.max(F.col("med.med_his_ind")).alias("med_his_ind"),                        
                        F.max(F.col("excl.excl_ind")).alias("excl_ind"),
                        F.max(F.col("rate.rating_his_ind")).alias("rating_his_ind"),
                        F.max(F.col("occp.occp_cls_ind")).alias("occp_cls_ind"),
                        F.max(F.col("decl.decl_ind")).alias("decl_ind"),
                        F.max(F.col("exclusion_ind")).alias("exclusion_ind")
                    )

#excl_cus = excl_cus.filter(F.col("exclusion_ind") == "Y")

excl_cus.createOrReplaceTempView("excl_cus")
print("No. of excluded cus:", excl_cus.count())
print("No. of HCR cus:", excl_cus.filter(F.col("hcr_ind")=="Y").count())
#excl_pols.limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Sizing on TCB customers

# COMMAND ----------

# Derive base policys from TCB (or any Banca locations)
pol_base = spark.sql(f"""
with pol_onb as (
    select  po_num,
            case 
            when b.loc_cd is not null and c.loc_cd is not null then '1.Onboarded and served by TCB'
            when b.loc_cd is not null then 
                case 
                    when c.loc_cd is not null then '2.Onboarded by TCB and served by TCB' 
                    when c.loc_cd is null and a.dist_chnl_cd in ('10','24','49') then '3.Onboarded by TCB'
                    else '5.Non-TCB'
                end
            when c.loc_cd is not null then '3.Served by TCB'
            when a.dist_chnl_cd in ('10','24','49') then '4.Onboarded by TCB'
            else '5.Non-TCB'
        end as tcb_cat
    from    tpolidm a                         left join
            tagtdm  b on a.wa_code=b.agt_code left join
            tagtdm  c on a.sa_code=c.agt_code 
    where   1=1
        and pol_stat_cd not in ('A','N','R','X')
        and (b.loc_cd like 'TCB%' or
             c.loc_cd like 'TCB%' or
             a.dist_chnl_cd in ('10','24','49'))
),
po_cat as (
    select  po_num, min(tcb_cat) tcb_cat
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
)
select  t.po_num as po_num, nvl(rider_cnt,0) rider_cnt, nvl(rider_ape,0) rider_ape, agt_code, tcb_cat, 
        coalesce(ul19_ind,'N') ul19_ind,
        coalesce(hcr_ind,'N') hcr_ind, 
        coalesce(clm_ind,'N') clm_ind,
        coalesce(med_his_ind,'N') med_his_ind,
        coalesce(excl_ind,'N') excl_ind,
        coalesce(rating_his_ind,'N') rating_his_ind,
        coalesce(occp_cls_ind,'N') occp_cls_ind,
        coalesce(decl_ind,'N') decl_ind,
        coalesce(exclusion_ind,'N') exclusion_ind
from    (
        select  po_num, sa_code agt_code, 
                row_number() over (partition by po_num order by pol_eff_dt desc) as rn
        from    tpolidm
        where   1=1
        ) t inner join
        po_cat      b on t.po_num=b.po_num  left join
        excl_cus    c on t.po_num=c.po_num  left join
        rider       d on t.po_num=d.po_num
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
        where   pol.pol_stat_cd not in ('8','A','L','N','R','X')
        ) t
where   rn=1
""")

# Group by customer level and latest serving agent
tcb_cus_base = pol_base.groupBy(['po_num', 'agt_code'])\
                    .agg(
                        F.sum('rider_cnt').alias('rider_cnt'),
                        F.sum('rider_ape').alias('rider_ape'),
                        F.min('tcb_cat').alias('tcb_cat'),
                        F.max('ul19_ind').alias('ul19_ind'),
                        F.max('hcr_ind').alias('hcr_ind'),
                        F.max('clm_ind').alias('clm_ind'),
                        F.max('med_his_ind').alias('med_his_ind'),
                        F.max('excl_ind').alias('excl_ind'),
                        F.max('rating_his_ind').alias('rating_his_ind'),
                        F.max('occp_cls_ind').alias('occp_cls_ind'),
                        F.max('decl_ind').alias('decl_ind'),
                        F.max('exclusion_ind').alias('exclusion_ind')
                    )

# Exclusion customers group
tcb_excl_cus = excl_cus.filter(F.col('exclusion_ind')=="Y")

# Add exclusion / PII / contact and/or demographic information
tcb_cus_base_xtra = tcb_cus_base \
                    .join(tcb_excl_cus,
                          tcb_cus_base["po_num"]==tcb_excl_cus["po_num"],
                         how="left"
                    ) \
                    .join(tcustdm_df,
                          tcb_cus_base["po_num"]==tcustdm_df["cli_num"],
                          how="inner"     
                    ) \
                    .join(lst_prod,
                          tcb_cus_base["po_num"]==lst_prod["po_num"],
                          how="inner"     
                    ) \
                    .join(csegdm_df,
                          tcb_cus_base["po_num"]==csegdm_df["po_num"],
                          how="left") \
                    .select(
                        tcb_cus_base["po_num"].alias("po_num"),
                        "cli_nm",
                        "tcb_cat",
                        "mobl_phon_num",
                        "email_addr",
                        "sms_ind",
                        "sms_eservice_ind",
                        tcb_cus_base["ul19_ind"],
                        tcb_cus_base["hcr_ind"],
                        tcb_cus_base["clm_ind"],
                        tcb_cus_base["med_his_ind"],
                        tcb_cus_base["excl_ind"],
                        tcb_cus_base["rating_his_ind"],
                        tcb_cus_base["occp_cls_ind"],
                        tcb_cus_base["decl_ind"],
                        tcb_cus_base["exclusion_ind"].alias("exclusion"),
                        "agt_code",
                        "ins_typ_count",
                        "inforce_pol",
                        "investment_pol",
                        "term_pol",
                        "endow_pol",
                        "whole_pol",
                        "health_indem_pol",
                        csegdm_df["total_ape"],
                        F.coalesce(tcb_cus_base["rider_cnt"],csegdm_df["rider_cnt"]).alias("rider_cnt"),
                        F.coalesce(tcb_cus_base["rider_ape"],csegdm_df["rider_ape"]).alias("rider_ape"),
                        "client_tenure",
                        "adj_mthly_incm",
                        "income_decile",
                        lst_prod["lst_eff_dt"],
                        "lst_prod_typ",
                        "inforce_ind",
                        (F.datediff(F.lit(lst_mthend), F.col("birth_dt")) / 365.25).cast("int").alias("age")  # Calculate age in years
                    ) \
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
#tcb_cus_base_xtra = tcb_cus_base_xtra.withColumn("exclusion_hcr", 
#    F.when((F.col("hcr_ind") == "Y"), "Y")
#    .otherwise(F.col("exclusion")))

# Double check the numbers before proceed
tcb_sum = tcb_cus_base_xtra.filter(F.col('inforce_ind')==1) \
    .groupBy(['exclusion' #,'exclusion_hcr','rider_typ'
              ]) \
    .agg(
        F.count('po_num').alias('no_records'),
        F.countDistinct('po_num').alias('no_of_pos'),
        F.countDistinct('agt_code').alias('no_of_agts'),
        F.sum('inforce_pol').alias('no_of_pols'),
        F.sum('total_ape').alias('total_ape')
    ).orderBy(['exclusion'])

tcb_sum.display()
#tcb_cus_base_xtra.filter(F.col('po_num').isin('2805687402','2807427062')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only Inforce TCB customers

# COMMAND ----------

tcb_cus_pd = tcb_cus_base_xtra.filter(F.col('inforce_ind') == 1).toPandas()
print(tcb_cus_pd.shape[0])
tcb_cus_pd[tcb_cus_pd['po_num'].isin(['2805687402','2807427062'])]

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
    ('total_ape', [0, 1000, 2000, 3000, 5000, 7000, 10000, float('inf')], 
     ['1. <=1k', '2. 1-2k', '3. 2-3k', '4. 3-5k', '5. 5-7k', '6. 7-10k', '7. >10k']),
    ('rider_cnt', [0, 0.1, 1, 2, float('inf')],
     ['0', '1', '2', '3+']),
    ('rider_ape', [0, 1000, 2000, 3000, 5000, float('inf')], 
     ['1. <=1k', '2. 1-2k', '3. 2-3k', '4. 3-5k', '5. 5k+']),
    ('adj_mthly_incm', [500, 1000, 1500, 2000, 3000, 5000, float('inf')],
     ['1. 500-1k', '2. 1-1.5k', '3. 1.5-2k', '4. 2-3k', '5. 3-5k', '6. >5k']),    
    ('client_tenure', [0, 1, 2, 3, 4, float('inf')],
     ['<1', '1-2', '2-3', '3-4', '4+']),
]

# Apply the function to each feature
for column, bins, labels in bins_labels:
    create_categorical(tcb_cus_pd, column, bins, labels)

# COMMAND ----------

# Initialize an empty list to store the results
exclusion_data = []

# Calculate the count and percentage of each exclusion flag
exclusion_flags = ["clm_ind", "med_his_ind", "excl_ind", "rating_his_ind", "occp_cls_ind", "ul19_ind", "hcr_ind", "decl_ind"]

total_customers = tcb_cus_pd["po_num"].nunique()
remaining_count = total_customers

for flag in exclusion_flags:
    flag_count = 0
    flag_count = tcb_cus_pd[tcb_cus_pd[flag] == "Y"]["po_num"].nunique()
    
    remaining_count = total_customers - flag_count
    flag_ratio = flag_count / total_customers * 100
    exclusion_data.append({"exclusion": flag, "count": flag_count, "remaining": remaining_count, "ratio": flag_ratio})

# Create the Pandas dataframe from the list of dictionaries using pd.concat()
exclusion_pd = pd.concat([pd.DataFrame([data]) for data in exclusion_data], ignore_index=True)

# Display the resulting Pandas dataframe
print(exclusion_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store as partitioned data

# COMMAND ----------

tcb_cus_pd.to_parquet(f'/dbfs/mnt/lab/vn/project/cpm/TCB/tcb_preapproved_uw_{run_date}.parquet', index=False)
tcb_cus_pd.to_csv(f'/dbfs/mnt/lab/vn/project/cpm/TCB/tcb_preapproved_uw_{run_date}.csv', header=True, index=False, encoding='utf-8-sig')
print(tcb_cus_pd.shape)
#tcb_cus_pd[tcb_cus_pd['po_num'].isin(['2805687402','2807427062'])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate detail lead list

# COMMAND ----------


#lead_list = tcb_cus_pd[tcb_cus_pd['exclusion'] == 'N'].drop(columns=['exclusion'])
#lead_list.to_csv(f'/dbfs/mnt/lab/vn/project/cpm/TCB/Lead_lists/TCB_Leads_List_{run_date}.csv', header=True, index=False)
#lead_list.head(2)
