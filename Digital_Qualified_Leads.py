# Databricks notebook source
# MAGIC %md
# MAGIC # Digital Qualified Leads - Existing Customers

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load lib, paths and params</strong>

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta
import calendar

existing_leads_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/existing_leads.csv'
cus_seg_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/cseg_cltv'
needs_model_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/score/ex_score.csv'

tcoverages_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCOVERAGES/'
tfield_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TFIELD_VALUES/'
tpolidm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/'
tcustdm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TCUSTDM_MTHEND/'

# Get the last month-end from current system date
#last_mthend = datetime.strftime(datetime.now().replace(day=1) - timedelta(days=1), '%Y-%m-%d')

x = 0 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
last_mthend = last_day_of_x_months_ago.strftime('%Y-%m-%d')
monthend_dt = last_mthend[0:7]
print("Selected last_mthend =", last_mthend)
print("Selected monthend_dt =", monthend_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load and immediate tables</strong>

# COMMAND ----------

existing_leads = spark.read.format("csv").option("header", True).load(existing_leads_path)
cus_seg = spark.read.format("parquet").load(cus_seg_path)
needs_model = spark.read.format("csv").option("header", True).load(needs_model_path)
tcoverages = spark.read.format("parquet").load(tcoverages_path)
tfield = spark.read.format("parquet").load(tfield_path) 
tpolidm = spark.read.format("parquet").load(tpolidm_path)
tcustdm = spark.read.format("parquet").load(tcustdm_path)

# Standardize column headers
#existing_leads = existing_leads.toDF(*[col.lower() for col in existing_leads.columns])
cus_seg = cus_seg.toDF(*[col.lower() for col in cus_seg.columns])
needs_model = needs_model.toDF(*[col.lower() for col in needs_model.columns])
tcoverages = tcoverages.toDF(*[col.lower() for col in tcoverages.columns])
tfield = tfield.toDF(*[col.lower() for col in tfield.columns])
tpolidm = tpolidm.toDF(*[col.lower() for col in tpolidm.columns])
tcustdm = tcustdm.toDF(*[col.lower() for col in tcustdm.columns])

# Add decile cluster to cus_seg
#cus_seg.createOrReplaceTempView("cus_seg")
#cus_seg_qtl = spark.sql("""select a.* , ntile(10) over (order by a.cli_ltv desc) as decile from cus_seg a """)

# Get Face amount for Protection gap calculation
fatmpDF = tpolidm.filter(col("pol_stat_cd").isin(['1', '3'])) \
    .groupBy("po_num", "pol_num").agg(max("tot_face_amt").alias("tot_face_amt"))

faDF = fatmpDF.groupBy("po_num").agg((sum("tot_face_amt") / 23.145).alias("tot_face_amt_usd"))

tcoverages = tcoverages.filter(col("image_date") == last_mthend)
tfield = tfield.filter(col("image_date") == last_mthend)
tpolidm = tpolidm.filter(col("image_date") == last_mthend)
tcustdm = tcustdm.filter(col("image_date") == last_mthend)

# Map existing leads with tpolidm to get po_num
existing_leads = existing_leads.alias("ex") \
    .join(tpolidm.alias("pol"), on="pol_num", how="inner") \
    .select("pol.po_num").dropDuplicates()

#print("# inforce policies: ", faDF.count())
#faDF.display(20)
#print("Existing leads: ", existing_leads.count())
#existing_leads.display(10)

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Retrieve family relation data</strong>

# COMMAND ----------

fml_tmp = tpolidm.alias("pol") \
    .join(tcoverages.alias("cov"), col("pol.pol_num") == col("cov.pol_num")) \
    .join(tfield.alias("fld"), col("cov.rel_to_insrd") == col("fld.fld_valu")) \
    .where((col("fld.fld_nm") == "REL_TO_INSRD") & (~col("cov.rel_to_insrd").isin(["00", "01"]))) \
    .selectExpr("po_num", "cli_num", "cov.rel_to_insrd", "fld_valu_desc", "fld_valu_desc_eng").dropDuplicates()

fml_grandpa = fml_tmp.filter(col('rel_to_insrd') == '05').select('po_num').distinct().withColumn('dpnd_grandpa_ind', lit(1))
fml_parent = fml_tmp.filter(col('rel_to_insrd').isin(['51', '52'])).select('po_num').distinct().withColumn('dpnd_parent_ind', lit(1))
fml_spouse = fml_tmp.filter(col('rel_to_insrd') == '02').select('po_num').distinct().withColumn('dpnd_spouse_ind', lit(1))
fml_child = fml_tmp.filter(col('rel_to_insrd') == '03').select('po_num').distinct().withColumn('dpnd_child_ind', lit(1))
fml_sib = fml_tmp.filter(col('rel_to_insrd') == '04').select('po_num').distinct().withColumn('dpnd_sibling_ind', lit(1))
fml_oth = fml_tmp.filter(col('rel_to_insrd').isin(['10', '31'])).select('po_num').distinct().withColumn('dpnd_oth_ind', lit(1))

fmlDF = tcustdm.select(col('cli_num').alias('po_num')) \
    .join(fml_grandpa, on='po_num', how='left') \
    .join(fml_parent, on='po_num', how='left') \
    .join(fml_spouse, on='po_num', how='left') \
    .join(fml_child, on='po_num', how='left') \
    .join(fml_sib, on='po_num', how='left') \
    .join(fml_oth, on='po_num', how='left') \
    .select(
        'po_num',
        when(col('dpnd_grandpa_ind').isNull(), 0).otherwise(col('dpnd_grandpa_ind')).alias('dpnd_grandpa_ind'),
        when(col('dpnd_parent_ind').isNull(), 0).otherwise(col('dpnd_parent_ind')).alias('dpnd_parent_ind'),
        when(col('dpnd_spouse_ind').isNull(), 0).otherwise(col('dpnd_spouse_ind')).alias('dpnd_spouse_ind'),
        when(col('dpnd_child_ind').isNull(), 0).otherwise(col('dpnd_child_ind')).alias('dpnd_child_ind'),
        when(col('dpnd_sibling_ind').isNull(), 0).otherwise(col('dpnd_sibling_ind')).alias('dpnd_sibling_ind'),
        when(col('dpnd_oth_ind').isNull(), 0).otherwise(col('dpnd_oth_ind')).alias('dpnd_oth_ind')
    ) \
    .where((col('dpnd_grandpa_ind') == 1) |
           (col('dpnd_parent_ind') == 1) |
           (col('dpnd_spouse_ind') == 1) |
           (col('dpnd_child_ind') == 1) |
           (col('dpnd_sibling_ind') == 1) |
           (col('dpnd_oth_ind') == 1)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Add all data points to Master</strong>

# COMMAND ----------

existing_leads.createOrReplaceTempView("existing_leads")
cus_seg.createOrReplaceTempView("cus_seg")
needs_model.createOrReplaceTempView("needs_model")
fmlDF.createOrReplaceTempView("fmlDF")
faDF.createOrReplaceTempView("faDF")

existing_leads_tmp = spark.sql(f"""
    select  ex.po_num as po_num,
            cseg.cus_gender as gender,
            cseg.first_pol_eff_dt as frst_pol_dt,
            floor(months_between('{last_mthend}', cseg.first_pol_eff_dt)) as mob,
            case when floor(months_between('{last_mthend}', cseg.first_pol_eff_dt)) < 12 then '1. One year'
                 when floor(months_between('{last_mthend}', cseg.first_pol_eff_dt)) between 12 and 23 then '2. Less than 2 years'
                 when floor(months_between('{last_mthend}', cseg.first_pol_eff_dt)) between 24 and 36 then '3. 2-3 years'
                 when floor(months_between('{last_mthend}', cseg.first_pol_eff_dt)) > 36 then '4. More than 3 years'
            end as mob_cat,
            cseg.client_tenure as client_tenure,
            case when cseg.pol_count<2 then '1.Only 1 policy'
                 when cseg.pol_count between 2 and 3 then '2.2-3 policies'
                 when cseg.pol_count between 3 and 5 then '3.3-5 policies'
                 when cseg.pol_count>5 then '4.>5 policies'
            end as policy_holding,
            case when ins_typ_count <= 1 then '1. Single'
                 else '2. Multiple'
            end as ins_cat,
            cseg.f_term_pol as term_ind,
            cseg.f_endow_pol as endow_ind,
            cseg.f_health_indem_pol as health_ind,
            cseg.f_whole_pol as whole_ind,
            cseg.f_investment_pol as investment_ind,
            case when inforce_ind = 1 then 'Inforce' else 'Not-inforce' end as inforce_cat,
            cseg.agt_tenure_yrs as agt_tenure_yrs,
            case when cseg.agt_tenure_yrs is not null then
                 case when cseg.agt_tenure_yrs < 3 then '1. New agent'
                      when cseg.agt_tenure_yrs between 3 and 5 then '2. Experienced agent'
                      when cseg.agt_tenure_yrs > 5 and cseg.agt_tenure_yrs < 10 then '3. Veteran agent'
                      when cseg.agt_tenure_yrs >= 10 then '4. Senior agent'
                 end
                 else '5. None'
            end as agt_tenure_cat,
            case when cseg.mdrt_tot_flag = 1 then '1. TOT'
                 when cseg.mdrt_cot_flag = 1 then '2. COT'
                 when cseg.mdrt_flag = 1 then '3. MDRT'
                 when cseg.active_1m_flag = 1 then '4. 1mA'
                 else '5. Others'
            end as agt_type,
            case when cseg.unassigned_ind = 1 then 'Unassigned' else 'Active agent' end as unassigned_cat,
            case when cseg.valid_email=1 and cseg.valid_mobile=1 then '1. Both email and mobile'
                 when cseg.valid_email=0 and cseg.valid_mobile=1 then '2. Mobile only'
                 when cseg.valid_email=1 and cseg.valid_mobile=0 then '3. Email only'
                 when cseg.valid_email=0 and cseg.valid_mobile=0 then '4. None'
            end as comm_type,
            case when cseg.total_ape < 500 then '1. Less than $500'
                 when cseg.total_ape >= 500 and cseg.total_ape < 1000 then '2. $500-$1k'
                 when cseg.total_ape >= 2000 and cseg.total_ape < 3000 then '3. $2k-3k'
                 when cseg.total_ape >= 3000 and cseg.total_ape < 5000 then '4. $3k-5k'
                 when cseg.total_ape >= 5000 and cseg.total_ape < 10000 then '5. $5k-10k'
                 when cseg.total_ape >= 10000 then '6. More than $10k'
            end as ape_usd_cat,
            case when cseg.top1_3_lapse = 1 then 'High lapse rate'
                 else 'Low lapse rate'
            end as lapse_ind,
            cseg.channel_final as channel,
            case when cseg.min_decile between 1 and 2 then 'High' else 'Low' end as next_propensity,
            case when need.decile_inv between 1 and 2 then 'High' else 'Low' end as next_inv,
            case when need.decile_ci between 1 and 2 then 'High' else 'Low' end as next_ci,
            case when need.decile_lp between 1 and 2 then 'High' else 'Low' end as next_lp,
            case when need.decile_lt between 1 and 2 then 'High' else 'Low' end as next_lt,
            case when need.decile_acc between 1 and 2 then 'High' else 'Low' end as next_acc,
            case when need.decile_med between 1 and 2 then 'High' else 'Low' end as next_med,
            cseg.cur_age as cur_age,
            cseg.cus_age_band as cus_age_band,
            cseg.customer_segment as customer_segment,
            cseg.city as city,
            NVL(cseg.no_dpnd,0) as no_dpnd,
            NVL(fml.dpnd_grandpa_ind,0) as dpnd_grandpa_ind,
            NVL(fml.dpnd_parent_ind,0) as dpnd_parent_ind,
            NVL(fml.dpnd_spouse_ind,0) as dpnd_spouse_ind,
            NVL(fml.dpnd_child_ind,0) as dpnd_child_ind,
            NVL(fml.dpnd_sibling_ind,0) as dpnd_sibling_ind,
            NVL(fml.dpnd_oth_ind,0) as dpnd_oth_ind,
            case when cseg.cws_reg=1 and cseg.move_reg=1 then '1. Registered to both'
                 when cseg.cws_reg=1 and cseg.move_reg=0 then '2. CWS only'
                 when cseg.cws_reg=0 and cseg.move_reg=1 then '3. MOVE only'
                 when cseg.cws_reg=0 and cseg.move_reg=0 then '4. None'
            end as digital_cat,
            case when NVL(cseg.cws_last_log_days,999) <= 90 or NVL(cseg.move_last_log_days,999) <= 90 then '1. Active last 90 days' 
                 when NVL(cseg.cws_last_log_days,999) <= 180 or NVL(cseg.move_last_log_days,999) <= 180 then '2. Active last 180 days'
                 when NVL(cseg.cws_last_log_days,999) < 999 or NVL(cseg.move_last_log_days,999) < 999 then '3. Barely active'
                 when cseg.cws_last_log_days is null and cseg.move_last_log_days is null then '4. Never logged'
            end as digital_act_cat,
            case when cseg.f_vip_elite=1 then '1. Elite'
                 when cseg.f_vip_plat=1 then '2. Platinum'
                 when cseg.f_vip_gold=1 then '3. Gold'
                 when cseg.f_vip_silver=1 then '4. Silver'
                 else '5. Not categorized'
            end as vip_cat,
            cseg.f_1st_term as term_1st,
            cseg.f_1st_endow as endow_1st,
            cseg.f_1st_health_indem as health_1st,
            cseg.f_1st_whole as whole_1st,
            cseg.f_1st_invest as invest_1st,
            cseg.f_2nd_term as term_2nd,
            cseg.f_2nd_endow as endow_2nd,
            cseg.f_2nd_health_indem as health_2nd,
            cseg.f_2nd_whole as whole_2nd,
            cseg.f_2nd_invest as invest_2nd,
            cseg.yr_2nd_prod as yr_2nd_prod,
            CAST(NVL(cseg.mthly_incm, 0) as decimal(12,2)) as mthly_incm,
            case when cseg.mthly_incm is not null then
                 case when cseg.mthly_incm < 1000 then '1. Less than $1k'
                      when cseg.mthly_incm >= 1000 and cseg.mthly_incm <= 2000 then '2. $1k-2k'
                      when cseg.mthly_incm > 2000 and cseg.mthly_incm <= 3000 then '3. $2k-3k'
                      when cseg.mthly_incm > 3000 and cseg.mthly_incm <= 5000 then '4. $3k-5k'
                      when cseg.mthly_incm > 5000 then '5. More than $5k'
                 end
                 else '9. Income not declared'
            end as mthly_income_cat,
            case when cseg.lead is not null then '1. CPM lead' else '2. Non-CPM' end as cpm_cat,
            case when cseg.new_pol_cust_id is not null then '1. CPM purchase' else '2. Non-CPM purchase' end as cpm_pur_cat,
            CAST(NVL(fa.tot_face_amt_usd, 0) as decimal(12,2)) as total_face_amt,
            case when NVL(cseg.mthly_incm, 0) = 0 then 0
                 else CAST(NVL(cseg.mthly_incm, 0)*84 - NVL(fa.tot_face_amt_usd, 0) as decimal(12,2)) 
            end as protection_gap,
            case when cseg.decile = 1 then '1. VIP'
                 when cseg.decile = 2 then '2. High Value'
                 when cseg.decile = 3 then '3. Med Value'
                 when cseg.decile = 4 then '4. Low Value'
                 when cseg.decile between 5 and 8 then '5. Rev. Neutral'
                 else '6. Loss Making'
            end as segment,
            cseg.cli_ltv as cltv,
            cseg.cli_ltv_post as cltv_post
    from    existing_leads ex left join
            cus_seg cseg on ex.po_num=cseg.po_num left join
            needs_model need on ex.po_num=need.cli_num left join
            fmlDF fml on ex.po_num=fml.po_num left join
            faDF fa on ex.po_num=fa.po_num
""")

existing_leads_final = existing_leads_tmp.filter(col("gender").isin(["Male","Female"])) \
    .withColumn(
        "protection_cat",
        when(col("protection_gap") < 0, "Over-protected")
        .otherwise(
            when(col("protection_gap") == 0, "Fully protected")
            .otherwise("Under-protected")
    )
)
    
existing_leads_final = existing_leads_final.dropDuplicates()

print("Existing Leads (Final):", existing_leads_final.count())

# COMMAND ----------

existing_leads_final.display()
#existing_leads_final.coalesce(1).write.option("header", "true").csv("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/existing_leads_final.csv")

# COMMAND ----------
