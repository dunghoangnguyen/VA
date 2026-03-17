# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime, timedelta
import pandas as pd

run_date = pd.Timestamp.now().strftime('%Y%m%d')
lst_mthend = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%Y-%m-%d')
mthend_sht = lst_mthend[:4]+lst_mthend[5:7]

dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
mat_val_path = '/mnt/lab/vn/project/scratch/ORC_ACID_to_Parquet_Conversion/asia/vn/lab/project/scratch/hive/mat_plan_code/**'

# COMMAND ----------

tpardm_df = spark.read.parquet(f'/mnt/lab/vn/project/cpm/datamarts/TPARDM_MTHEND/image_date={lst_mthend}')
tpardm_df = tpardm_df.select(F.col('agt_cd').alias('agt_code'),'rank_code','tier','last_9m_pol').dropDuplicates()
agt_tier = tpardm_df.withColumn('last_9m_pol_cat', F.when(F.col('last_9m_pol')>0, 'Active').otherwise('Inactive'))

tpolidm_df = spark.read.parquet(f'{dm_path}TPOLIDM_DAILY')
tpolidm_df.createOrReplaceTempView('tpolidm')

#tpardm_df.groupBy('tier').agg(F.count(F.col('agt_cd')).alias('no_agents')).display()
tagtdm_df = spark.read.parquet(f'{dm_path}TAGTDM_DAILY/')
tagtdm_df.createOrReplaceTempView('tagtdm')
agt_string = '''
select  distinct
        tagtdm.agt_code, tagtdm.loc_cd loc_code, mdrt_desc, 
        CASE tagtdm.mpro_title
                WHEN 'P' THEN 'Platinum'
                WHEN 'G' THEN 'Gold'
                WHEN 'S' THEN 'Silver'
        END AS mpro_title,
        CASE WHEN tagtdm.mpro_title IS NOT NULL THEN
            CASE WHEN mdrt_desc = 'TOT' THEN 1
                 WHEN mdrt_desc = 'COT' THEN 2
                 WHEN mdrt_desc = 'MDRT' THEN 3
                 WHEN tagtdm.mpro_title = 'P' THEN 4
                 WHEN tagtdm.mpro_title = 'G' THEN 5
                 WHEN tagtdm.mpro_title = 'S' THEN 6
                 ELSE 7
            END
            ELSE 7
        END as tier_rank,
        CASE
                WHEN tagtdm.trmn_dt IS NOT NULL
                AND tagtdm.comp_prvd_num IN ('01', '04', '97', '98') THEN '5.Terminated'
                WHEN tagtdm.trmn_dt IS NOT NULL                         THEN '5.Terminated'
                WHEN tagtdm.comp_prvd_num = '01'                        THEN '1.Inforce'
                WHEN tagtdm.comp_prvd_num = '04'                        THEN '2.Collector'
                WHEN tagtdm.comp_prvd_num = '08'                        THEN '3.GA'
                WHEN tagtdm.comp_prvd_num IN ('97', '98')               THEN '4.SM'
                ELSE '5.Unknown'
        END AS agt_rltnshp,
        coalesce(loc.manager_code_0, loc.manager_code_1, loc.manager_code_2, loc.manager_code_3, loc.manager_code_4, loc.manager_code_5, loc.manager_code_6, 'Empty') sm_code,
        coalesce(loc.manager_name_0, loc.manager_name_1, loc.manager_name_2, loc.manager_name_3, loc.manager_name_4, loc.manager_name_5, loc.manager_name_6, 'Empty') sm_name,
        rh_name 
from    tagtdm inner join
        vn_published_ams_db.tams_agents agt on tagtdm.agt_code=agt.agt_code left join
        vn_curated_reports_db.loc_to_sm_mapping loc on agt.loc_code=loc.loc_cd
where   channel='Agency'
    and tagtdm.comp_prvd_num in ('01', '04', '08', '97', '98')
'''
agt_stat = sql_to_df(agt_string, 1, spark)

agt_merged = agt_stat.alias(
        'agt'
).join(
        agt_tier.alias('tier'), 
        on='agt_code', 
        how='left'
).withColumn(
        'tier', 
        F.when(F.col('mdrt_desc').isNotNull(), F.col('mdrt_desc'))
        .when(F.col('mpro_title').isNotNull(), F.col('mpro_title'))
        .otherwise(F.col('tier'))
).withColumn(
        'agt_status', 
        F.when(F.col('agt_rltnshp').isin('2.Collector','5.Terminated'), 'UCM')
        .when((F.col('tier').isNull()) & (F.col('agt_rltnshp') == '1.Inforce'), 'New')                          
        .otherwise(F.col('last_9m_pol_cat'))
).select('agt_code', 'loc_code', 'agt_status', 'agt_rltnshp', 'tier', 'tier_rank', 'sm_code', 'sm_name', 'rh_name')

#agt_merged.groupBy('agt_status','agt_rltnshp','tier').agg(F.count(F.col('agt_code')).alias('no_agents')).display()
#agt_merged.filter((F.col('agt_status').isNull()) &
#                  (F.col('tier').isNull()) &
#                  (F.col('agt_rltnshp') == '1.Inforce')).display()
#print(agt_merged.count())
agt_pd = agt_merged.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load tables used for calculating Maturity Value

# COMMAND ----------

spark.read.format('parquet').load(mat_val_path).createOrReplaceTempView("mat_plan_code")
spark.read.format('parquet').load(f'{cas_path}TPOL_INVNTY_CTL/').createOrReplaceTempView("tpol_invnty_ctl")
spark.read.format('parquet').load(f'{cas_path}TAVY_VALUE_DTLS/').createOrReplaceTempView("tavy_value_dtls")

# COMMAND ----------

rider_string = '''
with rider_typ as (
select  pol_num
        , case when pln.prod_typ='TP_RIDER' then 'Y' else 'N' end as maturity_tp_rider
        , case when pln.prod_typ='CI_RIDER' then 'Y' else 'N' end as maturity_ci_rider
        , case when pln.prod_typ='MC_RIDER' then 'Y' else 'N' end as maturity_mc_rider
        , case when pln.prod_typ='ADD_RIDER' then 'Y' else 'N' end as maturity_add_rider
        , case when pln.prod_typ='HC_RIDER' then 'Y' else 'N' end as maturity_hc_rider
        , case when pln.prod_typ='TPD_RIDER' then 'Y' else 'N' end as maturity_tpd_rider
from    vn_published_cas_db.tcoverages cvg inner join
        vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code and cvg.vers_num=pln.vers_num
where   cvg.cvg_typ<>'B'
)
select  cvg.pol_num
        ,cvg.ins_typ
        ,max(maturity_tp_rider) as maturity_tp_rider
        ,max(maturity_ci_rider) as maturity_ci_rider
        ,max(maturity_mc_rider) as maturity_mc_rider
        ,max(maturity_add_rider) as maturity_add_rider
        ,max(maturity_hc_rider) as maturity_hc_rider
        ,max(maturity_tpd_rider) as maturity_tpd_rider
from    vn_published_cas_db.tcoverages cvg left join
        rider_typ on cvg.pol_num=rider_typ.pol_num
where   cvg.cvg_typ='B'
group by cvg.pol_num, cvg.ins_typ
'''
riders = sql_to_df(rider_string, 1, spark)
riders.createOrReplaceTempView('riders')

mat_string = '''
  select po_num
        , pol.pol_num maturing_policy
        , case  when pol.PLAN_CODE in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001') then 'Digital'
                when pol.PLAN_CODE in ('CA360','CX360') then 'Cancer360'
                else fld.fld_valu_desc_eng end maturing_product_type
        , pol.plan_code maturing_product
        , case when pol_stat_cd in ('F','H') then concat_ws(' - ', pol_stat_cd, 'Matured')
          else concat_ws(' - ', pol_stat_cd, pol_stat_desc) end as policy_status
        , substr(to_date(pol.xpry_dt),1,7) maturity_month
        , to_date(pol.xpry_dt) maturity_date
        , cast(pmt_mode as int) pmt_mode
        , cast(case when pol_stat_cd='H' then mat_val else greatest(tot_ape, base_ape+rid_ape) end as int) maturity_ape
        , sa_code maturity_serving_agent
        , nvl(maturity_tp_rider,'N') maturity_tp_rider
        , nvl(maturity_ci_rider,'N') maturity_ci_rider
        , nvl(maturity_mc_rider,'N') maturity_mc_rider
        , nvl(maturity_add_rider,'N') maturity_add_rider
        , nvl(maturity_hc_rider,'N') maturity_hc_rider
        , nvl(maturity_tpd_rider,'N') maturity_tpd_rider
  from  tpolidm pol inner join
        riders cvg on pol.pol_num=cvg.pol_num inner join
        vn_published_cas_db.tfield_values fld on cvg.ins_typ=fld.fld_valu and fld.fld_nm='INS_TYP'
  where  months_between(last_day(pol.XPRY_DT), '{lst_mthend}') between 2 and 15   -- Maturity/Expiry year is within 1 year / 12 months
     --and pol.PLAN_CODE not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001','CA360','CX360')  -- Exclude Digital products and CA360
     and pol.PLAN_CODE not like 'EV%'    -- Exclude exchange rate conversion products
     and pol.pol_stat_cd in ('1','2','3','5','7','9','F','H') -- Only premium paying, holiday or matured
'''
maturing_policy = sql_to_df(mat_string, 1, spark)
maturing_policy.createOrReplaceTempView("maturing_policy")

rem_string = '''
  select distinct non.po_num, non.pol_num, non.sa_code
  from    maturing_policy mat left join
          tpolidm non on non.po_num=mat.po_num 
  where   non.pol_num is null or
          mat.maturing_policy <> non.pol_num
'''
remaining_policy = sql_to_df(rem_string, 1, spark)
remaining_policy.createOrReplaceTempView("remaining_policy")

mat_val_string = '''
WITH vpo AS (
    SELECT
        cvg.pol_num
       ,SUM(cvg.face_amt) AS vpo_face_amt
    FROM vn_published_cas_db.tcoverages cvg INNER JOIN 
         vn_curated_campaign_db.vn_plan_code_map pln ON cvg.plan_code = pln.plan_code
    WHERE pln.nbv_factor_group = 'Value Preservation Option'
        AND cvg.cvg_stat_cd NOT IN ('B', 'L')
    GROUP BY cvg.pol_num
), mat as (
SELECT DISTINCT
        pol_num, plan_code, face_amt
FROM    vn_published_cas_db.tcoverages
WHERE   cvg_typ = 'B'
    AND cvg_reasn = 'O'
)
SELECT DISTINCT
    mat.pol_num
   --,mat.face_amt AS base_face_amt
   ,NVL(pln.maturity_benefit_per_FA,0) AS maturity_benefit_per_FA
   --,inv.acum_div_bal --accum dividend balance
   --,inv.csh_cpn_bal --cash coupon balance
   --,avy.lump_sum_bal --lump sum balance
   --,inv.pol_loan_bal --policy loan balance
   --,vpo.vpo_face_amt --sum of VPO face amount
   ,((mat.face_amt + NVL(vpo.vpo_face_amt,0))*pln.maturity_benefit_per_FA) --maturity benefit
    + (NVL(inv.acum_div_bal,0) + NVL(inv.csh_cpn_bal,0) + NVL(avy.lump_sum_bal,0)) --amount on deposit
    - NVL(inv.pol_loan_bal,0) AS maturity_value --policy loan balance
FROM    mat LEFT JOIN 
        tpol_invnty_ctl inv ON mat.pol_num = inv.pol_num LEFT JOIN 
        tavy_value_dtls avy ON mat.pol_num = avy.pol_num LEFT JOIN 
        mat_plan_code   pln ON mat.plan_code = pln.base_plan_code 
                           AND pln.has_maturity_benefit = 1
LEFT JOIN
        vpo ON mat.pol_num = vpo.pol_num
'''
mat_value = sql_to_df(mat_val_string, 1, spark)
#mat_value.createOrReplaceTempView("mat_value")

ape_string = '''
select  po_num, cast(sum(base_ape+rid_ape) as int) cvg_ape
from    tpolidm
where   pol_stat_cd in ('1','2','3','5','7','9','F','H')
group by po_num
'''
mat_ape = sql_to_df(ape_string, 1, spark)
#mat_ape.filter(F.col('po_num')=='2803040742').display()

final_string = '''
select  pol.po_num
        , pol.maturing_policy
        , pol.maturing_product_type
        , pol.maturing_product
        , pol.policy_status
        , pol.maturity_month
        , pol.maturity_date
        , pol.maturity_ape
        , pol.maturity_serving_agent
        , pol.maturity_tp_rider
        , pol.maturity_ci_rider
        , pol.maturity_mc_rider
        , pol.maturity_add_rider
        , pol.maturity_hc_rider
        , pol.maturity_tpd_rider
        , rem.pol_num oth_pol_num
        , rem.sa_code oth_serving_agent
        , cvg.plan_code oth_plan_code
        , cvg.cvg_typ
        , fld.fld_valu_desc_eng as need_type
        , pln.prod_typ as product_type
from    maturing_policy pol left join
        remaining_policy rem on pol.po_num=rem.po_num left join
        vn_published_cas_db.tcoverages cvg on rem.pol_num=cvg.pol_num left join
        vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code and cvg.vers_num=pln.vers_num left join
        vn_published_cas_db.tfield_values fld on cvg.ins_typ=fld.fld_valu and fld.fld_nm='INS_TYP'
where   1=1
    and (pln.prod_typ is null or
         pln.prod_typ not in ('WOP_RIDER','WOD_RIDER','PW_RIDER','WOC_RIDER')) -- Exclude waiver riders
'''
result = sql_to_df(final_string, 1, spark)
#print(result.count())
#result.filter(F.col("maturing_policy")=='2801679331').display()

# COMMAND ----------

#result = result.join(ape, on='po_num', how='left').dropDuplicates()
#final = final.drop('agt_code')
ape = mat_ape.toPandas()
value = mat_value.toPandas()
final = result.toPandas()
final.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group everything by maturing policy

# COMMAND ----------

final_pd = final.copy()

# 1. Lower case all values from columns "need_type" and "product_type"
final_pd['need_type'] = final_pd['need_type'].str.lower()
final_pd['product_type'] = final_pd['product_type'].str.lower()

# 2. If "cvg_typ" <> "B" then replace the value in "need_type" with the value from "product_type"
final_pd.loc[final_pd['cvg_typ'] != 'B', 'need_type'] = final_pd['product_type']

# 2.5 If "need_type" is null then set it to "legacy"
final_pd['need_type'].fillna('add_rider', inplace=True)

# 3. For each distinct value in "need_type", create/add a new column with the header names being these values
new_columns = final_pd['need_type'].unique()
for col in new_columns:
    final_pd[col] = 0

# 4. Set 1 to the appropriate column newly created, otherwise 0 to all others
for col in new_columns:
    final_pd.loc[final_pd['need_type'] == col, col] = 1

# 5. Group the data by specified columns and sum each and all of the newly created columns
grouped_data = final_pd.groupby(['po_num', 'maturing_policy', 'maturing_product_type', 'maturing_product', 'policy_status', 'maturity_month', 'maturity_date', 'maturity_ape', 'maturity_serving_agent', 'maturity_tp_rider', 'maturity_ci_rider', 'maturity_mc_rider', 'maturity_add_rider', 'maturity_hc_rider', 'maturity_tpd_rider']).sum().reset_index()

print("Number of maturity customers: ", grouped_data.shape[0])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge with Agent information

# COMMAND ----------

# Add APE to "po_num"
grouped_data = grouped_data.merge(ape, how='left', on='po_num')

# Fill null values with 0s
grouped_data['cvg_ape'] = grouped_data['cvg_ape'].fillna(0)

# Add maturity value to "pol_num"
grouped_data = grouped_data.merge(value, how='left', left_on='maturing_policy', right_on='pol_num')

# Fill null values with 0s
grouped_data['maturity_value'] = grouped_data['maturity_value'].fillna(0)

# Add serving agent to "pol_num"
grouped_data = grouped_data.merge(agt_pd, how='left', left_on='maturity_serving_agent', right_on='agt_code')
grouped_data = grouped_data.drop(columns={'agt_code'})

# Print out the result / or write to ADLS as csv file
#grouped_data.to_csv(f'/dbfs/mnt/lab/vn/project/cpm/Adhoc/maturity_policies_dtl_w_excl.csv', index=False, header=True, encoding='utf-8-sig')
grouped_data.to_parquet(f'/dbfs/mnt/lab/vn/project/cpm/Adhoc/Maturity/maturity_policies_dtl_w_excl_{mthend_sht}.parquet')
#display(grouped_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation

# COMMAND ----------

grouped_data[grouped_data['maturing_policy'] == '2801289743']
