# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

import pyspark.sql.functions as F
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

# Change x to the number of months to which we go back
x = 0
run_date = datetime.now().replace(day=1) #.strftime('%Y%m%d')
x_month = run_date - relativedelta(months=x)
lst_mthend = (x_month - timedelta(days=1)).strftime('%Y-%m-%d')
mthend_sht = lst_mthend[:7]

dm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
#cseg_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/INCOME_BASED_DECILE_AGENCY/image_date={lst_mthend}'
#life_path = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/CUST_LIFESTAGE/monthend_dt={mthend_sht}'

# COMMAND ----------

tpardm_df = spark.read.parquet(f'/mnt/lab/vn/project/cpm/datamarts/TPARDM_MTHEND/image_date={lst_mthend}')
tpardm_df = tpardm_df.select(F.col('agt_cd').alias('agt_code'),'rank_code','tier','last_9m_pol').dropDuplicates()
agt_tier = tpardm_df.withColumn('last_9m_pol_cat', F.when(F.col('last_9m_pol')>0, 'Active').otherwise('Inactive'))

tpolidm_df = spark.read.parquet(f'{dm_path}TPOLIDM_MTHEND/image_date={lst_mthend}')
tpolidm_df.createOrReplaceTempView('tpolidm')

#tpardm_df.groupBy('tier').agg(F.count(F.col('agt_cd')).alias('no_agents')).display()
tagtdm_df = spark.read.parquet(f'{dm_path}TAGTDM_MTHEND/image_date={lst_mthend}')
tagtdm_df.createOrReplaceTempView('tagtdm')
agt_string = '''
select  distinct
        tagtdm.agt_code, tagtdm.loc_cd loc_code, mdrt_desc, 
        CASE tagtdm.mpro_title
                WHEN 'P' THEN 'Platinum'
                WHEN 'G' THEN 'Gold'
                WHEN 'S' THEN 'Silver'
        END AS mpro_title,
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
        vn_published_casm_ams_snapshot_db.tams_agents agt on tagtdm.agt_code=agt.agt_code and agt.image_date='{lst_mthend}' left join
        vn_curated_reports_db.loc_to_sm_mapping_hist loc on agt.loc_code=loc.loc_cd and agt.image_date=loc.image_date
where   channel='Agency'
    and tagtdm.comp_prvd_num in ('01', '04', '08', '97', '98')
'''
agt_stat = sql_to_df(agt_string, 1, spark)
#agt_stat.groupBy('agt_rltnshp').agg(F.count(F.col('agt_code')).alias('no_agents')).display()
agt_merged = agt_stat.alias('agt')\
                .join(agt_tier.alias('tier'), 
                      on='agt_code', how='left')\
                .withColumn('tier', 
                            F.when(F.col('mdrt_desc').isNotNull(), F.col('mdrt_desc'))
                            .when(F.col('mpro_title').isNotNull(), F.col('mpro_title'))
                            .otherwise(F.col('tier')))\
                .withColumn('agt_status', 
                            F.when(F.col('agt_rltnshp').isin('2.Collector','5.Terminated'), 'UCM')
                            .when((F.col('tier').isNull()) & (F.col('agt_rltnshp') == '1.Inforce'), 'New')                          
                            .otherwise(F.col('last_9m_pol_cat')))\
                .select('agt_code', 'loc_code', 'agt_status', 'agt_rltnshp', 'tier', 'sm_code', 'sm_name', 'rh_name')

#agt_merged.groupBy('agt_status','agt_rltnshp','tier').agg(F.count(F.col('agt_code')).alias('no_agents')).display()
#agt_merged.filter((F.col('agt_status').isNull()) &
#                  (F.col('tier').isNull()) &
#                  (F.col('agt_rltnshp') == '1.Inforce')).display()
#print(agt_merged.count())
agt_pd = agt_merged.toPandas()

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
from    vn_published_casm_cas_snapshot_db.tcoverages cvg inner join
        vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code and cvg.vers_num=pln.vers_num
where   cvg.cvg_typ<>'B'
    and cvg.image_date='{lst_mthend}'
)
select  cvg.pol_num
        ,cvg.ins_typ
        ,max(maturity_tp_rider) as maturity_tp_rider
        ,max(maturity_ci_rider) as maturity_ci_rider
        ,max(maturity_mc_rider) as maturity_mc_rider
        ,max(maturity_add_rider) as maturity_add_rider
        ,max(maturity_hc_rider) as maturity_hc_rider
        ,max(maturity_tpd_rider) as maturity_tpd_rider
from    vn_published_casm_cas_snapshot_db.tcoverages cvg left join
        rider_typ on cvg.pol_num=rider_typ.pol_num
where   cvg.cvg_typ='B'
    and cvg.image_date='{lst_mthend}'
group by cvg.pol_num, cvg.ins_typ
'''
riders = sql_to_df(rider_string, 1, spark)
riders.createOrReplaceTempView('riders')

mat_string = '''
  select po_num
        , pol.pol_num maturing_policy
        , case  when pol.PLAN_CODE in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001') then 'Digital'
                when pol.PLAN_CODE = 'CA360' then 'Cancer360'
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
  where  1=1
     --and year(pol.XPRY_DT)=2024  -- Maturity/Expiry year is 2024
     --and pol.PLAN_CODE not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001','CA360')  -- Exclude Digital products and CA360
     and pol.PLAN_CODE not like 'EV%'    -- Exclude exchange rate conversion products
     and pol.pol_stat_cd in ('1','2','3','5','7','9') -- Only premium paying or holiday
'''
maturing_policy = sql_to_df(mat_string, 1, spark)
#print(maturing_policy.count())
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

ape_string = '''
select  po_num, cast(sum(base_ape+rid_ape) as int) cvg_ape
from    tpolidm
where   pol_stat_cd in ('1','2','3','5','7','9')
group by po_num
'''
ape = sql_to_df(ape_string, 1, spark)
#ape.createOrReplaceTempView("ape")

final_string = '''
select  pol.po_num
        , max(pol.maturity_tp_rider) tp_rider_ind
        , max(pol.maturity_ci_rider) ci_rider_ind
        , max(pol.maturity_mc_rider) mc_rider_ind
        , max(pol.maturity_add_rider) add_rider_ind
        , max(pol.maturity_hc_rider) hc_rider_ind
        , max(pol.maturity_tpd_rider) tpd_rider_ind
from    maturing_policy pol left join
        remaining_policy rem on pol.po_num=rem.po_num left join
        vn_published_cas_db.tcoverages cvg on rem.pol_num=cvg.pol_num left join
        vn_published_cas_db.tplans pln on cvg.plan_code=pln.plan_code and cvg.vers_num=pln.vers_num left join
        vn_published_cas_db.tfield_values fld on cvg.ins_typ=fld.fld_valu and fld.fld_nm='INS_TYP'
where   1=1
    and (pln.prod_typ is null or
         pln.prod_typ not in ('WOP_RIDER','WOD_RIDER','PW_RIDER','WOC_RIDER')) -- Exclude waiver riders
group by
        pol.po_num
'''
result = sql_to_df(final_string, 1, spark)
#print("# of records: ", result.count(), ", # of POs: ", result.select("po_num").distinct().count())
#result.filter(F.col("po_num") == "2800558502").display()

# COMMAND ----------

#result = result.join(ape, on='po_num', how='left').dropDuplicates()
#final = final.drop('agt_code')
ape = ape.toPandas()
final = result.toPandas()
print(final.shape)
final.to_csv(f"/dbfs/mnt/lab/vn/project/cpm/LEGO/customer_rider_holding.csv", index=False, header=True, encoding='utf-8-sig')
