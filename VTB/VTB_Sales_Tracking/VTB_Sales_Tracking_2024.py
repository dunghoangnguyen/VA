# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

from pyspark.sql import Window
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType
from pyspark.sql.types import IntegerType, DateType, LongType
from datetime import datetime, timedelta, date
import calendar

image_date='2024-04-30'

src_path1 = f'/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
src_path2 = f'/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
src_path3 = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
src_path4 = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAMPAIGN_FILEBASED_DB/'
in_path = f'/mnt/lab/vn/project/cpm/VTB/batch_all_2024.csv' # batch1_2024.csv


# COMMAND ----------


tpolidm_df = spark.read.parquet(f'{src_path1}TPOLIDM_DAILY/')
tagtdm_df = spark.read.parquet(f'{src_path1}TAGTDM_DAILY/')
tporidm_df = spark.read.parquet(f'{src_path1}TPORIDM_DAILY/')
plncode_df = spark.read.parquet(f'{src_path2}VN_PLAN_CODE_MAP/')
nbvhis_df  = spark.read.parquet(f'{src_path4}NBV_MARGIN_HISTORIES/').dropDuplicates()
vtb_leads_df = spark.read.csv(f'{in_path}', header=True)
control = spark.read.parquet(
'/mnt/lab/vn/project/scratch/banca_profile/vtb_trigger_income_ape/control', header=True
)

df_list = [tpolidm_df, tagtdm_df, tporidm_df, plncode_df, nbvhis_df]

# Loop through each DataFrame and convert column names to lowercase
for i, df in enumerate(df_list):
    df_list[i] = df.toDF(*[col.lower() for col in df.columns])

# Unpack the modified DataFrames back to their original names
tpolidm_df, tagtdm_df, tporidm_df, plncode_df, nbvhis_df = df_list

#tpol_cols = ['pol_num', 'plan_code_base', 'pol_iss_dt', 'pol_stat_cd', 'dist_chnl_cd', 'pmt_mode', 'mode_prem', 'last_pd_to_dt',
#             'renw_yr']
#tcov_cols = ['pol_num', 'pd_to_dt', 'trxn_amt', 'eff_dt', 'trxn_cd', 'trxn_desc', 'trxn_id', 'undo_trxn_id', 'reasn_code']
#tcus_cols = ['pol_num', 'po_num']
#tpln_cols = ['cli_num', 'sms_ind', 'email_addr']

#tpol_df = tpolidm_df.select(*tpol_cols).dropDuplicates()
#tcov_df = tporidm_df.select(*tcov_cols).dropDuplicates()
#tcus_df  = tpolidm_df.select(*tcus_cols).dropDuplicates()
#tpln_df = tcustdm_df.select(*tpln_cols).dropDuplicates()

# COMMAND ----------

tpolidm_df.createOrReplaceTempView('tpolidm_daily')
tagtdm_df.createOrReplaceTempView('tagtdm_daily')
tporidm_df.createOrReplaceTempView('tporidm_daily')
plncode_df.createOrReplaceTempView('vn_plan_code_map')
nbvhis_df.createOrReplaceTempView('nbv_margin_histories')
vtb_leads_df.createOrReplaceTempView('vtb_leads')
control.createOrReplaceTempView('control')

# COMMAND ----------

vtb_lead_details_df = spark.sql(f"""
select  distinct 
        p.po_num
        , p.insrd_num
        , date_add(last_day(add_months('{image_date}', -1)), 1) as reass_dt
        , p.sa_code agt_code
        , case when l.in_scope='Y' then 'Target' else 'Non-target' end as lead_grp
from    vtb_leads l inner join
        tpolidm_daily p on l.pol_num=p.pol_num     
union   
select  distinct
        l.po_num
        , p.insrd_num
        , date_add(last_day(add_months('{image_date}', -1)), 1) as reass_dt
        , p.sa_code agt_code
        , 'Control' as lead_grp
from    control l inner join
        tpolidm_daily p on l.po_num=p.po_num  
""")
vtb_lead_details_df.createOrReplaceTempView('lead_details')
#vtb_lead_details_df.limit(20).display()

vtb_sales_rslt_df = spark.sql("""
select  l.po_num
        , l.agt_code
        , p.pol_num
        , c.plan_code
        , p.wa_code
        , cast(c.cvg_prem as float) cvg_prem
        , case 
            when a.loc_cd is null then 
			    (case 
                    when DIST_CHNL_CD in ('48') then round(c.cvg_prem*n.nbv_margin_other_channel_affinity,2)--'Affinity'
                    when DIST_CHNL_CD in ('01','02','08','50','*') then round(c.cvg_prem*n.nbv_margin_agency,2)--'Agency'
                    when DIST_CHNL_CD in ('05','06','07','34','36') then round(c.cvg_prem*n.nbv_margin_dmtm,2)--'DMTM'
                    when DIST_CHNL_CD in ('09') then round(c.cvg_prem*-1.34041044648343,2)--'MI'
                    else round(c.cvg_prem*0.331369,2)--'Banca'
                end)
			when a.loc_cd like 'TCB%' then round(c.cvg_prem*n.nbv_margin_banca_tcb,2) --'TCB'
			when a.loc_cd like 'SAG%' then round(c.cvg_prem*n.nbv_margin_banca_scb,2) --'SCB'
            when a.loc_cd like 'VTI%' then round(c.cvg_prem*nvl(n.nbv_margin_banca_vti,0.331369),2) --'VTB'
		else round(c.cvg_prem*nvl(n.nbv_margin_agency,0.331369),2) 
        end as nbv --'Agency
        , n.effective_date nbv_effective_date
		, case
			when DIST_CHNL_CD in ('01','02','08','50','*')
			then 'Agency'
			when DIST_CHNL_CD IN ('05','06','07','34','36')
			then 'DMTM'
			else 'Banca'
		end as channel
        , to_date(cvg_eff_dt) cvg_eff_dt
        , substr(to_date(cvg_eff_dt),1,7) reporting_month
        , case when l.agt_code = p.wa_code then 'Y' else 'N' end as same_agt_ind
        , l.lead_grp
from    lead_details l inner join
        tpolidm_daily p on l.po_num = p.po_num inner join
		tporidm_daily c on p.pol_num = c.pol_num inner join 
		tagtdm_daily a on p.wa_code = a.agt_code left join
        nbv_margin_histories n on n.plan_code=c.plan_code and floor(months_between(c.cvg_eff_dt,n.effective_date)) between 0 and 2 left join
        vn_plan_code_map b on c.plan_code=b.plan_code
where   c.cvg_eff_dt > l.reass_dt   -- issued after reassignment date
    and c.cvg_eff_dt between date_add(last_day(add_months(current_date(), -2)), 1) and last_day(add_months(current_date(), -1))
union
select  l.po_num
        , l.agt_code
        , p.pol_num
        , c.plan_code
        , p.wa_code
        , cast(c.cvg_prem as float) cvg_prem
        , case 
            when a.loc_cd is null then 
			    (case 
                    when DIST_CHNL_CD in ('48') then round(c.cvg_prem*n.nbv_margin_other_channel_affinity,2)--'Affinity'
                    when DIST_CHNL_CD in ('01','02','08','50','*') then round(c.cvg_prem*n.nbv_margin_agency,2)--'Agency'
                    when DIST_CHNL_CD in ('05','06','07','34','36') then round(c.cvg_prem*n.nbv_margin_dmtm,2)--'DMTM'
                    when DIST_CHNL_CD in ('09') then round(c.cvg_prem*-1.34041044648343,2)--'MI'
                    else round(c.cvg_prem*0.331369,2)--'Banca'
                end)
			when a.loc_cd like 'TCB%' then round(c.cvg_prem*n.nbv_margin_banca_tcb,2) --'TCB'
			when a.loc_cd like 'SAG%' then round(c.cvg_prem*n.nbv_margin_banca_scb,2) --'SCB'
            when a.loc_cd like 'VTI%' then round(c.cvg_prem*nvl(n.nbv_margin_banca_vti,0.331369),2) --'VTB'
		else round(c.cvg_prem*nvl(n.nbv_margin_agency,0.331369),2) 
        end as nbv --'Agency'
        , n.effective_date nbv_effective_date
		, case
			when DIST_CHNL_CD in ('01','02','08','50','*')
			then 'Agency'
			when DIST_CHNL_CD IN ('05','06','07','34','36')
			then 'DMTM'
			else 'Banca'
		end as channel
        , to_date(cvg_eff_dt) cvg_eff_dt
        , substr(to_date(cvg_eff_dt),1,7) reporting_month
        , case when l.agt_code = p.wa_code then 'Y' else 'N' end as same_agt_ind
        , l.lead_grp
from    lead_details l inner join
        tpolidm_daily p on l.insrd_num = p.po_num inner join
		tporidm_daily c on p.pol_num = c.pol_num inner join 
		tagtdm_daily a on p.wa_code = a.agt_code left join
        nbv_margin_histories n on n.plan_code=c.plan_code and floor(months_between(c.cvg_eff_dt,n.effective_date)) between 0 and 2 left join
        vn_plan_code_map b on c.plan_code = b.plan_code
where   c.cvg_eff_dt >= l.reass_dt   -- issued after batch's launch date
    and c.cvg_eff_dt between date_add(last_day(add_months(current_date(), -2)), 1) and last_day(add_months(current_date(), -1))
""")
vtb_sales_rslt_df.createOrReplaceTempView('sales_rslt')

vtb_rslt_sum_df = spark.sql("""
select  l.lead_grp
        , min(l.reass_dt) batch_start_date
        , max(reporting_month) as reporting_month
        , count(distinct l.po_num) total_leads_distributed
        --, count(distinct l.agt_code) no_agt_cnt
        --, cast(count(distinct case when s.same_agt_ind='Y' then s.wa_code end) /
        --  count(distinct s.wa_code) as decimal(4,2)) as same_agt_ratio
        , count(distinct s.po_num) customers_with_takeup
        , count(s.pol_num) products_sold
        , cast(count(distinct s.po_num)*100 / count(distinct l.po_num) as decimal(4,2)) conversion_rate
        , cast(sum(s.cvg_prem)/23.145 as decimal(12,2)) campaign_ape_usd
        , cast(sum(case when nvl(s.nbv,0)=0 then s.cvg_prem*0.331369 else s.nbv end)/23.145 as decimal(12,2)) campaign_nbv_usd
        , count(distinct s.pol_num) new_case_count
from    lead_details l left join
        sales_rslt s on l.po_num = s.po_num
group by l.lead_grp
order by l.lead_grp desc
""")

vtb_rslt_sum_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### List out details of leads' sales

# COMMAND ----------

#vtb_sales_rslt_df.display()
