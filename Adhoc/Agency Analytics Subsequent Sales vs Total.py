# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# Import the necessary functions from the pyspark.sql module
from pyspark.sql import functions as F
import pandas as pd
# Declare string date
min_date_str='2023-01-01'
max_date_str='2024-01-31'
yymm        =max_date_str[2:4]+max_date_str[5:7]

# Specify the path to the data mart folder
ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_AMS_SNAPSHOT_DB/'
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'
#dm_path  = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
sc_path  = '/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'
rpt_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
lab_path = '/mnt/lab/vn/project/scratch/ORC_ACID_to_Parquet_Conversion/apps/hive/warehouse/vn_processing_datamart_temp_db.db/'

out_path = '/dbfs/mnt/lab/vn/project/scratch/adhoc/'

# Define the names of the tables
tbl1 = 'tcoverages'
tbl2 = 'tpolicys'
tbl3 = 'tams_agents'
tbl4 = 'customers_table_mthly'
tbl5 = 'agent_scorecard'
tbl6 = 'loc_to_sm_mapping_hist'
tbl7 = 'tfield_values'
tbl8 = 'vn_plan_code_map'
tbl9 = 'tclient_policy_links'

# Create a list containing the data mart path
path_list = [#ams_path, 
             cas_path, cpm_path,
             sc_path, rpt_path, lab_path]

# Create a list containing the table names
tbl_list = [tbl1, tbl2, #tbl3, 
            tbl4,
            tbl5, tbl6, tbl7, tbl8,
            tbl9]

# Load the parquet files into a dictionary of DataFrames
df_list = load_parquet_files(path_list, tbl_list)

# Iterate over the tables in the df_list dictionary
for tbl_name in df_list:
    # Retrieve the DataFrame for the current table
    df = df_list[tbl_name]

    # Filter the DataFrame to keep only rows where the 'image_date' column is equal to 'max_date_str'
    if "image_date".upper() in (col_name.upper() for col_name in df.columns):
        df = df.filter(F.col('image_date') == F.to_date(F.lit(max_date_str)))
    elif "reporting_date".upper() in (col_name.upper() for col_name in df.columns):
        df = df.filter(F.col('reporting_date') >= F.to_date(F.lit(min_date_str)))
    elif "monthend_dt".upper()in (col_name.upper() for col_name in df.columns):
        df = df.filter(F.col('monthend_dt') == F.to_date(F.lit(max_date_str)))
    print("Count of records in", tbl_name, ":", df.count())
    
    # Update the DataFrame in the df_list dictionary with the filtered DataFrame
    df_list[tbl_name] = df

spark.read.parquet(f'{ams_path}TAMS_AGENTS/image_date={max_date_str}').createOrReplaceTempView('tams_agents')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Intermediate data

# COMMAND ----------

# Generate temporary views for each DataFrame in the df_list dictionary
generate_temp_view(df_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare working dataset

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Denominator metrics</strong>

# COMMAND ----------

denom_df = spark.sql(f"""
select  --concat(substr(reporting_date,1,4),substr(reporting_date,6,2)) as rpt_mth
        reporting_date as rpt_mth
        ,count(po_num) as denom_inf_cus_cnt
        ,cast(sum(tot_ape) as bigint) as denom_inf_total_ape
from    customers_table_mthly
where   channel='Agency'
group by 
        reporting_date
order by rpt_mth        
""")

denom_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Active list of Agency customers</strong>

# COMMAND ----------

sql_string = """
select distinct tcpl.cli_num po_num
from    tpolicys tpol inner join
        tclient_policy_links tcpl on tpol.pol_num = tcpl.pol_num and tcpl.link_typ='O'
where   tpol.dist_chnl_cd in ('01', '02', '08', '50', '*')    
    and tpol.pol_stat_cd in ('1','2','3','5','7')
"""
acus_df = sql_to_df(sql_string, 0, spark)
print("Number of rows:     ", acus_df.count())
print("Number of total PO: ", acus_df.distinct().count())
acus_df.createOrReplaceTempView('acus')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>First policiy sold</strong>

# COMMAND ----------

sql_string = """
    select  po_num, pol_num as frst_pol_num, agt_cd, to_date(pol_eff_dt) as frst_pol_eff_dt
    from    (select  tcpl.cli_num po_num, tpol.pol_num, tpol.wa_cd_1 agt_cd, tpol.pol_eff_dt, 
                     case when tpol.dist_chnl_cd in ('03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53') then 'Banca'
                          when tpol.dist_chnl_cd in ('01', '02', '08', '50', '*') then 'Agency'
                          else 'Others'
                     end as channel,
                     row_number() over (partition by tcpl.cli_num order by tpol.pol_eff_dt, tpol.pol_num) rn
             from    tpolicys tpol inner join
                     tclient_policy_links tcpl on tpol.pol_num = tcpl.pol_num and tcpl.link_typ='O'
             where   tpol.dist_chnl_cd in ('01', '02', '08', '50', '*')    
                 and tpol.pol_stat_cd not in ('8','A','N','R')
            ) t
    where   rn=1
"""
fpol_df = sql_to_df(sql_string, 0, spark)
print("Number of rows:     ", fpol_df.count())
print("Number of total PO: ", fpol_df.dropDuplicates(['po_num']).count())
fpol_df.limit(2).display()
fpol_df.createOrReplaceTempView('fpol')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Retrieve all sales past X months</strong>

# COMMAND ----------

sql_string = """
select  tcpl.cli_num po_num,
        tpol.pol_num,
        tpol.plan_code_base plan_code,
        tpol.wa_cd_1 agt_cd,
        to_date(tpol.sbmt_dt) sbmt_dt,
        to_date(tcov.cvg_eff_dt) cvg_eff_dt,
        tcov.cvg_typ,
        cast(tcov.cvg_prem*cast(tpol.pmt_mode as int)/12 as decimal(18,2)) ape,
        nvl(tfld.fld_valu_desc,'') prd_typ,
        case when tpln.nbv_factor_group='Value Preservation Option' then 'VPO'
             else nvl(substring_index(tpln.nbv_factor_group, ' -', 1),'') 
        end as rid_typ
from    tcoverages tcov inner join
        tpolicys tpol on tcov.pol_num = tpol.pol_num inner join
        tclient_policy_links tcpl on tpol.pol_num = tcpl.pol_num and tcpl.link_typ='O' and tcpl.rec_status='A' left join
        tfield_values tfld on tpol.ins_typ_base = tfld.fld_valu and tfld.fld_nm='INS_TYP' and tfld.rec_status='A' left join
        vn_plan_code_map tpln on tcov.plan_code = tpln.plan_code
where   to_date(tcov.cvg_eff_dt) between '{min_date_str}' and '{max_date_str}'
    and tpol.dist_chnl_cd in ('01', '02', '08', '50', '*')    
    and tpol.pol_stat_cd not in ('8','A','N','R')
"""
nsales_df = sql_to_df(sql_string, 1, spark)
print("Number of coverages:", nsales_df.count())
print("Number of policies: ", nsales_df.dropDuplicates(['pol_num']).count())
print("Number of total PO: ", nsales_df.dropDuplicates(['po_num']).count())
nsales_df.limit(5).display()
nsales_df.createOrReplaceTempView('nsales')

# COMMAND ----------

# MAGIC %md
# MAGIC </strong>Identify sales from New and Existing</strong>

# COMMAND ----------

sql_string = """
select  acus.po_num,
        fpol.agt_cd frst_agt_cd,
        case when nsales.agt_cd is not null and fpol.agt_cd=nsales.agt_cd then 'Y' else 'N' end same_agt_ind,
        fpol.frst_pol_eff_dt,
        nsales.pol_num new_pol_num,
        nsales.po_num new_po_num,
        nsales.plan_code,
        nsales.sbmt_dt,
        nsales.cvg_eff_dt,
        concat(substr(nsales.cvg_eff_dt,1,4),substr(nsales.cvg_eff_dt,6,2)) rpt_mth,
        case when nsales.pol_num is not null then 
                case when nsales.sbmt_dt>fpol.frst_pol_eff_dt then 1 
                     else 0
                end
            else 0
        end repeat_ind,
        nsales.agt_cd,
        case when nsales.pol_num is not null then datediff(nsales.sbmt_dt,fpol.frst_pol_eff_dt) else -999 end days_repeat,
        nsales.cvg_typ,
        nvl(nsales.ape,0) ape,
        case when nsales.cvg_typ='B' then nsales.ape else 0 end new_base_ape,
        case when nsales.cvg_typ in ('R','W') then nsales.ape else 0 end new_rider_ape,
        nvl(nsales.prd_typ,'N/A') prd_typ,
        nvl(nsales.rid_typ,'N/A') rid_typ
from    acus inner join
        fpol on acus.po_num=fpol.po_num left join
        nsales on fpol.po_num=nsales.po_num --and nsales.sbmt_dt>fpol.frst_pol_eff_dt
"""

tsales_df = sql_to_df(sql_string, 0, spark)

tsales_df = tsales_df.withColumn('cus_typ', F.when(F.col('repeat_ind')==1, '2. Existing').otherwise('1. New')) \
                     .withColumn('repeat_mth', 
                        F.when(F.col('days_repeat').between(1,   30), '1. <=1mo').otherwise(
                        F.when(F.col('days_repeat').between(31,  60), '2. 1-2mo').otherwise(
                        F.when(F.col('days_repeat').between(61,  90), '3. 2-3mo').otherwise(
                        F.when(F.col('days_repeat').between(91, 180), '4. 3-6mo').otherwise(
                        F.when(F.col('days_repeat').between(181,365), '5. 6-12mo').otherwise(
                        F.when(F.col('days_repeat')>365, '6. More than 12mo') \
                                .otherwise('9. None')))))))
#print("Number of rows:     ", tsales_df.count())
#print("Number of sales:    ", tsales_df.dropDuplicates(['new_pol_num']).count())
#print("Number of new PO:   ", tsales_df.dropDuplicates(['new_po_num']).count())
#print("Number of total PO: ", tsales_df.dropDuplicates(['po_num']).count())
tsales_df.filter(F.col('repeat_mth')!='9. None').limit(5).display()
#tsales_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in tsales_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Agents' related information</strong>

# COMMAND ----------

loc = spark.sql('''
SELECT	*
FROM    loc_to_sm_mapping_hist
''').dropDuplicates(['loc_cd'])

loc.createOrReplaceTempView('loc')

# COMMAND ----------

sql_string = """
with sc as (
    select distinct a.agt_code, a.agent_tier
    from    agent_scorecard a inner join
            (select agt_code, max(monthend_dt) lst_sc_dt
             from   agent_scorecard
             group by agt_code
            ) b on a.agt_code=b.agt_code and a.monthend_dt=b.lst_sc_dt
),
um as (
    SELECT	DISTINCT
				agt.AGT_CODE AGT_CODE,
				grp.STRU_GRP_NM UM_NM,
				grp.MGR_CD UM_CD,
				mgr.RANK_CD UM_LEVEL
		 FROM 	hive_metastore.vn_published_ams_db.TAMS_AGENTS agt
			INNER JOIN
				    hive_metastore.vn_published_ams_db.TAMS_STRU_GROUPS grp
			 ON	agt.UNIT_CODE=grp.STRU_GRP_CD
			INNER JOIN
				    hive_metastore.vn_published_ams_db.TAMS_AGENTS mgr
			 ON	grp.MGR_CD=mgr.AGT_CODE
    WHERE	agt.COMP_PRVD_NUM IN ('01','97','98')
        AND	mgr.RANK_CD IN ('AM','BM','PM','DM','SDM','UM','SUM')
)
select  tagt.agt_code agt_cd,
        case when tagt.stat_cd = '01' then '1. Active'
             when tagt.stat_cd = '99' then '2. Terminated'
        end agt_status,
        case when tagt.mdrt_desc  is NOT NULL then '1. MDRT'
             when tagt.mpro_title is NOT NULL then '2. MPro'
                                              else '9. Others'
        end agt_segment,
        case when tagt.mdrt_desc  is NOT NULL then tagt.mdrt_desc
             when tagt.mpro_title is NOT NULL then concat_ws('_','MPro',tagt.mpro_title) 
             else nvl(sc.agent_tier,'N/A')
        end agt_rank,
        tagt.rank_cd,
        datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25 yr_at_wrk,
        case when floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) <= 1 then '1. <=1yr'
             when floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) > 1 and 
                  floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) <=2 then '2. 1-2yr'
             when floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) > 2 and 
                  floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) <=3 then '3. 2-3yr'
             when floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) > 3 and 
                  floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) <=5 then '4. 3-5yr'
             when floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) > 5 and
                  floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) <=10 then '5. 5-10yr'
             when floor(datediff('{max_date_str}',to_date(tagt.cntrct_eff_dt))/365.25) >10 then '6. >10yr'
        end tenure,
        nvl(tagt.loc_code,'N/A') loc_code,
        nvl(um_cd,'N/A') um_code,
        coalesce(manager_code_0,manager_code_1,manager_code_2,manager_code_3,manager_code_4,manager_code_5,manager_code_6,'N/A') sm_code,
        nvl(loc.rh_name,'N/A') rh_name
from    tams_agents tagt left join
        sc on tagt.agt_code=sc.agt_code left join
        um on tagt.agt_code=um.agt_code left join
        loc on tagt.loc_code=loc.loc_cd
"""

tagt_df = sql_to_df(sql_string, 1, spark)
#print("Number of rows:   ", tagt_df.count())
#print("Number of agents: ", tagt_df.dropDuplicates(['agt_cd']).count())
#tagt_df.limit(5).display()
#tagt_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in tagt_df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Merge sales and agents data</strong>

# COMMAND ----------

rslt_df = tsales_df.join(tagt_df, on='agt_cd')

print('No coverages: ', rslt_df.count())
print('No new pols:  ', rslt_df.dropDuplicates(['new_pol_num']).count())
print('No new cus:   ', rslt_df.dropDuplicates(['new_po_num']).count())
print('Total new ape:', rslt_df.agg({'ape': 'sum'}).collect()[0][0])
print('No new agt:   ', rslt_df.dropDuplicates(['agt_cd']).count())
rslt_df.limit(5).display()
#rslt_df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in rslt_df.columns]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store data for later use

# COMMAND ----------

rslt = rslt_df.toPandas()
rslt.to_csv(f'{out_path}agency_analytics_{yymm}.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze data

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load data for analysis</strong>

# COMMAND ----------

rslt = pd.read_csv(f'{out_path}agency_analytics_{yymm}.csv')
# Fill all empty rows with 'N/A'
rslt = rslt.fillna({'rid_typ': 'N/A', 'agt_rank': 'N/A', 'um_code': 'N/A', 'sm_code': 'N/A', 'rh_name': 'N/A'}).fillna(0)
rslt.isna().any()

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>1)	Agency new sales split by New vs Existing customers</strong>

# COMMAND ----------

# Splitting the rslt by rpt_mth, cus_typ, same_agt_ind, agt_status, agt_segment and counting the number of new_pol_num/new_po_num and sum of APE for each combination
div_by_1000 = lambda x: (x / 1000).sum()
view1 = rslt.groupby(['rpt_mth', 'cus_typ', 'repeat_mth', 'agt_segment'], as_index=True) \
    .agg(
    total_nb=('new_pol_num', 'nunique'),
    total_po=('new_po_num', 'nunique'),
    total_ape=('ape', div_by_1000),
    base_ape=('new_base_ape', div_by_1000),
    rider_ape=('new_rider_ape', div_by_1000)
  ) \
  .sort_values(['rpt_mth', 'cus_typ']) \
  .reset_index()

# Displaying the result
view1.to_csv(f'{out_path}agency_analytics_{yymm}_view1.csv')
print(view1.agg({'total_ape':'sum'}))
view1


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>2)	Agency sales split by all required metrics</strong>

# COMMAND ----------

# Splitting the rslt by all metrics and counting the number of new_pol_num/new_po_num and sum of APE for each combination
rslt.isna().any()
view2 = rslt.groupby(['rpt_mth', 'cus_typ', 'same_agt_ind', 'agt_segment',
                      'agt_rank', 'tenure', 'prd_typ', 'rid_typ'], as_index=True) \
    .agg(
    total_nb=('new_pol_num', 'nunique'),
    total_po=('new_po_num', 'nunique'),
    total_ape=('ape', div_by_1000),
    base_ape=('new_base_ape', div_by_1000),
    rider_ape=('new_rider_ape', div_by_1000)
  ) \
  .sort_values(['rpt_mth', 'cus_typ']) \
  .reset_index()

# Displaying the result
view2.to_csv(f'{out_path}agency_analytics_{yymm}_view2.csv')
print(view2.agg({'total_ape':'sum'}))
view2


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>3)	Split by Agents regions and locations</strong>

# COMMAND ----------

# Splitting the rslt by RH/SM/UM and location counting the number of new_pol_num/new_po_num and sum of APE for each combination
#div_by_1000 = lambda x: (x / 1000).sum()
view3 = rslt.groupby(['rpt_mth', 'cus_typ', 'repeat_mth', 'rh_name', 'sm_code', 'um_code', 'loc_code',
                       'same_agt_ind', 'agt_segment', 'agt_rank', 'tenure'], as_index=True) \
    .agg(
    total_nb=('new_pol_num', 'nunique'),
    total_po=('new_po_num', 'nunique'),
    total_ape=('ape', div_by_1000),
    base_ape=('new_base_ape', div_by_1000),
    rider_ape=('new_rider_ape', div_by_1000)
  ) \
  .sort_values(['rpt_mth', 'cus_typ']) \
  .reset_index()

# Displaying the result
view3.to_csv(f'{out_path}agency_analytics_{yymm}_view3.csv')
print(view3.agg({'total_ape':'sum'}))
view3


# COMMAND ----------

# MAGIC %md
# MAGIC <strong>4) Select top 10 location with highest sales from existing customers</strong>

# COMMAND ----------

view4 = rslt.loc[rslt['loc_code'].isin(['NIB01','DOA02','HCM39','HN38','HCM69',
                                        'HCM01','HN25','HPH18','CMC99','HN96'])] \
    .groupby(['rpt_mth', 'repeat_mth', 'loc_code', 'rh_name', 'sm_code'], as_index=True) \
    .agg(
    total_nb=('new_pol_num', 'nunique'),
    total_po=('new_po_num', 'nunique'),
    total_ape=('ape', div_by_1000),
    base_ape=('new_base_ape', div_by_1000),
    rider_ape=('new_rider_ape', div_by_1000)
  ) \
  .sort_values(by=['total_ape'], ascending=False) \
  .reset_index()

# Displaying the result
view4.to_csv(f'{out_path}agency_analytics_{yymm}_view4.csv')
print(view4.agg({'total_ape':'sum'}))
view4
