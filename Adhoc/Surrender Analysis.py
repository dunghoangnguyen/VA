# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# Import the necessary functions from the pyspark.sql module
from pyspark.sql import functions as F
import pandas as pd
# Declare string date
min_date_str='2019-01-01'
max_date_str='2024-02-29'
max_date = F.to_date(F.lit(max_date_str))
yymm        =max_date_str[2:4]+max_date_str[5:7]

# Specify the path to the data mart folder
ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_AMS_SNAPSHOT_DB/'
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'
#dm_path  = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
sc_path  = '/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'
rpt_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/'

out_path = '/dbfs/mnt/lab/vn/project/scratch/adhoc/'

# Define the names of the tables
#tbl1 = 'tcoverages'
tbl2 = 'tpolicys'
tbl3 = 'tams_agents'
tbl4 = 'agent_scorecard'
tbl5 = 'loc_to_sm_mapping_hist'
tbl6 = 'tfield_values'
tbl7 = 'vn_plan_code_map'
tbl8 = 'tclient_policy_links'

# Create a list containing the data mart path
path_list = [#ams_path, 
             cas_path, cpm_path,
             sc_path, rpt_path]

# Create a list containing the table names
tbl_list = [#tbl1,
            tbl2, #tbl3, 
            tbl4,
            tbl5, tbl6, tbl7, tbl8
            ]

# Load the parquet files into a dictionary of DataFrames
df_list = load_parquet_files(path_list, tbl_list)

df_list['TPOLICYS'] = df_list['TPOLICYS'].select('pol_num', 'plan_code_base', 'ins_typ_base', 'wa_cd_1', 'sbmt_dt', 'pol_eff_dt', 
                                                 'pol_stat_cd', 'mode_prem', 'pmt_mode', 'pol_trmn_dt', 'image_date')
df_list['TCLIENT_POLICY_LINKS'] = df_list['TCLIENT_POLICY_LINKS'].select('cli_num', 'pol_num', 'link_typ', 'image_date') \
    .where(F.col('rec_status')=='A')

# Iterate over the tables in the df_list dictionary
for tbl_name in df_list:
    # Retrieve the DataFrame for the current table
    df = df_list[tbl_name]

    # Filter the DataFrame to keep only rows where the 'image_date' column is equal to 'max_date_str'
    if "image_date".upper() in (col_name.upper() for col_name in df.columns):
        df = df.filter(F.col('image_date') >= F.to_date(F.lit(min_date_str)))
    elif "monthend_dt".upper()in (col_name.upper() for col_name in df.columns):
        df = df.filter(F.col('monthend_dt') == F.to_date(F.lit(max_date_str)))
    #print("Count of records in", tbl_name, ":", df.count())
    
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
# MAGIC <strong>Get list of inforce (premium paying) policies before surrender - break-down by Product type/group/codes</strong>

# COMMAND ----------

sql_string = """
select  tpol.image_date,
        tcpl.cli_num po_num,
        tpol.pol_num,
        tpol.plan_code_base plan_code,
        tpol.wa_cd_1 agt_cd,
        to_date(tpol.sbmt_dt) sbmt_dt,
        to_date(tpol.pol_eff_dt) pol_eff_dt,
        cast(tpol.mode_prem*cast(tpol.pmt_mode as int)/12 as decimal(18,2)) ape,
        nvl(tfld.fld_valu_desc,'') prd_typ,
        case when tpln.nbv_factor_group='Value Preservation Option' then 'VPO'
             else nvl(substring_index(tpln.nbv_factor_group, ' -', 1),'') 
        end as prd_desc
from    tpolicys tpol inner join
        tclient_policy_links tcpl on tpol.pol_num=tcpl.pol_num and tpol.image_date=tcpl.image_date and tcpl.link_typ='O' left join
        tfield_values tfld on tpol.ins_typ_base=tfld.fld_valu and tpol.image_date=tfld.image_date and tfld.fld_nm='INS_TYP' and tfld.rec_status='A' left join
        vn_plan_code_map tpln on tpol.plan_code_base = tpln.plan_code
where   tpol.pol_stat_cd in ('1','3') -- select only premium paying policies
    --and tpol.image_date < '{max_date_str}'
"""

ipol_df = sql_to_df(sql_string, 1, spark)
#ipol_df.display()
ipol_sum = ipol_df.groupBy(['image_date', 'prd_typ', 'plan_code', 'prd_desc']) \
                .agg(
                    F.countDistinct('po_num').alias('total_pos'),
                    F.countDistinct('pol_num').alias('total_pols'),
                    F.sum('ape').alias('total_ape')
                ) \
                .where((F.col('prd_typ')!='') &
                       (F.col('image_date') < max_date)
                ) \
                .orderBy('image_date', ascending=False)

ipol_sum.display()
ipol_df.createOrReplaceTempView('ipol')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Get list of policies surrendered from 2019 - Feb'2024</strong>

# COMMAND ----------

sql_string = """
select  last_day(tpol.pol_trmn_dt) image_date,
        tcpl.cli_num po_num,
        tpol.pol_num,
        tpol.plan_code_base plan_code,
        tpol.wa_cd_1 agt_cd,
        to_date(tpol.sbmt_dt) sbmt_dt,
        to_date(tpol.pol_eff_dt) pol_eff_dt,
        cast(tpol.mode_prem*cast(tpol.pmt_mode as int)/12 as decimal(18,2)) ape,
        nvl(tfld.fld_valu_desc,'') prd_typ,
        case when tpln.nbv_factor_group='Value Preservation Option' then 'VPO'
             else nvl(substring_index(tpln.nbv_factor_group, ' -', 1),'') 
        end as prd_desc
from    tpolicys tpol inner join
        tclient_policy_links tcpl on tpol.pol_num=tcpl.pol_num and tpol.image_date=tcpl.image_date and tcpl.link_typ='O' left join
        tfield_values tfld on tpol.ins_typ_base=tfld.fld_valu and tpol.image_date=tfld.image_date and tfld.fld_nm='INS_TYP' and tfld.rec_status='A' left join
        vn_plan_code_map tpln on tpol.plan_code_base = tpln.plan_code
where   tpol.pol_stat_cd = 'E' -- select only premium paying policies
    and tpol.image_date = '{max_date_str}'
"""

spol_df = sql_to_df(sql_string, 1, spark)
#ipol_df.display()
spol_sum = spol_df.groupBy(['image_date', 'prd_typ', 'plan_code', 'prd_desc']) \
                .agg(
                    F.countDistinct('po_num').alias('surrender_pos'),
                    F.countDistinct('pol_num').alias('surrender_pols'),
                    F.sum('ape').alias('surrender_ape')
                ) \
                .where(F.col('prd_typ')!='') \
                .orderBy('image_date', ascending=False)

spol_sum.display()
#ipol_sum.createOrReplaceTempView('ipol_sum')
#spol_sum.createOrReplaceTempView('spol_sum')

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Consolidate inforce and surrender numbers from Jan'19 - Feb'24</strong>

# COMMAND ----------

rslt_df = ipol_sum.alias('a').join(spol_sum.alias('b'), on=['image_date','prd_typ','plan_code'], how='inner') \
                .select(
                    'a.image_date',
                    'a.prd_typ',
                    'a.plan_code',
                    'a.prd_desc',
                    'total_pos',
                    'total_pols',
                    'total_ape',
                    'surrender_pos',
                    'surrender_pols',
                    'surrender_ape'
                ) \
                .where(F.col('surrender_ape')>0)

rslt = rslt_df.toPandas()
rslt.shape
rslt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store data for later use

# COMMAND ----------

rslt.to_csv(f'{out_path}surrender_analysis_{yymm}.csv', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze data

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Load data for analysis</strong>

# COMMAND ----------

rslt = pd.read_csv(f'{out_path}surrender_analysis_{yymm}.csv')
# Fill all empty rows with 'N/A'
rslt = rslt.fillna({'prd_typ': 'N/A', 'prd_desc': 'N/A'}).fillna(0)
rslt.isna().any()
