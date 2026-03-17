# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# Import the necessary functions from the pyspark.sql module
from pyspark.sql import functions as F
# Declare string date
#min_date_str='2023-01-01'
#max_date_str='2024-01-31'

# Specify the path to the data mart folder
ams_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_AMS_SNAPSHOT_DB/'
cas_path = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/'
dm_path  = '/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
cpm_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/'
sf_path  = '/mnt/prod/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/'
rpt_path = '/mnt/prod/Curated/VN/Master/VN_CURATED_REPORTS_DB/'
lab_path = '/mnt/lab/vn/project/scratch/adhoc/'

# Define the names of the tables
tbl1 = 'account/'
tbl2 = 'tpolidm_daily/'
tbl3 = 'tcustdm_daily/'
tbl4 = 'tagtdm_daily/'

# Create a list containing the data mart path
path_list = [dm_path, sf_path,
             #ams_path, cas_path, cpm_path, rpt_path, lab_path
             ]

# Create a list containing the table names
tbl_list = [tbl1, tbl2, tbl3, tbl4,
            #tbl5, tbl6, tbl7, tbl8
            ]

cus_visit = spark.read.csv(f'{lab_path}customer_visit_product_page.csv', header=True)

# Load the parquet files into a dictionary of DataFrames
df_list = load_parquet_files(path_list, tbl_list)

# COMMAND ----------

generate_temp_view(df_list)
cus_visit.createOrReplaceTempView("cus_visit")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Map all data

# COMMAND ----------

mapped_df = spark.sql("""
select  a.*, 
        b.External_Id__c po_num, --b.MCF_User_Id__pc,
        e.cli_nm, e.mobl_phon_num, e.email_addr, e.sms_ind, e.sms_eservice_ind,
        c.pol_num, c.plan_code, c.plan_nm, c.pol_stat_desc, c.sa_code agt_code,
        d.agt_nm, d.rank_code, d.loc_cd, d.mobl_phon_num agt_mobile, d.email_addr agt_email
from    cus_visit a left join
        account b on a.id=b.MCF_User_Id__pc left join
        tpolidm_daily c on b.External_Id__c=c.po_num left join
        tagtdm_daily d on c.sa_code=d.agt_code left join
        tcustdm_daily e on c.po_num=e.cli_num
where   1=1
""")

mapped_pd = mapped_df.toPandas()

mapped_pd.shape
mapped_pd.head(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save file to https://abcmfcadovnedl01psea.dfs.core.windows.net/lab/vn/project/scratch/adhoc/

# COMMAND ----------

mapped_pd.to_csv(f'/dbfs{lab_path}customer_visit_product_page_pii.csv', index=False, header=True, encoding='utf-8-sig')
