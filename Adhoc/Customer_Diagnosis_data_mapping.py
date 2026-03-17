# Databricks notebook source
# MAGIC %run "/Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions"

# COMMAND ----------

import pyspark.sql.functions as F

dm_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
lab_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/'

tblSrc1 = 'tcustdm_daily/'
tblSrc2 = 'tpolidm_daily/'
tblSrc3 = 'tagtdm_daily/'

abfss_paths = [dm_path, lab_path]
parquet_files = [tblSrc1,tblSrc2,tblSrc3]

list_df = load_parquet_files(abfss_paths, parquet_files)
srcDF = spark.read.csv(f'{lab_path}hba1c.csv', header=True)

# COMMAND ----------

generate_temp_view(list_df)
srcDF.createOrReplaceTempView('hba1c')

# COMMAND ----------

sql_string = """
select  hba.pol_num,
        pol.po_num,
        cus.cli_nm,
        cus.birth_dt,
        cus.cur_age,
        cus.mobl_phon_num cli_mobl_phone,
        cus.email_addr cli_email,
        'NaN' med_report,
        cus.city,
        agt.agt_nm,
        agt.mobl_phon_num agt_mobl_phone,
        agt.email_addr agt_email
from    hba1c hba inner join
        tpolidm_daily pol on hba.pol_num=pol.pol_num inner join
        tcustdm_daily cus on pol.po_num=cus.cli_num inner join
        tagtdm_daily agt on pol.sa_code=agt.agt_code
"""

resultDF = sql_to_df(sql_string, 0, spark)

# COMMAND ----------

resultDF.coalesce(1).write.mode('overwrite').csv(f'{lab_path}hba1c_dtl.csv', header=True)
