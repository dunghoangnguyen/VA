# Databricks notebook source
# MAGIC %md
# MAGIC # ADEC Metrics Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### GENERATE AUTO METRICS

# COMMAND ----------

# MAGIC %md
# MAGIC <strong>Initialize tables and params</strong>

# COMMAND ----------

rpt_yr = 2023
lmth = -1
llmth = -2
rpt_mth = 2307
rpt_prv_mth = 2306
exclusion_list_full = ['MI007','PA007','PA008', 'EV001', 'EV002', 'EV003', 'EV004', 'EV005', 'EV006', 'EV007', 'EV008', 'EV009', 'EV010', 'EV011', 'EV012', 'EV013', 'EV014', 'EV015', 'EV016', 'EV017', 'EV018', 'EV019', 'EV101', 'EV102', 'EV103', 'EV104', 'EV105', 'EV201', 'EV202', 'EV203', 'EVS01', 'EVS02', 'EVS03', 'EVS04', 'EVS05', 'FC103', 'FC208', 'FD101', 'FD102', 'FD103', 'FD104', 'FD105', 'FD106', 'FD107', 'FD108', 'FD109', 'FD204', 'FD205', 'FD206', 'FD207', 'FD208','FD209','FD210','FD211','FD212','FD213','FD214','FS101','FS102','FS103','FS104','FS105','FS106','FS107','FS108','FS109','FS205','FS206','FS207','FS208','FS209','FS210','FS211','FS212','FS213','FS214','FC101','FC102','FC104','FC105','FC106','FC107','FC108','FC109','FC206','FC207','FC209','FC210','FC211','FC212','FC213','FC214','VEH10','VEU14','VEP18','FC205','FS204','FC204','EVX03','FD203',
'FS203','FC203','FD202','FS202','FC202','VEDCL','VEDEN']
exclusion_list_sub = ['MI007','PA007','PA008']
active_sts = ['1','2','3','5']
pi_wam_cust = 15788 # Total Number of Unique Customers (r 6)
pi_wam_no_per_cust = 1.13178 # Number of Funds per Customer (r 32)
pi_wam_no_per_agt_cust = 1.132 # r 33
pi_wam_no_per_banca_cust = 1.0769 # r 34
pi_no_cyst_ly = 1561144
pi_no_cust_tgt = 0 # pre 1151196
pi_digital_leads = 1373
pi_rnps = 70
pi_entry_goal_rnps = 0
pi_strech_goal_rnps = 0
pi_top_comp_rnps = 0
pi_top_comp_name = "Manulife"

customer_table = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/customer_table_{rpt_mth}")
product_table = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/product_table_{rpt_mth}")
policy_base = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/policy_base_{rpt_mth}")
att_cus = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/att_cus_{rpt_mth}")
newcustomer_year = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/newcustomer_{rpt_yr}")
newcustomer = spark.read.parquet(f"abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/ADEC/WorkingData/newcustomer_{rpt_mth}")

print("customer_table:",customer_table.count())
print("product_table:",product_table.count())
print("policy_base:",policy_base.count())
print("att_cus:",att_cus.count())
print("newcustomer:",newcustomer.count())

# COMMAND ----------


