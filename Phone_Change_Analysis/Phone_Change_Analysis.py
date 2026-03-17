# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load necessary tables

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from datetime import datetime, timedelta
import pandas as pd
from dateutil.relativedelta import relativedelta

x = 3 # number of month we move back from the current month
y = 1

# Set time
last_mthend = ((pd.Timestamp.now() -relativedelta(months=x)).replace(day=1) - pd.Timedelta(days=1))
prev_12mth  = (last_mthend - relativedelta(months=12)).replace(day=1) # - pd.Timedelta(days=1)

# Convert to date type
last_mthend_date = last_mthend.date()
prev_12mth_date = prev_12mth.date()

# Set txn period
txn_end = ((pd.Timestamp.now() -relativedelta(months=y)).replace(day=1) - pd.Timedelta(days=1)).date()
txn_st  = (txn_end - relativedelta(months=16)).replace(day=1)

# Convert to string
txn_end_str = txn_end.strftime('%Y-%m-%d')
print("Mobile change period:", txn_st, "to:", txn_end)

# Convert to string
last_mthend_str = last_mthend.strftime('%Y-%m-%d')
prev_12mth_str = prev_12mth.strftime('%Y-%m-%d')
print("Policy period:", last_mthend_date, "to:", prev_12mth_date)
#print(last_mthend_str, '\n', prev_12mth_str)

# Load and merge txn histories getting the past 12 months data
CAS_PATH = f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_CAS_DB/'
DM_PATH  = f'/mnt/prod/Curated/VN/Master/VN_CURATED_DATAMART_DB/'
CUR_PATH = f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/'

TXN1_FILE = 'TTRXN_HISTORIES_PARTIAL/'
TXN_FILE = 'TTRXN_HISTORIES/'
POL_FILE = 'TPOLIDM_MTHEND/'
CUS_FILE = 'TCUSTDM_MTHEND/'
AGT_FILE = 'TAGTDM_MTHEND/'
ACY_FILE = 'INCOME_BASED_DECILE_AGENCY/'
PD_FILE  = 'INCOME_BASED_DECILE_PD/'

FILE_PATHS = [CAS_PATH, DM_PATH, CUR_PATH]
FILE_NAMES = [TXN1_FILE, TXN_FILE, POL_FILE, CUS_FILE, AGT_FILE, ACY_FILE, PD_FILE]

# COMMAND ----------

dfs_list = load_parquet_files(FILE_PATHS, FILE_NAMES)
generate_temp_view(dfs_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 0. Generate customer portfolio information

# COMMAND ----------

nottaken_list = ['A','N','R','X']
nottaken = ','.join([f"'{x}'" for x in nottaken_list])

inforce_list = ['1','2','3','5','7','9']
inforce = ','.join([f"'{x}'" for x in inforce_list])

# Merge and retrieve some demographic and portfolio information for all customers
common_cols = ["po_num","first_pol_eff_dt","sex_code","client_tenure","pol_count","ins_typ_count","term_pol","endow_pol","health_indem_pol","whole_pol","investment_pol","inforce_pol","lapsed_pol","surrendered_pol","matured_pol","nottaken_pol","terminated_pol","f_owner_is_agent"]

cus_all_string = '''
with lst_prd as (
    select  PO_NUM, case when pol.PLAN_CODE like 'RUV%' then 'ILP'
                         when pol.PLAN_CODE like 'UL%' then 'UL'
                         else fld.FLD_VALU_DESC_ENG end LST_PRD_TYP,
            row_number() over (partition by PO_NUM order by POL_EFF_DT desc) rn
    from    tpolidm_mthend pol left join
            vn_published_cas_db.tplans pln on pol.PLAN_CODE = pln.PLAN_CODE left join
            vn_published_cas_db.tfield_values fld on pln.INS_TYP = fld.FLD_VALU and fld.FLD_NM='INS_TYP'
    where   pol.IMAGE_DATE='{txn_end}'
        and pol.POL_STAT_CD not in ({nottaken})
    qualify rn=1
),
rider as (
    select  PO_NUM, count(pol.POL_NUM) RIDER_CNT, sum(case when cov.PLAN_CODE like 'RHC%' then 1 else 0 end) HCR_CNT, 
            sum(case when cov.PLAN_CODE like 'MC%' then 1 else 0 end) MC_CNT, cast(sum(CVG_PREM)/25 as float) RIDER_APE
    from    tpolidm_mthend pol inner join
            vn_published_casm_cas_snapshot_db.tcoverages cov on pol.POL_NUM = cov.POL_NUM and pol.IMAGE_DATE = cov.IMAGE_DATE
    where   1=1 
      and   pol.IMAGE_DATE='{txn_end}'
      and   CVG_STAT_CD not in ({nottaken})
      and   CVG_TYP='R'
    group by PO_NUM
)
select  pol.PO_NUM,
        cli.SEX_CODE,
        lst.LST_PRD_TYP,
        nvl(rd.RIDER_CNT,0) RIDER_CNT, nvl(rd.HCR_CNT,0) HCR_CNT, nvl(rd.MC_CNT,0) MC_CNT, --rd.RIDER_APE,
        --cast(min(case when pol.POL_STAT_CD not in ({nottaken}) then pln.MLI_PREM_PRD end) as int) MIN_PREM_DUR,
        min(case when pol.POL_STAT_CD not in ({nottaken}) then to_date(pol.POL_EFF_DT) end) FIRST_POL_EFF_DT,
        max(case when pol.POL_STAT_CD not in ({nottaken}) then datediff('{txn_end}',pol.POL_EFF_DT)/365.25 end) CLIENT_TENURE,
        count(distinct case when pol.POL_STAT_CD not in ({nottaken}) and pln.PLAN_CODE is not null then pln.INS_TYP end) as INS_TYP_COUNT,
        count(distinct case when pol.POL_STAT_CD not in ({nottaken}) and pln.INS_TYP='4' then pol.POL_NUM end) TERM_POL,
        count(distinct case when pol.POL_STAT_CD not in ({nottaken}) and pln.INS_TYP='1' then pol.POL_NUM end) ENDOW_POL,
        count(distinct case when pol.POL_STAT_CD not in ({nottaken}) and pln.INS_TYP='7' then pol.POL_NUM end) HEALTH_INDEM_POL,
        count(distinct case when pol.POL_STAT_CD not in ({nottaken}) and pln.INS_TYP='0' then pol.POL_NUM end) WHOLE_POL,
        count(distinct case when pol.POL_STAT_CD not in ({nottaken}) and pln.INS_TYP='F' and pol.PLAN_CODE like 'RUV%' then pol.POL_NUM end) ILP_POL,
        count(distinct case when pol.POL_STAT_CD not in ({nottaken}) and pln.INS_TYP='F' and pol.PLAN_CODE like 'UL%' then pol.POL_NUM end) UL_POL,
        count(distinct case when pol.POL_STAT_CD in ({inforce}) then pol.POL_NUM end) INFORCE_POL,
        count(distinct case when pol.POL_STAT_CD='B' then pol.POL_NUM end) LAPSED_POL,
        count(distinct case when pol.POL_STAT_CD='E' then pol.POL_NUM end) SURRENDERED_POL,
        count(distinct case when pol.POL_STAT_CD in ('F','H') then pol.POL_NUM end) MATURED_POL,
        count(distinct case when pol.POL_STAT_CD in ({nottaken}) then pol.POL_NUM end) NOTTAKEN_POL,
        count(distinct case when pol.POL_STAT_CD not in ({inforce},{nottaken},'B','E','F','H') then pol.POL_NUM end) TERMINATED_POL,
        min(case when agt1.AGT_CODE is not null then 1 else 0 end) F_OWNER_IS_AGENT,
        min(case when agt2.AGT_CODE is not null then 1 else 0 end) F_OWNER_IS_AGENT_BY_NGD
from    tpolidm_mthend pol inner join
        tcustdm_mthend cli on pol.PO_NUM = cli.CLI_NUM and pol.IMAGE_DATE = cli.IMAGE_DATE left join
        vn_published_cas_db.tplans pln on pol.PLAN_CODE = pln.PLAN_CODE left join
        vn_published_cas_db.tfield_values fld on pln.INS_TYP = fld.FLD_VALU and fld.FLD_NM='INS_TYP' left join
        tagtdm_mthend agt1 on cli.IMAGE_DATE = agt1.IMAGE_DATE
                            and trim(cli.ID_NUM) = trim(agt1.ID_NUM) 
                            and cli.SEX_CODE = agt1.SEX_CODE 
                            and trim(lower(cli.CLI_NM)) = trim(lower(agt1.AGT_NM)) 
                            and cli.BIRTH_DT = agt1.BIRTH_DT left join
        tagtdm_mthend agt2 on cli.IMAGE_DATE = agt2.IMAGE_DATE
                            and cli.SEX_CODE = agt2.SEX_CODE 
                            and trim(lower(cli.CLI_NM)) = trim(lower(agt2.AGT_NM)) 
                            and cli.BIRTH_DT = agt2.BIRTH_DT left join
        lst_prd lst on pol.PO_NUM = lst.PO_NUM left join
        rider rd on pol.PO_NUM = rd.PO_NUM

where   1=1
    and pol.IMAGE_DATE='{txn_end}'
    and cli.SEX_CODE in ('F','M')
    and cli.CLI_TYP = 'Individual'
group by
        pol.PO_NUM, cli.SEX_CODE, lst.LST_PRD_TYP,
        rd.RIDER_CNT, rd.HCR_CNT, rd.MC_CNT--, rd.RIDER_APE
'''

cus_all = sql_to_df(cus_all_string, 1, spark).dropDuplicates()
#cus_all.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Retrieve mobile phone change request records

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC select  PO_NUM, txn.POL_NUM, pol.POL_STAT_DESC, to_date(POL_EFF_DT) POL_EFF_DT, to_date(TRXN_DT) MOBL_CHG_DT, substr(USER_ID,1,3) USER, CLI_NM, txn.TRXN_DESC, cli.MOBL_PHON_NUM
# MAGIC from    vn_published_cas_db.ttrxn_histories_partial txn inner join
# MAGIC         vn_curated_datamart_db.tpolidm_daily pol on txn.POL_NUM = pol.POL_NUM inner join
# MAGIC         vn_curated_datamart_db.tcustdm_daily cli on pol.PO_NUM = cli.CLI_NUM
# MAGIC where   TRXN_CD = 'CLICHG'
# MAGIC   and   lower(trim(TRXN_DESC)) like 'client%changed%mobile%from%'
# MAGIC   and   TRXN_DT between '2024-10-01' and '2024-10-31'
# MAGIC   and   PO_NUM = '2802313426'
# MAGIC limit 10*/

# COMMAND ----------

# Retrieve agent phone list history
agt_phone_raw_df = dfs_list["TAGTDM_MTHEND"].select("AGT_CODE","MOBL_PHON_NUM").withColumnRenamed("MOBL_PHON_NUM", "AGT_PHON").dropDuplicates()
agt_phone_df = agt_phone_raw_df.groupBy("AGT_PHON") \
        .agg(F.collect_set("AGT_CODE").alias("SA_CODE_ARRAY"))

# Merge all Policy Owners from income based data
agency_cus_df = dfs_list["INCOME_BASED_DECILE_AGENCY"] \
        .select([F.col(col).alias(col.upper()) for col in common_cols]) \
        .sort(F.desc("image_date"), "po_num")

pd_cus_df = dfs_list["INCOME_BASED_DECILE_PD"] \
        .select([F.col(col).alias(col.upper()) for col in common_cols]) \
        .sort(F.desc("image_date"), "po_num")

all_cus_df = agency_cus_df.unionAll(pd_cus_df).dropDuplicates(["PO_NUM"])
#print(agt_phone_df.count())
mobl_chg_par_sql = '''
select  distinct PO_NUM, pol.POL_NUM, cli.MOBL_PHON_NUM, substr(txn.USER_ID,1,3) USER, 
        to_date(pol.SBMT_DT) SBMT_DT, to_date(txn.TRXN_DT) MOBL_CHG_DT, 
        to_date(pol.POL_EFF_DT) POL_EFF_DT, TRXN_DESC,
        pol.DIST_CHNL_CD, pol.SA_CODE, pol.SA_LOC_CODE, nvl(loc.RH_NAME,'Non-Agency') REGION, cli.CITY,
        datediff(txn.TRXN_DT,pol.SBMT_DT) CHG_FR_SBMT_DAYS,
        datediff(txn.TRXN_DT,pol.POL_EFF_DT) CHG_FR_EFF_DAYS, SORT_SEQ
from    ttrxn_histories_partial txn inner join
        tpolidm_mthend pol on txn.POL_NUM = pol.POL_NUM inner join
        tcustdm_mthend cli on pol.PO_NUM = cli.CLI_NUM and pol.IMAGE_DATE = cli.IMAGE_DATE inner join
        tagtdm_mthend agt  on pol.SA_CODE = agt.AGT_CODE and pol.IMAGE_DATE = agt.IMAGE_DATE left join
        vn_curated_reports_db.loc_to_sm_mapping_hist loc on agt.LOC_CD = loc.LOC_CD and agt.IMAGE_DATE = loc.IMAGE_DATE
where   1=1
  and   pol.IMAGE_DATE = '{txn_end}'
  and   TRXN_CD = 'CLICHG'
  and   pol.POL_STAT_CD in ('1','2','3','5','7','9','B','E','F','H')
  and   lower(trim(TRXN_DESC)) like 'client%changed%mobile%from%'
  and   TRXN_DT between '{txn_st}' and '{txn_end}'
  and   cli.SEX_CODE in ('F','M')
  and   cli.CLI_TYP = 'Individual'
  and   pol.WA_CODE not in ('B3080','B4632','B2058','D0896','D1181','B4536','D0921')
  --and   abs(datediff(txn.TRXN_DT,pol.POL_EFF_DT)) <= 60         -- Mobile phone change after application submitted
  and   pol.PLAN_CODE not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001','PA001','CA360','CX360')
order by PO_NUM, POL_NUM, MOBL_CHG_DT, SORT_SEQ DESC
'''

mobl_chg_part1 = sql_to_df(mobl_chg_par_sql, 1, spark).drop("SORT_SEQ")

mobl_chg_sql = '''
select  distinct PO_NUM, pol.POL_NUM, cli.MOBL_PHON_NUM, substr(txn.USER_ID,1,3) USER, 
        to_date(pol.SBMT_DT) SBMT_DT, to_date(txn.TRXN_DT) MOBL_CHG_DT, 
        to_date(pol.POL_EFF_DT) POL_EFF_DT, TRXN_DESC,
        pol.DIST_CHNL_CD, pol.SA_CODE, pol.SA_LOC_CODE, nvl(loc.RH_NAME,'Non-Agency') REGION, cli.CITY,
        datediff(txn.TRXN_DT,pol.SBMT_DT) CHG_FR_SBMT_DAYS,
        datediff(txn.TRXN_DT,pol.POL_EFF_DT) CHG_FR_EFF_DAYS, SORT_SEQ
from    ttrxn_histories txn inner join
        tpolidm_mthend pol on txn.POL_NUM = pol.POL_NUM inner join
        tcustdm_mthend cli on pol.PO_NUM = cli.CLI_NUM and pol.IMAGE_DATE = cli.IMAGE_DATE inner join
        tagtdm_mthend agt  on pol.SA_CODE = agt.AGT_CODE and pol.IMAGE_DATE = agt.IMAGE_DATE left join
        vn_curated_reports_db.loc_to_sm_mapping_hist loc on agt.LOC_CD = loc.LOC_CD and agt.IMAGE_DATE = loc.IMAGE_DATE
where   1=1
  and   pol.IMAGE_DATE = '{txn_end}'
  and   TRXN_CD = 'CLICHG'
  and   pol.POL_STAT_CD in ('1','2','3','5','7','9','B','E','F','H')
  and   lower(trim(TRXN_DESC)) like 'client%changed%mobile%from%'
  and   TRXN_DT between '{txn_st}' and '{txn_end}'
  and   cli.SEX_CODE in ('F','M')
  and   cli.CLI_TYP = 'Individual'
  and   pol.WA_CODE not in ('B3080','B4632','B2058','D0896','D1181','B4536','D0921')    -- Remove dummy agent codes
  --and   abs(datediff(txn.TRXN_DT,pol.POL_EFF_DT)) <= 60         -- Mobile phone change after application submitted
  and   pol.PLAN_CODE not in ('FDB01','BIC01','BIC02','BIC03','BIC04','PN001','PA001','CA360','CX360')
order by PO_NUM, POL_NUM, MOBL_CHG_DT, SORT_SEQ DESC
'''

mobl_chg_part2 = sql_to_df(mobl_chg_sql, 1, spark).drop("SORT_SEQ")

mobl_chg = mobl_chg_part1.unionAll(mobl_chg_part2).dropDuplicates(["PO_NUM","POL_NUM","MOBL_CHG_DT"])

# Define the regular expressions to extract the old and new phone numbers
old_phone_regex = r'from (\d{10})'
new_phone_regex = r'to (\d{10})'

# Retrieve the list of PO and their respective policy holding (all status)
cus_pol_list = mobl_chg.groupBy("PO_NUM").agg(F.concat_ws(",",F.collect_set("POL_NUM")).alias("POLICY_LIST"))

# Extract the old and new phone numbers and create new columns
mobl_chg = mobl_chg \
                .withColumn("OLD_PHONE", F.regexp_extract("TRXN_DESC", old_phone_regex, 1)) \
                .withColumn("NEW_PHONE", F.regexp_extract("TRXN_DESC", new_phone_regex, 1)) \
                .withColumn("DAYS_CHG_AF_SBMT", 
                            F.when(F.col("CHG_FR_SBMT_DAYS") >= 0,
                                F.when(F.col("CHG_FR_SBMT_DAYS") < 7, "[0-7)")
                                .when(F.col("CHG_FR_SBMT_DAYS") < 14, "[7-14)")
                                .when(F.col("CHG_FR_SBMT_DAYS") < 30, "[14-30)")
                                .when(F.col("CHG_FR_SBMT_DAYS") < 60, "[30-60)")
                                .otherwise("[60-inf)"))
                            .otherwise("(-inf-0)")) \
                .withColumn("DAYS_CHG_AF_EFF", 
                            F.when(F.col("CHG_FR_EFF_DAYS") < 0, "(-inf-0)")
                            .when(F.col("CHG_FR_EFF_DAYS") < 7, "[0-7)")
                            .when(F.col("CHG_FR_EFF_DAYS") < 14, "[7-14)")
                            .when(F.col("CHG_FR_EFF_DAYS") < 30, "[14-30)")
                            .when(F.col("CHG_FR_EFF_DAYS") < 60, "[30-60)")
                            .otherwise("[60-inf)")) \
                .withColumn("USER", F.when(F.col("USER") == "CWS", F.lit("CWS")).otherwise("CAS")) \
                .drop("POL_NUM","TRXN_DESC")

# Join with agent phone DataFrame to add CHG_TO_AGT_IND and CHG_FR_AGT_IND
mobl_chg = mobl_chg \
    .join(
        cus_pol_list,
        on="PO_NUM",
        how="left"
    ) \
    .join(
        agt_phone_df.select("AGT_PHON", "SA_CODE_ARRAY").withColumnRenamed("SA_CODE_ARRAY", "SA_ARRAY_NEW"), 
        mobl_chg["NEW_PHONE"] == agt_phone_df["AGT_PHON"],
        how="left"
    ) \
    .withColumnRenamed("AGT_PHON", "AGT_PHON_NEW") \
    .join(
        agt_phone_df.select("AGT_PHON", "SA_CODE_ARRAY").withColumnRenamed("SA_CODE_ARRAY", "SA_ARRAY_OLD"),
        mobl_chg["OLD_PHONE"] == agt_phone_df["AGT_PHON"],
        how="left"
    ) \
    .withColumnRenamed("AGT_PHON", "AGT_PHON_OLD") \
    .withColumn("CHG_TO_AGT_IND", F.when(F.col("AGT_PHON_NEW").isNotNull(), "Y").otherwise("N")) \
    .withColumn("CHG_FR_AGT_IND", F.when(F.col("AGT_PHON_OLD").isNotNull(), "Y").otherwise("N")) \
    .withColumn("CHG_TO_SA_IND", F.when(F.array_contains(F.col("SA_ARRAY_NEW"), F.col("SA_CODE")), "Y").otherwise("N")) \
    .withColumn("CHG_FR_SA_IND", F.when(F.array_contains(F.col("SA_ARRAY_OLD"), F.col("SA_CODE")), "Y").otherwise("N")) \
    .drop("AGT_PHON_NEW", "AGT_PHON_OLD", "SA_ARRAY_NEW", "SA_ARRAY_OLD")

# Drop unused columns
mobl_chg = mobl_chg.drop("MOBL_PHON_NUM","SBMT_DT","POL_EFF_DT") #,"OLD_PHONE","NEW_PHONE")

sum_cols_1 = [#"CHG_FR_AGT_IND","CHG_TO_AGT_IND","USER","REGION","CITY","DAYS_CHG_AF_SBMT",
              "DAYS_CHG_AF_EFF"]
#sum_cols_2 = ["CHG_TO_AGT_IND","USER","REGION","CITY","DAYS_CHG_AF_SBMT","DAYS_CHG_AF_EFF"]

mobl_chg_sum1 = mobl_chg.groupBy(*sum_cols_1) \
        .agg(F.count("PO_NUM").alias("TXN_COUNT"),
             #F.countDistinct("POL_NUM").alias("POL_COUNT"), 
             F.countDistinct("PO_NUM").alias("PO_COUNT")) \
        .sort("DAYS_CHG_AF_EFF")

#mobl_chg.agg(F.count("PO_NUM").alias("TXN_COUNT"), F.countDistinct("PO_NUM").alias("PO_COUNT")).display()
#mobl_chg_sum1.display()
#print(mobl_chg.count())
#mobl_chg.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Retrieve fund withdrawal request records

# COMMAND ----------

fnd_wdr_par_sql = '''
select  distinct PO_NUM, pol.POL_NUM, to_date(txn.TRXN_DT) FUND_WITHDRAW_DT, 
        to_date(pol.POL_TRMN_DT) POL_TRMN_DT, SORT_SEQ, TRXN_DESC
        --cast(BASE_APE+RID_APE as int) TOT_APE, 
        --pol.DIST_CHNL_CD, pol.SA_CODE,
        --datediff(pol.POL_TRMN_DT,txn.TRXN_DT) FND_WDR_FR_CHG_DAYS
from    ttrxn_histories_partial txn inner join
        tpolidm_mthend pol on txn.POL_NUM = pol.POL_NUM
where   1=1
  and   pol.IMAGE_DATE = '{txn_end}'
  and   TRXN_CD = 'NFOACT'
  and   lower(trim(TRXN_DESC)) like '%fund%full%withdraw%'
  and   TRXN_DT between '{txn_st}' and '{txn_end}'
  and   POL_TRMN_DT between '{prev_12mth_str}' and '{last_mthend_str}'
order by PO_NUM, POL_NUM, FUND_WITHDRAW_DT DESC, SORT_SEQ DESC
'''

fnd_wdr_part1 = sql_to_df(fnd_wdr_par_sql, 1, spark).drop("SORT_SEQ")

fnd_wdr_sql = '''
select  distinct PO_NUM, pol.POL_NUM, to_date(txn.TRXN_DT) FUND_WITHDRAW_DT, 
        to_date(pol.POL_TRMN_DT) POL_TRMN_DT, SORT_SEQ, TRXN_DESC
        --cast(BASE_APE+RID_APE as int) TOT_APE,  
        --pol.DIST_CHNL_CD, pol.SA_CODE,
        --datediff(pol.POL_TRMN_DT,txn.TRXN_DT) FND_WDR_FR_CHG_DAYS
from    ttrxn_histories txn inner join
        tpolidm_mthend pol on txn.POL_NUM = pol.POL_NUM
where   1=1
  and   pol.IMAGE_DATE = '{last_mthend_str}'
  and   TRXN_CD = 'NFOACT'
  and   lower(trim(TRXN_DESC)) like '%fund%full%withdraw%'
  and   TRXN_DT between '{txn_st}' and '{txn_end}'
  and   POL_TRMN_DT between '{prev_12mth_str}' and '{last_mthend_str}'
order by PO_NUM, POL_NUM, FUND_WITHDRAW_DT DESC, SORT_SEQ DESC
'''

fnd_wdr_part2 = sql_to_df(fnd_wdr_sql, 1, spark).drop("SORT_SEQ")

fnd_wdr = fnd_wdr_part1.unionAll(fnd_wdr_part2).dropDuplicates(["PO_NUM","POL_NUM"])

# Define the regular expressions to extract the numeric parts [x] and [y]
x_regex = r'unit is (\d+)\.'
y_regex = r'\.(\d{4})'

# Extract the numeric parts and create new columns
fnd_wdr = fnd_wdr.withColumn("x_part", F.regexp_extract("TRXN_DESC", x_regex, 1)) \
                 .withColumn("y_part", F.regexp_extract("TRXN_DESC", y_regex, 1))

# Combine the extracted parts into a single value representing the fund amount as a string
fnd_wdr = fnd_wdr.withColumn("FND_AMT_str", F.concat_ws(".", "x_part", "y_part"))

# Cast the combined string to float and create the FND_AMT column
fnd_wdr = fnd_wdr.withColumn("FND_AMT", F.col("FND_AMT_str").cast("decimal(11,2)"))

# Drop the intermediate columns
fnd_wdr = fnd_wdr.drop("x_part", "y_part", "FND_AMT_str", "TRXN_DESC")

#print(fnd_wdr.count())
#fnd_wdr.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Merge all data points

# COMMAND ----------

final = mobl_chg \
    .join(fnd_wdr.withColumnRenamed("POL_NUM", "FND_WDR_POL_NUM"),
          on=["PO_NUM"],
          how="left") \
    .join(cus_all,
          on="PO_NUM",
          how="left") \
    .withColumn("CHG_FR_FND_WDR_DAYS", F.when(F.col("FND_WDR_POL_NUM").isNotNull(), F.datediff(F.col("FUND_WITHDRAW_DT"),F.col("MOBL_CHG_DT")))) \
    .drop("POL_TRMN_DT")

final = final.withColumn("DAYS_CHG_WITHDRAW",
                         F.when(F.col("CHG_FR_FND_WDR_DAYS").isNull(), "Out-of-scope")
                         .when(F.col("CHG_FR_FND_WDR_DAYS") >= 0,
                               F.when(F.col("CHG_FR_FND_WDR_DAYS") < 7, "[0-7)")
                               .when(F.col("CHG_FR_FND_WDR_DAYS") < 14, "[7-14)")
                               .when(F.col("CHG_FR_FND_WDR_DAYS") < 30, "[14-30)")
                               .when(F.col("CHG_FR_FND_WDR_DAYS") < 60, "[30-60)")
                               .otherwise("[60-inf)"))
                         .when(F.col("CHG_FR_FND_WDR_DAYS") < 0,
                               F.when(F.col("CHG_FR_FND_WDR_DAYS") >= -7, "(0--7]")
                               .when(F.col("CHG_FR_FND_WDR_DAYS") >= -14, "(-7--14]")
                               .when(F.col("CHG_FR_FND_WDR_DAYS") >= -30, "(-14--30]")
                               .when(F.col("CHG_FR_FND_WDR_DAYS") >= -60, "(-30--60]")
                               .otherwise("(-60--inf)"))
                         .otherwise("Out-of-scope"))

final = final.withColumn("CHANNEL", F.when(F.col("DIST_CHNL_CD").isin("*","01","02","08","50"), "AGENCY").otherwise("BANCA"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Add new MOC column

# COMMAND ----------

# Calculate the months difference between txn_end and MOBL_CHG_DT
#final = final.withColumn("txn_end", F.lit(txn_end_str))
#final = final.withColumn("MOBL_CHG_DT", F.to_date(final["MOBL_CHG_DT"]))
#final = final.withColumn("txn_end", F.to_date(final["txn_end"]))

# Calculate the months difference between txn_end and MOBL_CHG_DT
#final = final.withColumn(
#    "month_diff",
#    F.floor(F.months_between(F.col("txn_end"), F.col("MOBL_CHG_DT")))
#)

# Create the MOC column
#final = final.withColumn("MOC", F.when(F.col("month_diff").cast("int") == 0, F.lit("T")).otherwise(F.concat(F.lit("T+"), F.col("month_diff").cast("int"))))
final = final.withColumn("MOC", F.date_format(F.last_day(F.col("MOBL_CHG_DT")), "MMM-yy"))

# Drop the temporary columns if not needed
#final = final.drop("txn_end", "month_diff")
#final.limit(20).display()
#print(final.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Save data and do analysis

# COMMAND ----------

# Remove unused columns before conversion to improve run-time
final = final.drop('FIRST_POL_EFF_DT','CHG_FR_SBMT_DAYS','CHG_FR_EFF_DAYS','DIST_CHNL_CD','SA_CODE')
final_pd = final.toPandas()
#final_pd.drop(columns=['FIRST_POL_EFF_DT','CHG_FR_SBMT_DAYS','CHG_FR_EFF_DAYS','DIST_CHNL_CD','SA_CODE'], inplace=True)

bin_labels = [
    ('CLIENT_TENURE', [0, 1, 2, 3, 5, 7, 10, float('inf')],
     ['[0-1)','[1-2)','[2-3)','[3-5)','[5-7)','[7-10)','[10-inf)']),
    ('INFORCE_POL', [0, 1, 2, 3, 5, float('inf')],
     ['[0-1)','[1-2)','[2-3)','[3-5)','[5-inf)']),
    ('RIDER_CNT', [0, 1, 2, 3, 5, float('inf')],
     ['[0-1)','[1-2)','[2-3)','[3-5)','[5-inf)'])
]

for column, bins, labels in bin_labels:
    create_categorical(final_pd, column, bins, labels)

final_pd.to_csv(f"/dbfs/mnt/lab/vn/project/scratch/adhoc/phone_change_analysis_v2.csv", index=False, header=True, encoding='utf-8-sig')
print(final_pd.shape)
display(final_pd)
