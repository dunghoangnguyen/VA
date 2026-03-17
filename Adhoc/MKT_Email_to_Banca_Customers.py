# Databricks notebook source
# MAGIC %run /Repos/dung_nguyen_hoang@mfcgd.com/Utilities/Functions/

# COMMAND ----------

import pyspark.sql.functions as F
import pandas as pd

run_date = pd.Timestamp.now().strftime('%Y%m%d')
print(run_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Email MKT Outreach to Bank's Cutomers (TCB & VTB)
# MAGIC <strong>Required information: PO name, PO number, email_address

# COMMAND ----------

# MAGIC %md
# MAGIC Derive all addresses from customers whose all policies are still <i>inforce</i>

# COMMAND ----------

sql_string = '''
-- Get all in-force policies
with all_inf_pol as (
    select  po_num, dist_chnl_cd, pol_num, wa_code, sa_code, to_date(pol_eff_dt) pol_eff_dt
    from    vn_curated_datamart_db.tpolidm_daily pol
    where   pol_stat_cd in ('1','2','3','5','7','9')        -- Include only in-force policies
),
-- Get unique list of All-policy customers
base_pol_cus as (
    select  *,
            case when dist_chnl_cd in ('10','24','49')  then 'TCB'
                 when dist_chnl_cd in ('52','53')       then 'VTB'
            end banca_ind,
            row_number() over (partition by po_num order by pol_eff_dt desc) rn
    from    all_inf_pol
    where   1=1
        --and dist_chnl_cd in ('10','24','49','52','53')   -- Extract only either TCB or VTB customers
    qualify rn = 1
),
unique_banca_cus as (
    select  po_num, max(banca_ind) as banca_ind
    from    base_pol_cus
    where   banca_ind = 'Y'
    group by po_num
),
-- Get list of all in-force policies under those non-Agency-policy customers
all_pol_cus as (
    select  nap.po_num, pol.dist_chnl_cd, pol.pol_num, pol.wa_code, pol.sa_code, pol.pol_eff_dt, nap.banca_ind,
            case when nap.pol_num=pol.pol_num then 1 else 0 end oldest_pol_ind
    from    base_pol_cus nap inner join
            all_inf_pol pol on nap.po_num=pol.po_num
    --where   nap.rn=1
),
-- Retrieve all addresses from the policies linked
frst_pol_addr as (
select  pol.po_num, dist_chnl_cd, pol.pol_num, wa_code, sa_code, pol_eff_dt, banca_ind, oldest_pol_ind,
        'Mailing' ADDR_TYP, concat_ws(', ', mla.ADDR_1, mla.ADDR_2, mla.ADDR_3, mla.ADDR_4) REGISTERED_ADDR
from    all_pol_cus pol inner join
        vn_published_cas_db.tclient_policy_links  cpl on pol.pol_num=cpl.POL_NUM inner join
        vn_published_cas_db.tclient_addresses     mla on cpl.CLI_NUM=mla.CLI_NUM and cpl.ADDR_TYP=mla.ADDR_TYP                                                     
where   1=1
  and   cpl.LINK_TYP='O' and cpl.REC_STATUS='A'
union
select  pol.po_num, dist_chnl_cd, pol.pol_num, wa_code, sa_code, pol_eff_dt, banca_ind, oldest_pol_ind,
        'Residence' ADDR_TYP, concat_ws(', ', pma.ADDR_1, pma.ADDR_2, pma.ADDR_3, pma.ADDR_4) REGISTERED_ADDR
from    all_pol_cus pol inner join
        vn_published_cas_db.tclient_policy_links  cpl on pol.pol_num=cpl.POL_NUM inner join
        vn_published_cas_db.tclient_addresses     pma on cpl.CLI_NUM=pma.CLI_NUM and cpl.RES_ADDR_TYP=pma.ADDR_TYP                                                      
where   1=1
  and   cpl.LINK_TYP='O' and cpl.REC_STATUS='A'
  and   cpl.ADDR_TYP <> cpl.RES_ADDR_TYP
),
tcb_onb as (
    select  po_num,
            a.dist_chnl_cd,
            c.comp_prvd_num,
            b.loc_code wa_loc,
            c.loc_code sa_loc,
            case when c.trmn_dt is not null or c.comp_prvd_num='04' then c.br_code else c.agt_code end agent,
            case when c.trmn_dt is not null or c.comp_prvd_num='04' then 1 else 0 end ucm_ind,
            a.pol_num,
            a.oldest_pol_ind,
            d.cli_nm po_name,
            d.email_addr,
            d.mobl_phon_num, d.prim_phon_num, d.othr_phon_num,
            a.addr_typ, a.registered_addr
    from    frst_pol_addr a inner join
            vn_curated_datamart_db.tcustdm_daily d on a.po_num=d.cli_num left join
            vn_published_ams_db.tams_agents  b on a.wa_code=b.agt_code left join
            vn_published_ams_db.tams_agents  c on a.sa_code=c.agt_code 
    where   1=1
        and (b.loc_code like 'TCB%' or
             c.loc_code like 'TCB%' or
             a.dist_chnl_cd in ('10','24','49')
             )
),
vtb_onb as (
    select  po_num,
            a.dist_chnl_cd,
            c.comp_prvd_num,
            b.loc_code wa_loc,
            c.loc_code sa_loc,
            case when c.trmn_dt is not null or c.comp_prvd_num='04' then c.br_code else c.agt_code end agent,
            case when c.trmn_dt is not null or c.comp_prvd_num='04' then 1 else 0 end ucm_ind,
            a.pol_num,
            a.oldest_pol_ind,
            d.cli_nm po_name,
            d.email_addr,
            d.mobl_phon_num, d.prim_phon_num, d.othr_phon_num,
            a.addr_typ, a.registered_addr
    from    frst_pol_addr a inner join
            vn_curated_datamart_db.tcustdm_daily d on a.po_num=d.cli_num left join
            vn_published_ams_db.tams_agents  b on a.wa_code=b.agt_code left join
            vn_published_ams_db.tams_agents  c on a.sa_code=c.agt_code 
    where   1=1
        and (b.loc_code like 'VTI%' or
             c.loc_code like 'VTI%' or
             a.dist_chnl_cd in ('52','53')
             )
),
agency_pol as (
    select  po_num,
            a.dist_chnl_cd,
            c.comp_prvd_num,
            b.loc_code wa_loc,
            c.loc_code sa_loc,
            case when c.trmn_dt is not null or c.comp_prvd_num in ('04','97','98') then c.br_code else c.agt_code end agent,
            case when c.trmn_dt is not null or c.comp_prvd_num in ('04','97','98') then 1 else 0 end ucm_ind,
            a.pol_num,
            a.oldest_pol_ind,
            d.cli_nm po_name,
            d.email_addr,
            d.mobl_phon_num, d.prim_phon_num, d.othr_phon_num,
            a.addr_typ, a.registered_addr
    from    frst_pol_addr a inner join
            vn_curated_datamart_db.tcustdm_daily d on a.po_num=d.cli_num left join
            vn_published_ams_db.tams_agents  b on a.wa_code=b.agt_code left join
            vn_published_ams_db.tams_agents  c on a.sa_code=c.agt_code 
    where   1=1
        and (b.comp_prvd_num in ('01','04','97','98') or
             c.comp_prvd_num in ('01','04','97','98') or
             a.dist_chnl_cd in ('*','01','02','08','50')
             )
),
banca_onb as (
    select  *
    from    tcb_onb
    union   
    select  *
    from    vtb_onb
),
agency_onb as (
    select  a.*
    from    agency_pol a left join
            banca_onb b on a.po_num = b.po_num --and a.pol_num <> b.pol_num -- Only select different pol_num from the same po_num
    where   b.po_num is null
), 
-- Merge customers whose policies issued via different channels
all_onb as (
    select  *
    from    banca_onb
    union
    select  *
    from    agency_onb
    where   email_addr is not null or
            mobl_phon_num is not null or
            prim_phon_num is not null or
            othr_phon_num is not null or
            registered_addr is not null
)
-- Tag Bank indicator to each customer selected
select  t.*, nvl(s.banca_ind,'Agency') as banca_ind
from    all_onb t left join
        unique_banca_cus s on t.po_num=s.po_num
;
'''

mkt_cus = sql_to_df(sql_string, 1, spark)

# Add the banca_cat column with specified logic
mkt_cus = mkt_cus.withColumn(
    "banca_cat",
    F.when(
        (F.coalesce(F.col("wa_loc"), F.lit("")).like("VTI%")) & 
        (F.coalesce(F.col("sa_loc"), F.lit("")).like("VTI%")), 
        "2.1 Onboarded and served by VTB"
    )
    .when(
        F.coalesce(F.col("wa_loc"), F.lit("")).like("VTI%"), 
        F.when(
            F.coalesce(F.col("sa_loc"), F.lit("")).like("VTI%"), 
            "2.1 Onboarded and served by VTB"
        )
        .when(
            (~F.coalesce(F.col("sa_loc"), F.lit("")).like("VTI%")) & 
            (F.coalesce(F.col("dist_chnl_cd"), F.lit("")).isin("52", "53")), 
            "2.2 Onboarded by VTB"
        )
    )
    .when(
        F.coalesce(F.col("sa_loc"), F.lit("")).like("VTI%"), 
        "2.3 Served by VTB"
    )
    .when(
        F.coalesce(F.col("dist_chnl_cd"), F.lit("")).isin("52", "53"), 
        "2.2 Onboarded by VTB"
    )
    .when(
        (F.coalesce(F.col("wa_loc"), F.lit("")).like("TCB%")) & 
        (F.coalesce(F.col("sa_loc"), F.lit("")).like("TCB%")), 
        "1.1 Onboarded and served by TCB"
    )
    .when(
        F.coalesce(F.col("wa_loc"), F.lit("")).like("TCB%"), 
        F.when(
            F.coalesce(F.col("sa_loc"), F.lit("")).like("TCB%"), 
            "1.1 Onboarded and served by TCB"
        )
        .when(
            (~F.coalesce(F.col("sa_loc"), F.lit("")).like("TCB%")) & 
            (F.coalesce(F.col("dist_chnl_cd"), F.lit("")).isin("10", "24", "49")), 
            "1.2 Onboarded by TCB"
        )
    )
    .when(
        F.coalesce(F.col("sa_loc"), F.lit("")).like("TCB%"), 
        "1.3 Served by TCB"
    )
    .when(
        F.coalesce(F.col("dist_chnl_cd"), F.lit("")).isin("10", "24", "49"), 
        "1.2 Onboarded by TCB"
    )
    .when(
        (F.coalesce(F.col("dist_chnl_cd"), F.lit("")).isin("*", "01", "02", "08", "50")) & 
        (F.coalesce(F.col("comp_prvd_num"), F.lit("")).isin("01", "97", "98")), 
        "3.1 Onboarded and served by Agency"
    )
    .when(
        F.coalesce(F.col("dist_chnl_cd"), F.lit("")).isin("*", "01", "02", "08", "50"), 
        F.when(
            F.coalesce(F.col("comp_prvd_num"), F.lit("")).isin("01", "97", "98"), 
            "3.1 Onboarded and served by Agency"
        )
        .when(
            (~F.coalesce(F.col("comp_prvd_num"), F.lit("")).isin("01", "97", "98")) & 
            (F.coalesce(F.col("dist_chnl_cd"), F.lit("")).isin("*", "01", "02", "08", "50")), 
            "3.2 Onboarded by Agency"
        )
    )
    .when(
        F.coalesce(F.col("comp_prvd_num"), F.lit("")).isin("01", "97", "98"), 
        "3.3 Served by Agency"
    )
    .when(
        F.coalesce(F.col("dist_chnl_cd"), F.lit("")).isin("*", "01", "02", "08", "50"), 
        "3.2 Onboarded by Agency"
    )
    .otherwise("Other")
)
mkt_cus = mkt_cus.fillna({"banca_cat": "Other"})

mkt_cus_sum = mkt_cus.groupBy("banca_ind").agg(F.count("pol_num").alias("no_policies"), F.countDistinct("po_num").alias("no_customers")).orderBy(F.desc("no_customers"))

display(mkt_cus_sum)

# COMMAND ----------

mkt_cus.filter(F.col("banca_cat") == "Other").limit(10).display()

# COMMAND ----------

df = mkt_cus.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Contactability Validation

# COMMAND ----------

import re

# Define a function to check if the email address is valid
def is_valid_email(email):
    if pd.isnull(email):
        return "No Email"
    elif '@' in email and '.' in email and len(email)>5:
        return "Valid Email"
    else:
        return "Invalid Email"

# Function to check if a phone number is valid
def is_valid_phone(row):
    def clean_number(number):
        return re.sub(r'\D', '', number).strip() if number else ''
    
    mobl_phon_num = clean_number(row['mobl_phon_num'])
    prim_phon_num = clean_number(row['prim_phon_num'])
    othr_phon_num = clean_number(row['othr_phon_num'])
    
    if not mobl_phon_num and not prim_phon_num and not othr_phon_num:
        return "No Phone"
    elif len(mobl_phon_num) == 10:
        return "Valid Phone"
    elif len(prim_phon_num) >= 10 or len(othr_phon_num) >= 10:
        return "Valid Phone"
    else:
        return "No Valid Phone"

# Function to check if the address is valid
def is_valid_addr(row):
    if pd.isnull(row['registered_addr']) or row['registered_addr'].strip() == '':
        return "No Address"
    else:
        return f"Valid {row['addr_typ']} Address"

# Apply the function to create a new column 'email_status'
df['email_status'] = df['email_addr'].apply(is_valid_email)

# Apply the function to create a new column 'phone_status'
df['phone_status'] = df.apply(is_valid_phone, axis=1)

# Apply the function to create a new column 'addr_status'
df['addr_status'] = df.apply(is_valid_addr, axis=1)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Double checking/sizing

# COMMAND ----------

# Group by 'email_status', 'phone_status' and 'addr_status', and count the occurrences
#result = df.groupby(['email_status', 'phone_status', 'addr_status']).size().reset_index(name='customer_count')
result = df.groupby(['email_status', 'phone_status', 'addr_status'])['po_num'].nunique().reset_index(name='customer_count')

# Pivot the table to get the desired format
result_pivot = result.pivot_table(index=['phone_status'], columns=['email_status', 'addr_status'], values='customer_count').fillna(0)

# Ensure the pivot table shows the correct labels
result_pivot.index.name = 'Phone Status'
#result_pivot.columns.name = 'Email Status'

result_pivot

# COMMAND ----------

# MAGIC %md
# MAGIC Checking TCB customers

# COMMAND ----------

#tcb = df[df['banca_ind'] == 'TCB'].drop_duplicates(subset='po_num')

# Group by 'email_status', 'phone_status' and 'addr_status', and count the occurrences
#result = tcb.groupby(['email_status', 'phone_status', 'addr_status'])['po_num'].nunique().reset_index(name='customer_count')

# Pivot the table to get the desired format
#result_pivot = result.pivot_table(index=['phone_status'], columns=['email_status', 'addr_status'], values='customer_count').fillna(0)

# Ensure the pivot table shows the correct labels
#result_pivot.index.name = 'Phone Status'
#result_pivot.columns.name = 'Email Status'

#result_pivot

# COMMAND ----------

# MAGIC %md
# MAGIC Checking UCM customers

# COMMAND ----------

ucm_result = df.groupby(['banca_ind','ucm_ind']).size().reset_index(name='policy_count')

display(ucm_result)

# COMMAND ----------

#tmp = df[(df['email_status'] == 'No Email') &
#         (df['phone_status'].isin(['No Phone', 'No Valid Phone']))]

#tmp.to_csv(f'/dbfs/mnt/lab/vn/project/scratch/adhoc/Missing_emails_phones_banca_customers.csv', index=False, header=True, encoding='utf-8-sig')

# COMMAND ----------

df.to_csv(f'/dbfs/mnt/lab/vn/project/scratch/adhoc/MKT_comm_to_banca_customers.csv', index=False, header=True, encoding='utf-8-sig')
