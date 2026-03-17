# Databricks notebook source
#--Quick Check

#spark.sql("select distinct post_evar37 AS customerID,CONCAT(post_visid_high, '-', post_visid_low) AS visitor_id, user_server, case when user_server = 'hopdongcuatoi.manulife.com.vn' then 'CWS' when user_server = 'www.manulife.com.vn' then 'PWS' when user_server = 'boithuongbaohiem.manulife.com.vn' then 'eClaim' end as domain from hit_data where user_server = 'hopdongcuatoi.manulife.com.vn' or user_server = 'boithuongbaohiem.manulife.com.vn' or user_server = 'www.manulife.com.vn'").display()

# COMMAND ----------

last_mthend = '2025-01-31'

spark.read.format('parquet').load(f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_ADOBE_PWS_DB/HIT_DATA/partition_dt=2025-**-**').createOrReplaceTempView("hit_data")
#spark.read.format('parquet').load(f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_ADOBE_PWS_DB/OPERATING_SYSTEMS/').createOrReplaceTempView("operating_systems")
#spark.read.format('parquet').load(f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_ADOBE_PWS_DB/BROWSER/').createOrReplaceTempView("browser")
#spark.read.format('parquet').load(f'/mnt/prod/Published/VN/Master/VN_PUBLISHED_ADOBE_PWS_DB/REFERRER_TYPE/').createOrReplaceTempView("referrer_type")

# COMMAND ----------

#0. All visitor hits
all_visitor = spark.sql(f"""
select distinct mcvisid as ecid
     , CASE WHEN exclude_hit = '0' AND hit_source NOT IN ('5', '7', '8', '9') 
            THEN CONCAT(post_visid_high, post_visid_low)
            ELSE NULL 
       END AS visitor_id
     , CASE WHEN exclude_hit = '0' AND hit_source NOT IN ('5', '7', '8', '9') 
            THEN CONCAT(post_visid_high, post_visid_low, visit_num)
            ELSE NULL 
       END AS visit_id
     , CAST (visit_num AS INT) AS visit_num
     , CAST (visit_page_num AS INT) AS visit_page_num
     , CASE WHEN exclude_hit = '0' AND hit_source NOT IN ('5', '7', '8', '9') 
            THEN CONCAT(date_time, post_visid_high, post_visid_low, visit_num, visit_page_num)
            ELSE NULL
       END AS hit_id
     , date_time
     , visit_start_time_gmt
     , post_cust_hit_time_gmt
     ,CASE WHEN (user_server = 'hopdongcuatoi.manulife.com.vn') then 'CWS' WHEN user_server = 'www.manulife.com.vn' then  'PWS' ELSE  'Others' END AS server
     , post_pagename as pagename
     , post_evar16 AS page_url
     , geo_country  AS country
     , geo_city AS city
     , post_evar37                                                   AS customerID
      from hit_data
      where date(date_time) >= '2025-01-01' and date(date_time) <= '{last_mthend}';
      """)
all_visitor.createOrReplaceTempView("all_visitor_hits")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view all_visitors_w_time AS
# MAGIC SELECT DISTINCT ecid
# MAGIC      , visitor_id
# MAGIC      , visit_id
# MAGIC      , visit_num
# MAGIC      , visit_page_num
# MAGIC      , hit_id
# MAGIC      , date_time
# MAGIC      , visit_start_time_gmt
# MAGIC      , post_cust_hit_time_gmt
# MAGIC      , lead(date_time) OVER (PARTITION BY CONCAT(visit_id, visit_start_time_gmt)
# MAGIC                              ORDER BY visitor_id
# MAGIC                                     , CAST (visit_num AS INT)
# MAGIC                                     , CAST (visit_page_num AS INT)
# MAGIC                             ) AS next_hit_time
# MAGIC      , lead(post_cust_hit_time_gmt) OVER (PARTITION BY CONCAT(visit_id, visit_start_time_gmt)
# MAGIC                                           ORDER BY visitor_id
# MAGIC                                                  , CAST (visit_num AS INT)
# MAGIC                                                  , CAST (visit_page_num AS INT)
# MAGIC                                          ) AS next_hit_time_gmt
# MAGIC      , lag(date_time) OVER (PARTITION BY CONCAT(visit_id, visit_start_time_gmt)
# MAGIC                             ORDER BY visitor_id
# MAGIC                                    , CAST (visit_num AS INT)
# MAGIC                                    , CAST (visit_page_num AS INT)
# MAGIC                            ) AS last_hit_time
# MAGIC      , lag(post_cust_hit_time_gmt) OVER (PARTITION BY CONCAT(visit_id, visit_start_time_gmt)
# MAGIC                                          ORDER BY visitor_id
# MAGIC                                                 , CAST (visit_num AS INT)
# MAGIC                                                 , CAST (visit_page_num AS INT)
# MAGIC                                         ) AS last_hit_time_gmt
# MAGIC      , server
# MAGIC      , pagename
# MAGIC      , page_url
# MAGIC      , country
# MAGIC      , city
# MAGIC      , customerID
# MAGIC FROM all_visitor_hits
# MAGIC ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC create or replace temp view digital_w_time_customeridmap_tmp1 as
# MAGIC select visit_id
# MAGIC      , visitor_id
# MAGIC      , visit_num
# MAGIC      , customerID
# MAGIC      , min(visit_page_num) as visit_page_num_min
# MAGIC      , max(visit_page_num) as visit_page_num_max
# MAGIC from all_visitors_w_time
# MAGIC where not (visit_id is null or visit_id = '' or visitor_id is null or visitor_id = '')
# MAGIC group by visit_id, visitor_id, visit_num, customerID
# MAGIC order by visit_id, visit_page_num_min
# MAGIC ;
# MAGIC
# MAGIC create or replace temp view digital_w_time_customeridmap_tmp2 as
# MAGIC select a.*
# MAGIC      , case when (a.customerID is null or a.customerID = '') then b.customerID else a.customerID end as customerid_map1
# MAGIC from digital_w_time_customeridmap_tmp1 as a
# MAGIC left join (select visit_id, customerID, visit_page_num_min from digital_w_time_customeridmap_tmp1) as b 
# MAGIC        on a.visit_id=b.visit_id and a.visit_page_num_max+1=b.visit_page_num_min
# MAGIC order by a.visitor_id, a.visit_num, a.visit_page_num_min
# MAGIC ;
# MAGIC
# MAGIC create or replace temp view digital_w_time_customeridmap_tmp3 as
# MAGIC select visitor_id
# MAGIC      , max(visit_num) as visit_num_max
# MAGIC from digital_w_time_customeridmap_tmp2
# MAGIC where not (customerid_map1 is null or customerid_map1 = '')
# MAGIC group by visitor_id
# MAGIC ;
# MAGIC
# MAGIC create or replace temp view digital_w_time_customeridmap_tmp4 as
# MAGIC select distinct visit_id, customerid_map1 as customerid_map2
# MAGIC from digital_w_time_customeridmap_tmp2
# MAGIC where visit_id in ( select trim(visitor_id) || visit_num_max as visit_id from digital_w_time_customeridmap_tmp3)
# MAGIC ;
# MAGIC
# MAGIC create or replace temp view digital_w_time_customeridmap_tmp5 as
# MAGIC select a.*
# MAGIC      , case when (a.customerid_map1 is null or a.customerid_map1 = '') then b.customerid_map2 else a.customerid_map1 end as customerid_map
# MAGIC from digital_w_time_customeridmap_tmp2 as a
# MAGIC left join (select visit_id, customerid_map2 from digital_w_time_customeridmap_tmp4) as b
# MAGIC        on a.visit_id=b.visit_id
# MAGIC order by a.visitor_id, a.visit_num, a.visit_page_num_min
# MAGIC ;
# MAGIC
# MAGIC create or replace temp view digital_w_time_customeridmap as
# MAGIC select distinct visit_id
# MAGIC      , customerid_map 
# MAGIC from digital_w_time_customeridmap_tmp5
# MAGIC where not (customerid_map is null or customerid_map = '') and (customerID is null or customerID = '')
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view digital_w_time_traffic_all as
# MAGIC with final as(
# MAGIC select distinct b.ecid
# MAGIC               , b.visitor_id
# MAGIC               , b.visit_id
# MAGIC               , b.visit_num
# MAGIC               , b.visit_page_num
# MAGIC               , b.hit_id
# MAGIC               , b.date_time
# MAGIC               , b.visit_start_time_gmt
# MAGIC               , b.post_cust_hit_time_gmt
# MAGIC               , b.next_hit_time
# MAGIC               , CAST(b.next_hit_time_gmt AS INT) AS next_hit_time_gmt
# MAGIC               , b.last_hit_time
# MAGIC               , CAST(b.last_hit_time_gmt AS INT) AS last_hit_time_gmt
# MAGIC               , ABS(CAST(b.next_hit_time_gmt AS INT) - NVL(b.post_cust_hit_time_gmt, b.date_time)) AS time_on_page
# MAGIC               , b.server
# MAGIC               , b.pagename
# MAGIC               , b.page_url
# MAGIC               , case when (b.customerID is null or b.customerID = '') and c.customerid_map is not null 
# MAGIC                      then c.customerid_map else b.customerID end as cid_finl
# MAGIC from          all_visitors_w_time as b
# MAGIC left join     digital_w_time_customeridmap as c on b.visit_id=c.visit_id
# MAGIC ), cws_account as (
# MAGIC select        MCF_USER_ID__PC as cws_id, external_id__c as cli_num 
# MAGIC from          VN_PUBLISHED_SFDC_EASYCLAIMS_DB.ACCOUNT 
# MAGIC where         MCF_USER_ID__PC is not null
# MAGIC )
# MAGIC select        cws.cli_num,
# MAGIC               fnl.*
# MAGIC from          final fnl
# MAGIC inner join    cws_account cws on fnl.cid_finl = cws.cws_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select  distinct 
# MAGIC         cli_num
# MAGIC         , server
# MAGIC         , pagename
# MAGIC         , date_time
# MAGIC         , last_hit_time
# MAGIC         , next_hit_time
# MAGIC         , time_on_page
# MAGIC from    digital_w_time_traffic_all         
# MAGIC where   server='PWS'
# MAGIC   and   lower(trim(pagename)) like '%san%pham%'
# MAGIC   and   time_on_page >= 30

# COMMAND ----------


