CREATE EXTERNAL TABLE customers_cln(
cust_id int,
cust_nm string
,cust_email string
,cust_strt_dt string
,cust_member_card_no string
,cust_birth_dt string
,cust_birth_yr int
,cust_gender string
) 
STORED AS PARQUET
LOCATION '/tmp/default/customers_cln/'