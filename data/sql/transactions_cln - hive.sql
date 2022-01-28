CREATE EXTERNAL TABLE transactions_cln(
cust_id int
,odr_menu string
,odr_prc int
,odr_tms string
) 
STORED AS PARQUET
LOCATION'hdfs://quickstart.cloudera:8020/tmp/default/transactions_cln/'