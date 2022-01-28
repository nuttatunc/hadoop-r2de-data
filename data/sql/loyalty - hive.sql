CREATE EXTERNAL TABLE loyalty(
cust_id int
,cust_name string
,cust_member_card_no string
,spnd_amt int
) 
PARTITIONED BY (data_dt string)
STORED AS PARQUET
LOCATION'hdfs://quickstart.cloudera:8020/tmp/default/loyalty/'