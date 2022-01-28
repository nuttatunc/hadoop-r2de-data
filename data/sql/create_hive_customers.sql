CREATE TABLE customers(
	  customer_id int, 
	  home_store int, 
	  customer_firstname string, 
	  customer_email string, 
	  customer_since string, 
	  loyalty_card_number string, 
	  birthdate string, 
	  gender string, 
	  birth_year int)
	ROW FORMAT DELIMITED 
	  FIELDS TERMINATED BY ',' 
	  LINES TERMINATED BY '\n' 
	STORED AS INPUTFORMAT 
	  'org.apache.hadoop.mapred.TextInputFormat' 
	OUTPUTFORMAT 
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	LOCATION
	  '/tmp/file/sink/'


