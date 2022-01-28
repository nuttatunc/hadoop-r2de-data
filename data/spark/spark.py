import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("customers_spark_job").enableHiveSupport().getOrCreate()

cust_df = spark.sql("select * from default.customers")

clean_cust_df = cust_df.withColumn("customer_name", trim(col("customer_firstname"))) \
                    .withColumn("customer_since",to_date(col("customer_since"),"yyyy-MM-dd").cast("string")) \
                    .withColumn("loyalty_card_number",regexp_replace("loyalty_card_number",'\"', '')) \
                    .withColumn("birthdate",to_date(col("birthdate"),"yyyy-MM-dd").cast("string")) \
                    .withColumn("gender",expr("case when gender = 'M' then 'MALE' when gender = 'F' then 'FEMALE' else 'NA' end"))

final_cust_df = clean_cust_df.selectExpr("customer_id as cust_id", \
                                        "customer_name as cust_nm", \
                                        "customer_email as cust_email", \
                                        "customer_since as cust_strt_dt", \
                                        "loyalty_card_number as cust_member_card_no", \
                                        "birthdate as cust_birth_dt", \
                                        "birth_year as cust_birth_yr",\
                                        "gender as cust_gender")

final_cust_df.write.mode("overwrite").save("/tmp/default/customers_cln/")

spark.stop()


