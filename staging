%pyspark
from pyspark.sql import *

sales_df = spark.read.csv("s3://shoprite-demo/rawfiles/OnlineRetail.csv", inferSchema = True, header = True)
sales_df.write.format('parquet').option('header','true').save('s3a://my-shoprite/stage/',mode='overwrite')
