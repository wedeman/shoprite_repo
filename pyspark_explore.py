%pyspark

#pyspark_explore.py 
from pyspark.sql import *


in_path = ('s3://my-shoprite/stage/' +'*.parquet')
out_path = 's3://my-shoprite/explore/'
    
df = spark.read.format('parquet').option('header','true').load(in_path)
newdf = df.withColumn('amount',df.Quantity*df.UnitPrice)
newdf.show(10)
