import findspark
findspark.init()

from pyspark.sql import SparkSession

from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[4]")\
                            .appName("spark_session")\
                            .getOrCreate()

sc = spark.sparkContext

employee_df = spark.read.format("csv")\
    .option("header","true")\
        .option("inferschema","true")\
            .option("mode","PERMISSIVE")\
                .load("C:\\Users\\TARUN\\Desktop\\Pyspark\\2010-summary.csv")

employee_df.show()