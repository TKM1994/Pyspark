from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("window").getOrCreate()


emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

schema = ['id','name','salary','dept','gender']

employee_df = spark.createDataFrame(data = emp_data ,schema= schema)

employee_df.show()

window = Window.partitionBy("dept").orderBy("salary")

employee_df.withColumn("Row_Number",row_number().over(window))\
    .withColumn("Rank",rank().over(window))\
        .withColumn("Dense_Rank",dense_rank().over(window))\
            .show()
            
            
employee_df.createOrReplaceTempView("employee_df_table")

spark.sql("""
          select *,
          row_number() over(partition by dept order by salary desc) as row_number,
          rank() over(partition by dept order by salary desc) as rank,
          dense_rank() over(partition by dept order by salary desc) as dense_rank
          from employee_df_table
          """).show()

spark.stop()