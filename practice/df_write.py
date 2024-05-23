# Necessary libraries for PySpark
from pyspark.sql import SparkSession

def main():
    """
    Main function to demonstrate PySpark DataFrame creation and CSV writing.
    """
    # Creating Spark session
    spark = SparkSession.builder.master("local[5]").appName("demo").getOrCreate()

    
    employee_df = spark.read.format("csv")\
        .option("header","true")\
            .option("inferschema","true")\
                .option("mode","PERMISSIVE")\
                    .load(r"C:\Users\TARUN\Desktop\practice\employee.csv")

    employee_df.show()
    
    employee_df.printSchema()

    employee_df.write.format("csv")\
    .mode("overwrite")\
        .option("header","true")\
            .option("path",r"C:\Users\TARUN\Desktop\practice\csv_write")\
                .save()
                
    spark.stop()

if __name__ == '__main__':
    main()
