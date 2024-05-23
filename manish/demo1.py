# Necessary libraries for PySpark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def main():
    """
    Main function to demonstrate PySpark DataFrame creation and CSV writing.
    """
    # Creating Spark session
    spark = SparkSession.builder.master("local[5]").appName("demo").getOrCreate()

    # Create a list of data to prepare a DataFrame
    person_list = [
        ("Berry", "", "Allen", 1, "M"),
        ("Oliver", "Queen", "", 2, "M"),
        ("Robert", "", "Williams", 3, "M"),
        ("Tony", "", "Stark", 4, "F"),
        ("Rajiv", "Mary", "Kumar", 5, "F")
    ]

    # Define schema for the dataset
    schema = StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("gender", StringType(), True),
    ])

    # Create a Spark DataFrame
    df = spark.createDataFrame(data=person_list, schema=schema)

    # Print DataFrame schema
    df.printSchema()

    # Print DataFrame data
    df.show(truncate=False)

    # Write DataFrame to a CSV file in Spark context
    df.write.format("csv")\
        .option("header", "true")\
        .mode("overwrite")\
        .option("path", "C:\\Users\\TARUN\\Desktop\\practice\\records")\
        .save()

if __name__ == '__main__':
    main()
