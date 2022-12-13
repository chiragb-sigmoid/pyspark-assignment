from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql import functions as func
import logging


logging.basicConfig(level=logging.INFO)


def createSparkSession():
    logging.info("Creating spark session....")
    spark_session = SparkSession.builder.appName('PySpark Assignment').getOrCreate()
    spark_session.sparkContext.setLogLevel('ERROR')
    logging.info("Spark session created successfully!")
    return spark_session


def sparkDataAnalysis():
    # Spark queries

    spark = createSparkSession()
    df = spark.read.csv('/Users/chiragbansal/PycharmProjects/pythonProject/CovidData.csv', header=True)

    logging.info(f"Dataframe Schema ")
    df.printSchema()

    schema1 = StructType([
        StructField("total", IntegerType())
    ])

    schema2 = StructType([
        StructField("recovered", IntegerType())
    ])

    schema3 = StructType([
        StructField("critical", IntegerType())
    ])

    # Handling null values in dataset

    df = df.fillna({'continent': 'NA', 'population': '0'})
    df = df.withColumn('deaths', func.regexp_replace('deaths', 'None', '0'))\
        .withColumn('cases', func.regexp_replace('cases', 'None', '0'))\
        .withColumn("total_cases", func.from_json(func.col("cases"), schema1))\
        .withColumn("critical_cases", func.from_json(func.col("cases"), schema3))\
        .withColumn("recovered_cases", func.from_json(func.col("cases"), schema2))\
        .withColumn("total_deaths", func.from_json(func.col("deaths"), schema1))

    logging.info(f"Dataframe : ")
    df.show()

    # Most affected country

    df.select("country", "total_deaths", "total_cases", (func.col("total_deaths")["total"] / func.col("total_cases")["total"])
              .alias("death_rate")).orderBy("death_rate", ascending=[False]).show(1, 0)

    # Least affected country

    df.select("country", "total_deaths", "total_cases", (func.col("total_deaths")["total"] / func.col("total_cases")["total"])
              .alias("death_rate")).orderBy("death_rate", ascending=[True]).show(1, 0)

    # Country with most cases

    df.select("country", "total_cases").orderBy("total_cases", ascending=[False]).show(1, 0)

    # Country with least cases

    df.select("country", "total_cases").orderBy("total_cases", ascending=[True]).show(1, 0)

    # Total cases

    df.select(func.sum(func.col("total_cases")["total"])).show(1, 0)

    # Country that handled the covid most efficiently

    df.select("country", "recovered_cases", "total_cases",
              (func.col("recovered_cases")["recovered"] / func.col("total_cases")["total"])
              .alias("recovery_rate")).orderBy("recovery_rate", ascending=[False]).show(1, 0)

    # Country that handled the covid least efficiently

    df.select("country", "recovered_cases", "total_cases",
              (func.col("recovered_cases")["recovered"] / func.col("total_cases")["total"])
              .alias("recovery_rate")).orderBy("recovery_rate", ascending=[True]).show(1, 0)

    # Country least suffering from covid

    df.select("country", "critical_cases").orderBy("critical_cases", ascending=[True]).show(1, 0)

    # Country still suffering from covid

    df.select("country", "critical_cases").orderBy("critical_cases", ascending=[False]).show(1, 0)
