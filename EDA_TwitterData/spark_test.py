from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Intellj_J").master("local").getOrCreate()
spark.read.format("csv").load("test.csv").show(10,False)