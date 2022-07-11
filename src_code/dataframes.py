from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

ticker_symbols = ["A","R","M","ACER","AAPL","U","F","Y","W","D","H","SPY","V","G","K","L","X","SO","Z","FIND","KO","PK",
	"MEME", "T","ALL","ITC"]


spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
file_base_path = '/Users/adityagupta/PycharmProjects/python_spark_assignment/csv_files/'

val = 0
new_df = " "

for file in ticker_symbols:

	file_full_path = file_base_path + file + ".csv"
	df = spark.read.csv(file_full_path,header=True)
	if val == 0:
		val = 1
		new_df = df.withColumn("stock_name",lit(file))
	else:
		df = df.withColumn("stock_name",lit(file))
		new_df = new_df.unionByName(df)

# print(new_df.count())
# print(new_df.show(1300,False))
# new_df.write.format("csv").mode("overwrite").save(file_base_path+'output.csv')



