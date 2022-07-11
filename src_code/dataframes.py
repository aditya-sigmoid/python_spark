from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

# list of all stocks
ticker_symbols = ["A","R","M","ACER","AAPL","U","F","Y","W","D","H","SPY","V","G","K","L","X","SO","Z","FIND","KO","PK",
	"MEME", "T","ALL","ITC"]


spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

# folder/directory address of all the files
file_base_path = '/Users/adityagupta/PycharmProjects/python_spark_assignment/csv_files/'

val = 0
new_df = " "

for file in ticker_symbols:

	# specific path of file
	file_full_path = file_base_path + file + ".csv"

	# inferschema used here to maintain the datatype of the coulmns
	df = spark.read.csv(file_full_path,header=True,inferSchema=True)

	if val == 0:
		val = 1
		# adding a new column with the stock name
		new_df = df.withColumn("stock_name",lit(file))
	else:
		# adding a new column with the stock name
		df = df.withColumn("stock_name",lit(file))

		# used to join all the dataframes into one
		new_df = new_df.unionByName(df)





