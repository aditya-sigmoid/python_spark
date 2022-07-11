from flask import Flask, jsonify
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext

from src_code.dataframes import new_df

spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

app = Flask(__name__)

new_df.createOrReplaceTempView("stocks_data")

# print(new_df)

@app.route('/')
def home():
    pdf = spark.sql("SELECT * FROM stocks_data")
    data = (pdf.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'data': data})


########### query 1 ###################
@app.route('/query1')
def max_percentage():
    pass


########## query 2 ################

@app.route('/query2')
def most_traded_stock():
    
    query2 = "Select Date,stock_name ,Volume from stocks_data where Volume is not null AND Date is not null and"\
            "(Date, Volume) IN (Select Date, MAX(Volume) " \
            "from stocks_data  " \
            "group by Date) order by Date"
    answer = spark.sql(query2)
    answer = (answer.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'data': answer})

# print(most_traded_stock())

########## query 3 #####################

@app.route('/query3')
def max_gap():
    pass



###### query 4 ##########

@app.route('/query4')
def max_diff_stock():
    query = "select distinct stock_name, abs((first_value(Open) over (partition by stock_name order by Date asc) - " \
            "first_value(Close) over (partition by stock_name order by Date desc))) as diff " \
            "from stocks_data "
    answer = spark.sql(query)
    # print(answer.show())
    answer.createOrReplaceTempView("max_stock_table")
    query2 = "select stock_name, diff from max_stock_table order by diff desc limit(1)"
    answer = spark.sql(query2)
    answer = (answer.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'data': answer})

# max_diff_stock()


###### query 5 ###########

@app.route('/query5')
def standard_deviation():
    query = "Select stock_name, stddev(Volume) as Standard_deviation from stocks_data group by stock_name"
    answer = spark.sql(query)
    answer = (answer.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'data': answer})

# standard_deviation()


########## query 6 #############

@app.route('/query6')
def mean_median_stock():

    query = "SELECT stock_name, percentile_approx(Open, 0.5) as median_open,percentile_approx(Close, " \
            "0.5) as median_close, mean(Open) as mean_of_Open,mean(Close) as mean_of_Close FROM stocks_data GROUP BY " \
            "stock_name "
    answer = spark.sql(query)
    answer = (answer.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'data': answer})

# mean_median_stock()


######### query 7 ################

@app.route("/query7")
def average_stock_volume_over_period():
    query = "select stock_name, AVG(Volume) as Average_of_stock from table group by stock_name"
    answer = spark.sql(query)
    answer = (answer.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'data': answer})

# average_stock_volume_over_period()

###### query 8 #################

@app.route('/query8')
def highest_average_volume():

    query = "SELECT stock_name, AVG(Volume) as avg_vol from stocks_data " \
            "group by stock_name order by avg_vol desc limit 1"
    answer = spark.sql(query)
    answer = (answer.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'data': answer})

# highest_average_volume()


####### query 9 ###############

@app.route('/query9')
def highest_lowest_stock():

    query = "Select stock_name,max(High) as Highest, min(Low) as Lowest from " \
            "stocks_data group by stock_name"
    answer = spark.sql(query)
    answer = (answer.select('*').rdd.flatMap(lambda x: x).collect())
    return jsonify({'data': answer})

# highest_lowest_stock()

if __name__ == '__main__':
    app.run(debug=True)




