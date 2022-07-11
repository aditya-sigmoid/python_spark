import json

from flask import Flask, jsonify
from pyspark.sql import SparkSession
from src_code.business_logic import max_percentage_stocks, trending_stock, max_gap_stock, maximum_difference_stock, \
    standard_dev_stock, median_mean_stock, average_volume_stock, highest_average_volume_stock, highest_lowest_stock_name
from src_code.dataframes import new_df


spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

app = Flask(__name__)

new_df.createOrReplaceTempView("stocks_data")


@app.route('/')
def home():
    pdf = spark.sql("SELECT * FROM stocks_data")
    return jsonify(json.loads(pdf.toPandas().to_json(orient="table",index=False)))


########### query 1 ###################

@app.route('/query1')

def max_percentage():
    answer = max_percentage_stocks()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table",index=False)))



########## query 2 ################

@app.route('/query2')
def most_traded_stock():
    answer = trending_stock()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table",index=False)))


########## query 3 #####################

@app.route('/query3')
def max_gap():

    answer = max_gap_stock()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table", index=False)))


###### query 4 ##########

@app.route('/query4')
def max_diff_stock():

    answer = maximum_difference_stock()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table", index=False)))


###### query 5 ###########

@app.route('/query5')
def standard_deviation():

    answer = standard_dev_stock()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table", index=False)))


########## query 6 #############

@app.route('/query6')
def mean_median_stock():

    answer = median_mean_stock()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table", index=False)))

######### query 7 ################

@app.route("/query7")
def average_stock_volume_over_period():
    answer = average_volume_stock()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table", index=False)))



###### query 8 #################

@app.route('/query8')
def highest_average_volume():

    answer = highest_average_volume_stock()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table", index=False)))

####### query 9 ###############

@app.route('/query9')
def highest_lowest_stock():

    answer = highest_lowest_stock_name()
    return jsonify(json.loads(answer.toPandas().to_json(orient="table", index=False)))



if __name__ == '__main__':
    app.run(debug=True)




