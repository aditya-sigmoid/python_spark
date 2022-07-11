#variable should be more appropriate
# add more comments for better understanding


# this file includes all the business logic

from pyspark.sql import SparkSession


spark = SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()

def max_percentage_stocks():

    '''
         calculating the max percentage and stock name and min percentage with stock name
    '''

    try:
        query = "select Date,Volume,stock_name,Open,Close,High,Low,(((Close-Open)/Close))*100 as max_move from stocks_data"
        df1 = spark.sql(query)
        df1.createOrReplaceTempView("pdata")
        positive_movement = "select t1.Date,t1.stock_name,t1.max_move from pdata t1 join ( select Date, Max(max_move) AS positive_move from pdata Group By Date) t2 on t1.Date = t2.Date and t2.positive_move=t1.max_move"
        df2 = spark.sql(positive_movement)
        df2.createOrReplaceTempView("t1")
        negative_movement = "select t1.Date,t1.stock_name,t1.max_move from pdata t1 join ( select Date, Min(max_move) AS negative_move from pdata Group By Date) t2 on t1.Date = t2.Date and t2.negative_move=t1.max_move"
        df3 = spark.sql(negative_movement)
        df3.createOrReplaceTempView("t2")
        df4 = spark.sql("select t1.Date,t1.stock_name as max_stock_name,t1.max_move as maxPercMove, t2.stock_name  as min_stock_name,t2.max_move as minPercMove from t1 join t2 on t1.Date=t2.Date order by t1.Date")
        return df4
    except Exception as e:
        print("error occurs ",e)

def trending_stock():

    '''  find maximum trading stock according to volume '''

    try:
        query2 = "Select Date,stock_name ,Volume from stocks_data where Volume is not null AND Date is not null and" \
                 "(Date, Volume) IN (Select Date, MAX(Volume) " \
                 "from stocks_data  " \
                 "group by Date) order by Date"
        answer = spark.sql(query2)
        return answer

    except Exception as e:
        print("error occurs ",e)

def max_gap_stock():

    try:
        query3 = "SELECT stock_name, Date, Open, Close , Close- LAG(Open, 1, null) OVER (PARTITION BY stock_name ORDER BY Date) " \
                 "as diff FROM stocks_data "
        pdf = spark.sql(query3)
        pdf.createOrReplaceTempView("new_table")
        query3_1 = "select stock_name, MAX(diff), MIN(diff) from new_table group by stock_name"
        new_pdf = spark.sql(query3_1)
        return new_pdf
    except Exception as e:
        print("error occurs ",e)

def maximum_difference_stock():

    try:
        query = "select distinct stock_name, abs((first_value(Open) over (partition by stock_name order by Date asc) - " \
                "first_value(Close) over (partition by stock_name order by Date desc))) as diff " \
                "from stocks_data "
        answer = spark.sql(query)
        # print(answer.show())
        answer.createOrReplaceTempView("max_stock_table")
        query2 = "select stock_name, diff from max_stock_table order by diff desc limit(1)"
        answer = spark.sql(query2)
        return answer

    except Exception as e:
        print("error is ",e)

def standard_dev_stock():

    try:
        query = "Select stock_name, stddev(Volume) as Standard_deviation from stocks_data group by stock_name"
        answer = spark.sql(query)
        return answer
    except Exception as e:
        print("error is ",e)

def median_mean_stock():

    try:
        query = "SELECT stock_name, percentile_approx(Open, 0.5) as median_open,percentile_approx(Close, " \
                "0.5) as median_close, mean(Open) as mean_of_Open,mean(Close) as mean_of_Close FROM stocks_data GROUP BY " \
                "stock_name "
        answer = spark.sql(query)
        return answer
    except Exception as e:
        print("error is ",e)

def average_volume_stock():
    try:
        query = "select stock_name, AVG(Volume) as Average_of_stock from stocks_data group by stock_name"
        answer = spark.sql(query)
        return answer
    except Exception as e:
        print("error is ",e)

def highest_average_volume_stock():
    try:
        query = "SELECT stock_name, AVG(Volume) as avg_vol from stocks_data " \
                "group by stock_name order by avg_vol desc limit 1"
        answer = spark.sql(query)
        return answer
    except Exception as e:
        print("error is ",e)

def highest_lowest_stock_name():

    ''' from all the tables (all data ) calculate the max and min from it group it using stock name '''
    try:
        query = "Select stock_name , MAX(High) as Highest, MIN(Low) as Lowest from " \
                "stocks_data group by stock_name "

        answer = spark.sql(query)
        return answer
    except Exception as e:
        print("error is ",e)



