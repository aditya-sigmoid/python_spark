# python_spark

Use the API 
https://rapidapi.com/eec19846/api/investors-exchange-iex-trading/
to download data for 100 stocks and save in separate csv files for each stock.

Create Spark data frames by reading the csv files and do following analysis

- On each of the days find which stock has moved maximum %age wise in both directions (+ve, -ve)
- Which stock was most traded stock on each day
- Which stock had the max gap up or gap down opening from the previous day close price I.e. (previous day close -  current day open price )
- Which stock has moved maximum from 1st Day data to the latest day Daya
- Find the standard deviations for each stock over the period
- Mind the mean  and median prices for each stock
- Find the average volume over the period
- Find which stock has higher average volume
- Find the highest and lowest prices for a stock over the period of time

Create REST APIs to query the data for above questions as you could query from the website.

For Spark you can use Scala or Pyspark.
For API again you can use Scala or Python.
