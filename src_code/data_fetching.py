import csv
import requests
import json

ticker_symbols = ["A","R","M","ACER","AAPL","U","F","Y","W","D","H","SPY","V","G","K","L","X","SO","Z","FIND","KO","PK","MEME",
				  "T","ALL","ITC"]



url = "https://stock-market-data.p.rapidapi.com/stock/historical-prices"
headers = {
	"X-RapidAPI-Key": "bead41229dmshc0b659a5652458ep14f4b9jsn4b3015e6d74d",
	"X-RapidAPI-Host": "stock-market-data.p.rapidapi.com"
}

file_base_path = "/Users/adityagupta/PycharmProjects/python_spark_assignment/csv_files/"

for ticker_value in ticker_symbols:

	querystring = {"ticker_symbol":ticker_value,"years":"5","format":"json"}
	response = requests.request("GET", url, headers=headers, params=querystring)
	query = json.loads(response.text)

	# open the file in the write mode

	file_name = file_base_path + ticker_value + '.csv'

	csvfile = open(file_name, "w")

	csvwriter = csv.writer(csvfile)

	list = ["Open", "High","Low","Close","Adj Close","Volume", "Date"]

	csvwriter.writerow(list)

	count = 1

	for result in query['historical prices']:

		lis = [result["Open"],result["High"],result["Low"],result["Close"],result["Adj Close"],result["Volume"],result["Date"][:10]]
		csvwriter.writerow(lis)
		count += 1
		print(result)