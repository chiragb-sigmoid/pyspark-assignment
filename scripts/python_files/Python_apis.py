import requests
import json
import csv


def fetchData():
    # FETCH DATA FROM API

    url = "https://covid-193.p.rapidapi.com/statistics"

    headers = {
        "X-RapidAPI-Key": "22929a7a86mshedb9ed7d887cc29p17cadejsn256d236fba8d",
        "X-RapidAPI-Host": "covid-193.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)

    result = response.content
    result_data = json.loads(result)['response']
    print(result_data)

    csv_file = "CovidData.csv"
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=result_data[0].keys())
            writer.writeheader()
            # code for analysis only on 25 countries
            count = 0
            while count < 25:
                for data in result_data:
                    writer.writerow(data)
                    count += 1
    except IOError:
        print("I/O error")

