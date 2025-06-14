import os
import json
import pandas as pd
import psycopg2

#top_transaction

columns8 = {"States":[],"Years":[],"Quarter":[],"Pincodes":[],"Transaction_Count":[],"Transaction_Amount":[]}

path8 = "C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/top/transaction/country/india/state/"

top_state_list = os.listdir(path8)

for state in top_state_list:
    state_path = path8 + state + "/"
    top_year_list = os.listdir(state_path)

    for year in top_year_list:
        year_path = state_path + year + "/"
        top_file_list = os.listdir(year_path)

        for file in top_file_list:
            file_path = year_path + file 

            data = open(file_path, "r")
            H = json.load(data)

            for i in H["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns8["States"].append(state)
                columns8["Years"].append(year)
                columns8["Quarter"].append(int(file.strip(".json")))
                columns8["Pincodes"].append(entityName)
                columns8["Transaction_Count"].append(count)
                columns8["Transaction_Amount"].append(amount)

top_transaction = pd.DataFrame(columns8)

top_transaction["States"] = top_transaction["States"].str.replace("-"," ")
top_transaction["States"] = top_transaction["States"].str.title()

connection = psycopg2.connect(host="localhost",port="5432",user="postgres",password="12345",database="PhonePe_DB")
cursor = connection.cursor()

tt_create_q1 = "create table if not exists top_transaction(states varchar(255), years int, quarter int, pincodes bigint, transaction_count bigint, transaction_amount bigint)"
cursor.execute(tt_create_q1)
connection.commit()

tt_insert_q1 = "insert into top_transaction(states, years, quarter, pincodes, transaction_count, transaction_amount) values (%s,%s,%s,%s,%s,%s)"
values = top_transaction.values.tolist()
cursor.executemany(tt_insert_q1, values)
connection.commit()
