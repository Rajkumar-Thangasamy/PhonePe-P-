import os
import json
import pandas as pd
import psycopg2

#map transaction 

columns5 = {"States":[],"Years":[],"Quarter":[],"Districts":[],"Transaction_Count":[],"Transaction_Amount":[]}

path5 = "C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/map/transaction/hover/country/india/state/"
map_state_list = os.listdir(path5)

for state in map_state_list:
    state_path = path5 + state + "/"
    map_year_path = os.listdir(state_path)

    for year in map_year_path:
        year_path = state_path + year + "/"
        map_file_path = os.listdir(year_path)
        
        for file in map_file_path:
            file_path = year_path + file
            
            data = open(file_path,"r")
            E = json.load(data)
            
            for i in E["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns5["States"].append(state)
                columns5["Years"].append(year)
                columns5["Quarter"].append(int(file.strip(".json")))
                columns5["Districts"].append(name)
                columns5["Transaction_Count"].append(count)
                columns5["Transaction_Amount"].append(amount)

map_transaction = pd.DataFrame(columns5)

map_transaction["States"] = map_transaction["States"].str.replace("-"," ")
map_transaction["States"] = map_transaction["States"].str.title()
map_transaction["Districts"] = map_transaction["Districts"].str.replace("-"," ")
map_transaction["Districts"] = map_transaction["Districts"].str.title()

connection = psycopg2.connect(host="localhost",port="5432",user="postgres",password="12345",database="PhonePe_DB")
cursor = connection.cursor()

mt_create_q1 = "create table if not exists map_transaction(states varchar(255), years int, quarter int, districts varchar(255), transaction_count bigint, transaction_amount bigint)"
cursor.execute(mt_create_q1)
connection.commit()

mt_insert_q1 = "insert into map_transaction(states, years, quarter, districts, transaction_count, transaction_amount) values (%s,%s,%s,%s,%s,%s)"
values = map_transaction.values.tolist()
cursor.executemany(mt_insert_q1,values)
connection.commit()  

            
            

