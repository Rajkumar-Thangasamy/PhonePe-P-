import os
import json
import pandas as pd
import psycopg2

#map insurance

columns4 = {"States":[],"Years":[],"Quarter":[],"Districts":[],"Transaction_Count":[],"Transaction_Amount":[]}

path4 = "C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/map/insurance/hover/country/india/state/"
map_state_list = os.listdir(path4)

for state in map_state_list:
    state_path = path4 + state + "/"
    map_year_path = os.listdir(state_path)
    
    for year in map_year_path:
        year_path = state_path + year + "/"
        map_file_path = os.listdir(year_path)
        
        for file in map_file_path:
            file_path = year_path + file
            
            data = open(file_path,"r")
            D = json.load(data)

            for i in D["data"]["hoverDataList"]:
                name = i["name"]
                count = i["metric"][0]["count"]
                amount = i["metric"][0]["amount"]
                columns4["States"].append(state)
                columns4["Years"].append(year)
                columns4["Quarter"].append(int(file.strip(".json")))
                columns4["Districts"].append(name)
                columns4["Transaction_Count"].append(count)
                columns4["Transaction_Amount"].append(amount)

map_insurance = pd.DataFrame(columns4)

map_insurance["States"] = map_insurance["States"].str.replace("-"," ")
map_insurance["States"] = map_insurance["States"].str.title()
map_insurance["Districts"] = map_insurance["Districts"].str.replace("-"," ")
map_insurance["Districts"] = map_insurance["Districts"].str.title()

connection = psycopg2.connect(host="localhost",port="5432",user="postgres",password="12345",database="PhonePe_DB")
cursor = connection.cursor()
      
mi_create_q1 = "create table if not exists map_insurance(states varchar(255), years int, quarter int, districts varchar(255), transaction_count bigint, transaction_amount bigint)"
cursor.execute(mi_create_q1)
connection.commit()

mi_insert_q1 = "insert into map_insurance(states, years, quarter, districts, transaction_count, transaction_amount) values (%s,%s,%s,%s,%s,%s)"
values = map_insurance.values.tolist()
cursor.executemany(mi_insert_q1,values)
connection.commit()  



