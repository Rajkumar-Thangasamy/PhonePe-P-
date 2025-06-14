import os
import json
import pandas as pd
import psycopg2

#top insurnace

columns7 = {"States":[],"Years":[],"Quarter":[],"Pincodes":[],"Transaction_Count":[],"Transaction_Amount":[]}

path7 = "C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/top/insurance/country/india/state/"
top_state_list = os.listdir(path7)

for state in top_state_list:
    state_path = path7 + state + "/"
    top_year_path = os.listdir(state_path)
    
    for year in top_year_path:
        year_path = state_path + year + "/"
        top_file_path = os.listdir(year_path)
        
        for file in top_file_path:
            file_path = year_path + file
            
            data = open(file_path,"r")
            G = json.load(data)

            for i in G["data"]["pincodes"]:
                entityName = i["entityName"]
                count = i["metric"]["count"]
                amount = i["metric"]["amount"]
                columns7["States"].append(state)
                columns7["Years"].append(year)
                columns7["Quarter"].append(int(file.strip(".json")))
                columns7["Pincodes"].append(entityName)
                columns7["Transaction_Count"].append(count)
                columns7["Transaction_Amount"].append(amount)

top_insurance = pd.DataFrame(columns7)

top_insurance["States"] = top_insurance["States"].str.replace("-"," ")
top_insurance["States"] = top_insurance["States"].str.title()

connection = psycopg2.connect(host="localhost",port="5432",user="postgres",password="12345",database="PhonePe_DB")
cursor = connection.cursor()

ti_create_q1 = "create table if not exists top_insurance(states varchar(255), years int, quarter int, pincodes bigint, transaction_count bigint, transaction_amount bigint)"
cursor.execute(ti_create_q1)
connection.commit()

ti_insert_q1 = "insert into top_insurance(states, years, quarter, pincodes, transaction_count, transaction_amount) values (%s,%s,%s,%s,%s,%s)"
values = top_insurance.values.tolist()
cursor.executemany(ti_insert_q1, values)
connection.commit()