import os
import json
import pandas as pd
import psycopg2

#top_user

columns9 = {"States":[],"Years":[],"Quarter":[],"Pincodes":[],"Registered_Users":[]}

path9 = "C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/top/user/country/india/state/"

top_state_list = os.listdir(path9)

for state in top_state_list:
    state_path = path9 + state + "/"
    top_year_list = os.listdir(state_path)

    for year in top_year_list:
        year_path = state_path + year + "/"
        top_file_list = os.listdir(year_path)

        for file in top_file_list:
            file_path = year_path + file 

            data = open(file_path, "r")
            I = json.load(data)

            for i in I["data"]["pincodes"]:
                name = i["name"]
                registeredUsers = i["registeredUsers"]
                columns9["States"].append(state)
                columns9["Years"].append(year)
                columns9["Quarter"].append(int(file.strip(".json")))
                columns9["Pincodes"].append(name)
                columns9["Registered_Users"].append(registeredUsers)

top_user = pd.DataFrame(columns9)

top_user["States"] = top_user["States"].str.replace("-"," ")
top_user["States"] = top_user["States"].str.title()

connection = psycopg2.connect(host="localhost",port="5432",user="postgres",password="12345",database="PhonePe_DB")
cursor = connection.cursor()

tu_create_q1 = "create table if not exists top_user(states varchar(255), years int, quarter int, pincodes bigint, registered_users bigint)"
cursor.execute(tu_create_q1)
connection.commit()

tu_insert_q1 = "insert into top_user(states, years, quarter, pincodes, registered_users) values (%s,%s,%s,%s,%s)"
values = top_user.values.tolist()
cursor.executemany(tu_insert_q1, values)
connection.commit()



