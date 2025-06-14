import os
import json
import pandas as pd
import psycopg2

#map user 

columns6 = {"States":[],"Years":[],"Quarter":[],"Districts":[],"Registered_Users":[],"App_Opens":[]}

path6 = "C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/map/user/hover/country/india/state/"
map_state_list = os.listdir(path6)

for state in map_state_list:
    state_path = path6 + state + "/"
    map_year_path = os.listdir(state_path)

    for year in map_year_path:
        year_path = state_path + year + "/"
        map_file_path = os.listdir(year_path)

        for file in map_file_path:
            file_path = year_path + file

            data = open(file_path, "r")
            F = json.load(data)
            
            for i in F["data"]["hoverData"].items():
                districts=i[0]
                registeredUsers=i[1]["registeredUsers"]
                appOpens=i[1]["appOpens"]
                columns6["States"].append(state)
                columns6["Years"].append(year)
                columns6["Quarter"].append(int(file.strip(".json")))
                columns6["Districts"].append(districts)
                columns6["Registered_Users"].append(registeredUsers)
                columns6["App_Opens"].append(appOpens)

map_user = pd.DataFrame(columns6)

map_user["States"] = map_user["States"].str.replace("-"," ")
map_user["States"] = map_user["States"].str.title()
map_user["Districts"] = map_user["Districts"].str.replace("-"," ")
map_user["Districts"] = map_user["Districts"].str.title()

connection = psycopg2.connect(host="localhost",port="5432",user="postgres",password="12345",database="PhonePe_DB")
cursor = connection.cursor()

mu_create_q1 = "create table if not exists map_user(states varchar(255), years int, quarter int, districts varchar(255), registered_users bigint, app_opens bigint)"
cursor.execute(mu_create_q1)
connection.commit()

mu_insert_q1 = "insert into map_user(states, years, quarter, districts, registered_users, app_opens) values (%s,%s,%s,%s,%s,%s)"
values = map_user.values.tolist()
cursor.executemany(mu_insert_q1,values)
connection.commit() 



