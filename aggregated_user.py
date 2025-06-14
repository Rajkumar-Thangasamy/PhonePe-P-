import os
import json
import pandas as pd
import psycopg2

#aggregated_user

columns3={"States":[],"Years":[],"Quarter":[],"Brands":[],"Transaction_Count":[],"Percentage":[]}

path3="C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/aggregated/user/country/india/state/"
agg_state_list=os.listdir(path3)

for state in agg_state_list:
    state_path=path3+state+"/"
    agg_year_list=os.listdir(state_path)
    
    for year in agg_year_list:
        year_path=state_path+year+"/"
        agg_file_path=os.listdir(year_path)
        
        for file in agg_file_path:
            file_path=year_path+file

            data=open(file_path,"r")
            C=json.load(data)
   
            try:
                for i in C["data"]["usersByDevice"]:
                    brand=i["brand"]
                    count = i["count"]
                    percentage = i["percentage"]
                    columns3["Brands"].append(brand)
                    columns3["Transaction_Count"].append(count)
                    columns3["Percentage"].append(percentage)
                    columns3["States"].append(state)
                    columns3["Years"].append(year)
                    columns3["Quarter"].append(int(file.strip(".json")))
            except:
                pass

aggregated_user = pd.DataFrame(columns3)

aggregated_user["States"] = aggregated_user["States"].str.replace("-"," ")
aggregated_user["States"] = aggregated_user["States"].str.title()
 
connection = psycopg2.connect(host = "localhost",port = "5432",user = "postgres",password = "12345",database = "PhonePe_DB")
cursor = connection.cursor()

au_create_q1 = """create table if not exists aggregated_user(states varchar(255), years int, quarter int, brands varchar(255), transaction_count bigint, percentage bigint)"""
cursor.execute(au_create_q1)
connection.commit()

au_insert_q1 = """insert into aggregated_user (states, years, quarter, brands, transaction_count, percentage) values (%s,%s,%s,%s,%s,%s)"""
values = aggregated_user.values.tolist()
cursor.executemany(au_insert_q1, values)
connection.commit()