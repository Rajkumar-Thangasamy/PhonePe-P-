import os
import json
import pandas as pd
import psycopg2

#aggregated_transaction

columns2 = {"States":[],"Years":[],"Quarter":[],"Transaction_Type":[],"Transaction_Count":[],"Transaction_Amount":[]}

path2="C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/aggregated/transaction/country/india/state/"
agg_state_list=os.listdir(path2)

for state in agg_state_list:
    state_path=path2+state+"/"
    agg_years_list=os.listdir(state_path)
    
    for year in agg_years_list:
        year_path=state_path+year+"/"
        agg_file_list=os.listdir(year_path)
        
        for file in agg_file_list:
            file_path=year_path+file
            
            data=open(file_path,"r")
            B=json.load(data)

            for i in B["data"]["transactionData"]:
                name=i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                
                columns2["Transaction_Type"].append(name)
                columns2["Transaction_Count"].append(count)
                columns2["Transaction_Amount"].append(amount)
                columns2["States"].append(state)
                columns2["Years"].append(year)
                columns2["Quarter"].append(int(file.strip(".json")))

aggregated_transaction = pd.DataFrame(columns2)

aggregated_transaction["States"] = aggregated_transaction["States"].str.replace("-"," ")
aggregated_transaction["States"] = aggregated_transaction["States"].str.title()

connection = psycopg2.connect(host="localhost",port="5432",user="postgres",password="12345",database="PhonePe_DB")
cursor = connection.cursor()

at_create_q1 = """create table if not exists aggregated_transaction(states varchar(255), years int, quarter int, transaction_type varchar(255), transaction_count bigint, transaction_amount bigint)"""
cursor.execute(at_create_q1)
connection.commit()

at_insert_q1 = """insert into aggregated_transaction(states, years, quarter, transaction_type, transaction_count, transaction_amount) values (%s,%s,%s,%s,%s,%s)"""
values = aggregated_transaction.values.tolist()
cursor.executemany(at_insert_q1,values)
connection.commit() 