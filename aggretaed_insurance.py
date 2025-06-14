import os
import json
import pandas as pd
import psycopg2

#aggregated_insurance

path1 = "C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/aggregated/insurance/country/india/state/"
agg_state_list = os.listdir(path1)
#Yes, 'path1' is a string, but it represents a real folder path in the local system. We use it in an imported os function called 'os.listdir()' to access/interact with the actual files and folders in the local system. Also, 'os.listdir()' function gives all the states in the '/state/' folder as a list.
#When you paste this path in your File Explorer you land directly inside the "state" folder, and what you see are all the state folders (like tamil-nadu, karnataka, etc.)
#But here's the key difference between you using File Explorer manually vs. Python using code: Python doesn't "see" anything until you ask it to list the contents using:
#This line tells Python:
#“Hey, go to this folder, and give me the names of all items (like state folders) inside it.”

columns1 = {"States":[],"Years":[],"Quarter":[],"Transaction_Type":[],"Transaction_Count":[],"Transaction_Amount":[]}

for state in agg_state_list:
    state_path = path1 + state + "/" #C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/aggregated/insurance/country/india/state/andaman-&-nicobar-islands/ - #This loop goes through each state folder name in 'agg_state_list' [<- it is a list] and creates the full path to that state’s directory, storing it in 'state_path'.
    agg_year_list = os.listdir(state_path) #You're using agg_year_list = os.listdir(state_path) to get all year folders inside each state folder so you can process them next.
    
    for year in agg_year_list:
        year_path = state_path + year + "/" #C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/aggregated/insurance/country/india/state/andaman-&-nicobar-islands/2020/ - #This loop goes through each year folder name in 'agg_year_list' [<- it is a list] and creates the full path to that year’s directory, storing it in 'year_path'.
        agg_file_list = os.listdir(year_path) #You're using agg_file_list = os.listdir(year_path)) to get all file folders inside each year folder so you can process them next.
        
        for file in agg_file_list:
            file_path = year_path + file #C:/Users/Knodez Guest/Desktop/PhonePe(P)/pulse/data/aggregated/insurance/country/india/state/andaman-&-nicobar-islands/2020/ - #This loop goes through each files name in 'agg_file_list' [<- it is a list] and creates the full path to that year’s directory, storing it in 'file_path'.
            
            data=open(file_path,"r") #Even though file_path is a string, the open() function understands it as a path. If the file exists at that location, Python gives you a file object (like a door to look inside the file).
            A = json.load(data) #JSON files are text files in a special format. json.load() reads that text and parses it into Python data types. This tells Python: “Read the contents of this file and convert it into a Python dictionary or list, based on the JSON structure.”
            
            for i in A["data"]["transactionData"]:
                name=i["name"]
                count = i["paymentInstruments"][0]["count"]
                amount = i["paymentInstruments"][0]["amount"]
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))
                columns1["Transaction_Type"].append(name)
                columns1["Transaction_Count"].append(count)
                columns1["Transaction_Amount"].append(amount)     
                
aggregated_insurance = pd.DataFrame(columns1)

aggregated_insurance["States"] = aggregated_insurance["States"].str.replace("-"," ")
aggregated_insurance["States"] = aggregated_insurance["States"].str.title()

connection = psycopg2.connect(host="localhost",port="5432",user="postgres",password="12345",database="PhonePe_DB")
cursor = connection.cursor()
      
ai_create_q1 = "create table if not exists aggregated_insurance(states varchar(255), years int, quarter int, transaction_type varchar(255), transaction_count bigint, transaction_amount bigint)"
cursor.execute(ai_create_q1)
connection.commit()

ai_insert_q1 = "insert into aggregated_insurance(states, years, quarter, transaction_type, transaction_count, transaction_amount) values (%s,%s,%s,%s,%s,%s)"
values = aggregated_insurance.values.tolist()
cursor.execute(ai_insert_q1,values)
connection.commit()  