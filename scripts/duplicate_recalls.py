import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
from datetime import datetime
import urllib.parse
import sys
import json

# Database connection details
db_username = os.getenv('DB_USERNAME', 'shivam.a')
db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
db_host = os.getenv('DB_HOST', '172.20.129.12')
db_port = os.getenv('DB_PORT', '3306')
db_name = os.getenv('DB_NAME', 'bupa_production_v6')

db_to_folder = {
    'bupa_production_v6': 'BUPA'
}
folder_name = db_to_folder.get(db_name, 'UnknownDB')

# Connection string for MySQL
connection_string = f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
# print(f"Connecting to database: {db_name}")
# print("\n")

# Create a database connection
try:
    engine = create_engine(connection_string)
    connection = engine.connect()
    # print("Database connection successful.")
    # print("\n")
except Exception as e:
    # print(f"Error connecting to database: {e}")
    connection = None


if connection:
    modified_by_function = sys.argv[1]
    query = f"select rci.recall_work_item_id from recall_work_item rci INNER JOIN (select rci.recall_id,max(recall_work_item_id) max_recall_work_item_id,count(*) instances from recall_work_item rci where rci.completed_date is null and rci.started_date is null group by rci.recall_id having count(*) > 1)x ON (rci.recall_id = x.recall_id and rci.recall_work_item_id < x.max_recall_work_item_id) where rci.completed_date is null and rci.started_date is null;"
    data = pd.read_sql(query, engine)
    query2 = f"select r.recall_id from recall r inner join (select r.individual_id, r.recall_id, max(recall_id) max_recall_id, count(*) instances from recall r inner join patient p on p.individual_id = r.individual_id where started_date is null and completed_date is null group by r.individual_id, recall_string_behaviour_id having count(*) > 1)x ON (r.individual_id = x.individual_id and r.recall_id < x.max_recall_id) where r.completed_date is null and r.started_date is null;"
    data2 = pd.read_sql(query2, engine)
    query3 = f"select r.recall_id from recall r inner join (select r.individual_id, r.recall_id, max(recall_id) max_recall_id, count(*) instances from recall r inner join patient p on p.individual_id = r.individual_id where completed_date is null and started_date is not null group by r.individual_id having count(*) > 1)x ON (r.individual_id = x.individual_id and r.recall_id < x.max_recall_id) where r.completed_date is null and started_date is not null;"
    data3= pd.read_sql(query3, engine)

    # Initialize the rollback script string
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            a = f"select recall_work_item_id,started_date,completed_date,last_modified_by,last_modified,modified_by_function from recall_work_item where recall_work_item_id = {row['recall_work_item_id']};"
            data1 = pd.read_sql(a, engine)
            for _, row1 in data1.iterrows():
                implementation_query = f"UPDATE `{db_name}`.`recall_work_item` SET `started_date` = now(), `completed_date` = now(), `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE `recall_work_item_id` = '{row1['recall_work_item_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE `{db_name}`.`recall_work_item` SET `started_date` = NULL, `completed_date` = NULL, `last_modified_by` = '{row1['last_modified_by']}', `last_modified` = '{row1['last_modified']}', `modified_by_function` = '{row1['modified_by_function']}' WHERE `recall_work_item_id` = '{row1['recall_work_item_id']}';"
                rollback_script.append(rollback_query)
        for _, row in data2.iterrows():
            a = f"select individual_id, recall_id, `started_date`, `completed_date`, last_modified_by, last_modified, modified_by_function from recall where `recall_id` = {row['recall_id']};"
            data1 = pd.read_sql(a, engine)
            for _, row1 in data1.iterrows():
                implementation_query = f"UPDATE `{db_name}`.`recall` SET `started_date` = now(), `completed_date` = now(), `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE `recall_id` = '{row1['recall_id']}' and `started_date` is NULL and `completed_date` is NULL;"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE `{db_name}`.`recall` SET `started_date` = NULL, `completed_date` = NULL, `last_modified_by` = '{row1['last_modified_by']}', `last_modified` = '{row1['last_modified']}', `modified_by_function` = '{row1['modified_by_function']}' WHERE `recall_id` = '{row1['recall_id']}';"
                rollback_script.append(rollback_query)
        for _, row in data3.iterrows():
            a = f"select individual_id, recall_id, started_date, completed_date, last_modified_by, last_modified, modified_by_function from recall where recall_id = {row['recall_id']};"
            data1 = pd.read_sql(a, engine)
            for _, row1 in data1.iterrows():
                implementation_query = f"UPDATE `{db_name}`.`recall` SET `completed_date` = now(), `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE `recall_id` = '{row1['recall_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE `{db_name}`.`recall` SET `completed_date` = NULL, `last_modified_by` = '{row1['last_modified_by']}', `last_modified` = '{row1['last_modified']}', `modified_by_function` = '{row1['modified_by_function']}' WHERE `recall_id` = '{row1['recall_id']}';"
                rollback_script.append(rollback_query)

            b = f"select individual_id, recall_id, recall_work_item_id, started_date, completed_date, last_modified_by, last_modified, modified_by_function from recall_work_item where started_date is null and completed_date is null and recall_id = {row['recall_id']};"
            data1 = pd.read_sql(b, engine)
            for _, row1 in data1.iterrows():
                implementation_query = f"UPDATE `{db_name}`.`recall_work_item` SET `started_date` = now(), `completed_date` = now(), `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE `recall_work_item_id` = '{row1['recall_work_item_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE `{db_name}`.`recall_work_item` SET `started_date` = NULL, `completed_date` = NULL, `last_modified_by` = '{row1['last_modified_by']}', `last_modified` = '{row1['last_modified']}', `modified_by_function` = '{row1['modified_by_function']}' WHERE `recall_work_item_id` = '{row1['recall_work_item_id']}';"
                rollback_script.append(rollback_query)
        if len(implementation_script) > 0:
            implementation_script.append('COMMIT;')
            rollback_script.append('COMMIT;')
            implementation_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/BUPA/{}/implementation_script.sql'.format(modified_by_function)
            os.makedirs(os.path.dirname(implementation_file_path), exist_ok=True)
            with open(implementation_file_path, 'w') as sql_file:
                for statement in implementation_script:
                    sql_file.write(statement + '\n')
            rollback_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/BUPA/{}/rollback_script.sql'.format(modified_by_function)
            os.makedirs(os.path.dirname(rollback_file_path), exist_ok=True)
            with open(rollback_file_path, 'w') as sql_file:
                for statement in rollback_script:
                    sql_file.write(statement + '\n')
            
            result = {
                "folder_name": folder_name,
                "implementation_path": implementation_file_path,
                "rollback_path": rollback_file_path
            }

            print(json.dumps(result))
            
        else:
            result = {
                "folder_name": folder_name
            }
            print(json.dumps(result))
    except Exception as e:
        print(f"Error generating update/rollback statements")

    finally:
        connection.close()    

else:
    print("Unable to establish database connection.")