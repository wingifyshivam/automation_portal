import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
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
    # print("\n")
    connection = None


if connection:
    patient_id =  sys.argv[1]
    modified_by_function = sys.argv[2]
    query = f"select * from payment where patient_id = {patient_id} and card_pan IS NOT NULL;"
    data = pd.read_sql(query, engine)

    # Initialize the rollback script string
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            implementation_query = f"UPDATE {db_name}.payment SET card_pan = NULL, last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE payment_id = '{row['payment_id']}';"
            implementation_script.append(implementation_query)
            rollback_query = f"UPDATE {db_name}.payment SET card_pan = '{row['card_pan']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE payment_id = '{row['payment_id']}';"
            rollback_script.append(rollback_query)
        if len(implementation_script) > 0:
            implementation_script.append('COMMIT;')
            rollback_script.append('COMMIT;')
            implementation_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/{}/{}/implementation_script.sql'.format(folder_name, modified_by_function)
            os.makedirs(os.path.dirname(implementation_file_path), exist_ok=True)
            with open(implementation_file_path, 'w') as sql_file:
                for statement in implementation_script:
                    sql_file.write(statement + '\n')
            rollback_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/{}/{}/rollback_script.sql'.format(folder_name, modified_by_function)
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