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
    'bupa_production_v6': 'BUPA',
    'nuffield_live': 'Nuffield',
    'newmedica_live': 'Newmedica',
    'onebright_live': 'Onebright'
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
    # Read the Excel file
    try:
        file_path = sys.argv[1]
        df = pd.read_excel(file_path)
    except Exception as e:
        result = {
            "folder_name": folder_name
        }
        print(json.dumps(result))
        connection.close()

    modified_by_function = sys.argv[2]

    def split_and_process(column, delimiter='\n'):
        #return df[column].dropna().apply(lambda x: [int(float(item)) for item in str(x).split(delimiter)]).tolist()
        #return df[column].dropna().apply(lambda x: [int(float(item.strip())) for item in str(x).split(delimiter)]).tolist()
        return df[column].dropna().apply(
            lambda x: [int(float(item)) for item in str(x).split(delimiter) if item.strip().replace('.', '', 1).isdigit()]
        ).tolist()


    
    lab_order_ids = split_and_process('Lab Order ID')
    lab_set_ids = split_and_process('Lab Set ID')
    lab_observation_request_ids = split_and_process('Lab Observation Request ID')
    manual_order_ids = split_and_process('Manual Order id')

    lab_order_ids = list(chain(*split_and_process('Lab Order ID')))
    lab_set_ids = list(chain(*split_and_process('Lab Set ID')))
    lab_observation_request_ids = list(chain(*split_and_process('Lab Observation Request ID')))
    manual_order_ids = list(chain(*split_and_process('Manual Order id')))

    # Initialize an empty list to store update/rollback statements
    implementation_script = []
    rollback_script = []

    # Query the database for each list of primary keys
    try:
        if lab_order_ids:
            for x in lab_order_ids:
                implementation_query = f"UPDATE {db_name}.observation_order SET status = 'C', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE observation_order_id = '{int(x)}';"
                implementation_script.append(implementation_query)
                current_columns = f"select * from observation_order where observation_order_id = '{int(x)}'"
                data1 = pd.read_sql(current_columns, engine)
                for _, row1 in data1.iterrows():
                    rollback_query = f"UPDATE {db_name}.observation_order SET status = '{row1['status']}', last_modified_by = '{row1['last_modified_by']}', last_modified = '{row1['last_modified']}', modified_by_function = '{row1['modified_by_function']}' WHERE observation_order_id = '{int(x)}';"
                    rollback_script.append(rollback_query)
        
        if lab_set_ids:
            for x in lab_set_ids:
                implementation_query = f"UPDATE {db_name}.observation_set SET status = 'C', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE observation_set_id = '{int(x)}';"
                implementation_script.append(implementation_query)
                current_columns = f"select * from observation_set where observation_set_id = '{int(x)}'"
                data1 = pd.read_sql(current_columns, engine)
                for _, row1 in data1.iterrows():
                    rollback_query = f"UPDATE {db_name}.observation_set SET status = '{row1['status']}', last_modified_by = '{row1['last_modified_by']}', last_modified = '{row1['last_modified']}', modified_by_function = '{row1['modified_by_function']}' WHERE observation_set_id = '{int(x)}';"
                    rollback_script.append(rollback_query)
        
        if lab_observation_request_ids:
            for x in lab_observation_request_ids:
                implementation_query = f"UPDATE {db_name}.observation_request SET status = 'C', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE observation_request_id = '{int(x)}';"
                implementation_script.append(implementation_query)
                current_columns = f"select * from observation_request where observation_request_id = '{int(x)}'"
                data1 = pd.read_sql(current_columns, engine)
                for _, row1 in data1.iterrows():
                    rollback_query = f"UPDATE {db_name}.observation_request SET status = '{row1['status']}', last_modified_by = '{row1['last_modified_by']}', last_modified = '{row1['last_modified']}', modified_by_function = '{row1['modified_by_function']}' WHERE observation_request_id = '{int(x)}';"
                    rollback_script.append(rollback_query)
        
        if manual_order_ids:
            for x in manual_order_ids:
                implementation_query = f"UPDATE {db_name}.emr2_note_entry SET deleted_by_id = '-1000', deleted = now(), last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE emr2_note_entry_id = '{int(x)}';"
                implementation_script.append(implementation_query)
                current_columns = f"select * from emr2_note_entry where emr2_note_entry_id = '{int(x)}'"
                data1 = pd.read_sql(current_columns, engine)
                for _, row1 in data1.iterrows():
                    rollback_query = f"UPDATE {db_name}.emr2_note_entry SET deleted_by_id = NULL, deleted = NULL, last_modified_by = '{row1['last_modified_by']}', last_modified = '{row1['last_modified']}', modified_by_function = '{row1['modified_by_function']}' WHERE emr2_note_entry_id = '{int(x)}';"
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