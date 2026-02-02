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
db_host = os.getenv('DB_HOST', '172.20.131.33')
db_port = os.getenv('DB_PORT', '3306')
db_name = os.getenv('DB_NAME', 'newmedica_live')

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
    connection = None


if connection:
    current_datetime = datetime.now().strftime("%d%m%y")
    modified_by_function = sys.argv[1]
    query = f"select diary_date, location_name, cs.external_service_id, cab_service_name, CONCAT(i.forename, ' ', i.surname) AS 'practitioner', dscs.diary_section_cab_service_id, dscs.status as 'diary_section_cab_service status', ds.diary_section_id, ds.status as 'diary section status', ds.diary_id, d.status as 'diary status' from diary_section_cab_service dscs inner join diary_section ds on dscs.diary_section_id = ds.diary_section_id inner join diary d on ds.diary_id = d.diary_id  inner join cab_service cs on dscs.cab_service_id =cs.cab_service_id inner join location l on cs.location_id= l.location_id inner join individual i on d.doctor_id = i.individual_id where diary_date > now() and ds.status= 'C' and dscs.status  = 'A';"
    data = pd.read_sql(query, engine)
    # Initialize the rollback script string
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            a = f"select diary_section_cab_service_id, status, last_modified_by, last_modified, modified_by_function from diary_section_cab_service where diary_section_cab_service_id = {row['diary_section_cab_service_id']};"
            data1 = pd.read_sql(a, engine)
            for _, row1 in data1.iterrows():
                implementation_query = f"UPDATE diary_section_cab_service SET status = 'C', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE diary_section_cab_service_id = '{row1['diary_section_cab_service_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE diary_section_cab_service SET status = '{row1['status']}', last_modified_by = '{row1['last_modified_by']}', last_modified = '{row1['last_modified']}', modified_by_function = '{row1['modified_by_function']}' WHERE diary_section_cab_service_id = '{row1['diary_section_cab_service_id']}';"
                rollback_script.append(rollback_query)
        if len(implementation_script) > 0:
            implementation_script.append('COMMIT;')
            rollback_script.append('COMMIT;')
            implementation_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Newmedica/{}/implementation_script.sql'.format(modified_by_function)
            os.makedirs(os.path.dirname(implementation_file_path), exist_ok=True)
            with open(implementation_file_path, 'w') as sql_file:
                for statement in implementation_script:
                    sql_file.write(statement + '\n')
            rollback_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/Newmedica/{}/rollback_script.sql'.format(modified_by_function)
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