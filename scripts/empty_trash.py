import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
import urllib.parse
import sys
import json

# Database connection details

client = sys.argv[1]
new_patient = 0
new_folder = 0

if client == 'BUPA Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.129.12')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'bupa_production_v6')
    new_patient = 581228
    new_folder = 1767477
elif client == 'Nuffield Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.130.107')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'nuffield_live')
    new_patient = 2
    new_folder = 3
elif client == 'Newmedica Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.131.33')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'newmedica_live')
    new_patient = 400428
    new_folder = 446605
elif client == 'Onebright Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.136.63')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'onebright_live')
    new_patient = 13144
    new_folder = 41310
else:
    print('Invalid client!!')

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
    patient_id = sys.argv[2]
    modified_by_function = sys.argv[3]
    query = f"select * from paper_folder where patient_id = {patient_id} and special_type = 'trash';"
    data = pd.read_sql(query, engine)
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            a = f"SELECT * FROM paper_folder WHERE parent_id = {row['folder_id']}"
            data1 = pd.read_sql(a, engine)
            x = f"select * from paper_file where folder_id = {row['folder_id']}"
            data6 = pd.read_sql(x, engine)
            for _, row6 in data6.iterrows():
                implementation_query = f"UPDATE {db_name}.paper_file SET patient_id = '{new_patient}', folder_id = '{new_folder}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row6['file_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE {db_name}.paper_file SET patient_id = '{row6['patient_id']}', folder_id = '{row6['folder_id']}', last_modified_by = '{row6['last_modified_by']}', last_modified = '{row6['last_modified']}', modified_by_function = '{row6['modified_by_function']}' WHERE file_id = '{row6['file_id']}';"
                rollback_script.append(rollback_query)
            for _, row1 in data1.iterrows():
                implementation_query = f"UPDATE {db_name}.paper_folder SET patient_id = '{new_patient}', parent_id = '{new_folder}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE folder_id = '{row1['folder_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE {db_name}.paper_folder SET patient_id = '{row1['patient_id']}', parent_id = '{row1['parent_id']}', last_modified_by = '{row1['last_modified_by']}', last_modified = '{row1['last_modified']}', modified_by_function = '{row1['modified_by_function']}' WHERE folder_id = '{row1['folder_id']}';"
                rollback_script.append(rollback_query)
                b = f"SELECT * FROM paper_folder WHERE parent_id = {row1['folder_id']}"
                data2 = pd.read_sql(b, engine)
                if data2.empty:
                    c = f"select * from paper_file where folder_id = {row1['folder_id']}"
                    data3 = pd.read_sql(c, engine)
                    for _, row3 in data3.iterrows():
                        implementation_query = f"UPDATE {db_name}.paper_file SET patient_id = '{new_patient}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row3['file_id']}';"
                        implementation_script.append(implementation_query)
                        rollback_query = f"UPDATE {db_name}.paper_file SET patient_id = '{row3['patient_id']}', last_modified_by = '{row3['last_modified_by']}', last_modified = '{row3['last_modified']}', modified_by_function = '{row3['modified_by_function']}' WHERE file_id = '{row3['file_id']}';"
                        rollback_script.append(rollback_query)
                else:
                    c = f"select * from paper_file where folder_id = {row1['folder_id']}"
                    data3 = pd.read_sql(c, engine)
                    for _, row3 in data3.iterrows():
                        implementation_query = f"UPDATE {db_name}.paper_file SET patient_id = '{new_patient}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row3['file_id']}';"
                        implementation_script.append(implementation_query)
                        rollback_query = f"UPDATE {db_name}.paper_file SET patient_id = '{row3['patient_id']}', last_modified_by = '{row3['last_modified_by']}', last_modified = '{row3['last_modified']}', modified_by_function = '{row3['modified_by_function']}' WHERE file_id = '{row3['file_id']}';"
                        rollback_script.append(rollback_query)
                    for _, row4 in data2.iterrows():
                        implementation_query = f"UPDATE {db_name}.paper_folder SET patient_id = '{new_patient}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE folder_id = '{row4['folder_id']}';"
                        implementation_script.append(implementation_query)
                        rollback_query = f"UPDATE {db_name}.paper_folder SET patient_id = '{row4['patient_id']}', parent_id = '{row4['parent_id']}', last_modified_by = '{row4['last_modified_by']}', last_modified = '{row4['last_modified']}', modified_by_function = '{row4['modified_by_function']}' WHERE folder_id = '{row4['folder_id']}';"
                        rollback_script.append(rollback_query)
                        d = f"SELECT * FROM paper_folder WHERE parent_id = {row4['folder_id']}"
                        data4 = pd.read_sql(d, engine)
                        if data4.empty:
                            e = f"select * from paper_file where folder_id = {row4['folder_id']}"
                            data5 = pd.read_sql(e, engine)
                            for _, row5 in data5.iterrows():
                                implementation_query = f"UPDATE {db_name}.paper_file SET patient_id = '{new_patient}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row5['file_id']}';"
                                implementation_script.append(implementation_query)
                                rollback_query = f"UPDATE {db_name}.paper_file SET patient_id = '{row5['patient_id']}', folder_id = '{row5['folder_id']}', last_modified_by = '{row5['last_modified_by']}', last_modified = '{row5['last_modified']}', modified_by_function = '{row5['modified_by_function']}' WHERE file_id = '{row5['file_id']}';"
                                rollback_script.append(rollback_query)                        
                        else:
                            c = f"select * from paper_file where folder_id = {row4['folder_id']}"
                            data3 = pd.read_sql(c, engine)
                            for _, row3 in data3.iterrows():
                                implementation_query = f"UPDATE {db_name}.paper_file SET patient_id = '{new_patient}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row3['file_id']}';"
                                implementation_script.append(implementation_query)
                                rollback_query = f"UPDATE {db_name}.paper_file SET patient_id = '{row3['patient_id']}', last_modified_by = '{row3['last_modified_by']}', last_modified = '{row3['last_modified']}', modified_by_function = '{row3['modified_by_function']}' WHERE file_id = '{row3['file_id']}';"
                                rollback_script.append(rollback_query)
                            for _, row4 in data4.iterrows():
                                implementation_query = f"UPDATE {db_name}.paper_folder SET patient_id = '{new_patient}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE folder_id = '{row4['folder_id']}';"
                                implementation_script.append(implementation_query)
                                rollback_query = f"UPDATE {db_name}.paper_folder SET patient_id = '{row4['patient_id']}', parent_id = '{row4['parent_id']}', last_modified_by = '{row4['last_modified_by']}', last_modified = '{row4['last_modified']}', modified_by_function = '{row4['modified_by_function']}' WHERE folder_id = '{row4['folder_id']}';"
                                rollback_script.append(rollback_query)
                                d = f"SELECT * FROM paper_folder WHERE parent_id = {row4['folder_id']}"
                                data6 = pd.read_sql(d, engine)
                                if data6.empty:
                                    e = f"select * from paper_file where folder_id = {row4['folder_id']}"
                                    data5 = pd.read_sql(e, engine)
                                    for _, row5 in data5.iterrows():
                                        implementation_query = f"UPDATE {db_name}.paper_file SET patient_id = '{new_patient}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row5['file_id']}';"
                                        implementation_script.append(implementation_query)
                                        rollback_query = f"UPDATE {db_name}.paper_file SET patient_id = '{row5['patient_id']}', folder_id = '{row5['folder_id']}', last_modified_by = '{row5['last_modified_by']}', last_modified = '{row5['last_modified']}', modified_by_function = '{row5['modified_by_function']}' WHERE file_id = '{row5['file_id']}';"
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
                "folder_name": client,
                "implementation_path": implementation_file_path,
                "rollback_path": rollback_file_path
            }

            print(json.dumps(result))
        
        else:
            result = {
                "folder_name": client
            }
            print(json.dumps(result))
            
    except Exception as e:
        print(f"Error generating update/rollback statements")

    finally:
        connection.close()

else:
    print("Unable to establish database connection.")