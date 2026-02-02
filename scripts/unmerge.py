import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
import urllib.parse
import sys
import json

# Database connection details

client = sys.argv[1]

if client == 'BUPA Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.129.12')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'bupa_production_v6')
elif client == 'Nuffield Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.130.107')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'nuffield_live')
elif client == 'Newmedica Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.131.33')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'newmedica_live')
elif client == 'Onebright Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.136.63')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'onebright_live')
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
    patient_id_old = sys.argv[2]
    modified_by_function = sys.argv[3]
    query = f"SELECT * FROM patient_merge_log_other WHERE patient_id_old = {patient_id_old} group by column_name, row_id order by table_name"
    data = pd.read_sql(query, engine)

    # Initialize the rollback script string
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            if row['table_name'] == 'appointment':
                if row['column_name'] == 'appointment_id' or row['column_name'] == 'patient_id':
                    implementation_query = f"UPDATE {row['table_name']} SET patient_id = '{row['patient_id_old']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE {row['table_name']}_id = '{row['row_id']}';"
                    implementation_script.append(implementation_query)
                    rollback_query = f"UPDATE {row['table_name']} SET patient_id = '{row['patient_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE {row['table_name']}_id = '{row['row_id']}';"
                    rollback_script.append(rollback_query)
                else:
                    implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id_old']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE {row['table_name']}_id = '{row['row_id']}';"
                    implementation_script.append(implementation_query)
                    rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE {row['table_name']}_id = '{row['row_id']}';"
                    rollback_script.append(rollback_query)
            elif row['table_name'] == 'patient_letter':
                implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id_old']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE letter_id = '{row['row_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE letter_id = '{row['row_id']}';"
                rollback_script.append(rollback_query)
            elif row['table_name'] == 'paper_folder':
                if row['column_name'] == 'parent_id':
                    a = f"SELECT parent_id FROM paper_folder WHERE folder_id = {row['row_id']}"
                    data1 = pd.read_sql(a, engine)
                    for _, row1 in data1.iterrows():
                        b = f"SELECT special_type FROM paper_folder WHERE folder_id = {row1['parent_id']} and special_type is not NULL"
                        data2 = pd.read_sql(b, engine)
                        for _, row2 in data2.iterrows():
                            c = f"SELECT folder_id FROM paper_folder WHERE patient_id = {patient_id_old} and special_type = '{row2['special_type']}'"
                            data3 = pd.read_sql(c, engine)
                            for _, row3 in data3.iterrows():
                                implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row3['folder_id']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE folder_id = '{row['row_id']}';"
                                implementation_script.append(implementation_query)
                else:
                    implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id_old']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE folder_id = '{row['row_id']}';"
                    implementation_script.append(implementation_query)
                folder_query = f"select * from {row['table_name']} where folder_id = '{row['row_id']}'"
                data1 = pd.read_sql(folder_query, engine)
                for _, row1 in data1.iterrows():
                    if row['column_name'] == 'parent_id':
                        rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row1['parent_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE folder_id = '{row['row_id']}';"
                        rollback_script.append(rollback_query)
                    if row['column_name'] == 'patient_id':
                        rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE folder_id = '{row['row_id']}';"
                        rollback_script.append(rollback_query)
            elif row['table_name'] == 'paper_file':
                if row['column_name'] == 'folder_id':
                    a = f"SELECT * FROM paper_file WHERE file_id = {row['row_id']}"
                    data1 = pd.read_sql(a, engine)
                    for _, row1 in data1.iterrows():
                        b = f"SELECT * FROM paper_folder WHERE folder_id = {row1['folder_id']}"
                        data2 = pd.read_sql(b, engine)
                        for _, row2 in data2.iterrows():
                            if row2['special_type']:
                                c = f"SELECT folder_id FROM paper_folder WHERE patient_id = {patient_id_old} and special_type = '{row2['special_type']}'"
                                data3 = pd.read_sql(c, engine)
                                for _, row3 in data3.iterrows():
                                    implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row3['folder_id']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row['row_id']}';"
                                    implementation_script.append(implementation_query)
                            else:
                                d = f"SELECT folder_id FROM paper_folder WHERE patient_id = {patient_id_old} and special_type = 'patient'"
                                data4 = pd.read_sql(d, engine)
                                for _, row4 in data4.iterrows():
                                    implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row4['folder_id']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row['row_id']}';"
                                    implementation_script.append(implementation_query)
                else:
                    implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id_old']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE file_id = '{row['row_id']}';"
                    implementation_script.append(implementation_query)
                file_query = f"select * from {row['table_name']} where file_id = '{row['row_id']}'"
                data1 = pd.read_sql(file_query, engine)
                for _, row1 in data1.iterrows():
                    if row['column_name'] == 'folder_id':
                        rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row1['folder_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE folder_id = '{row['row_id']}';"
                        rollback_script.append(rollback_query)
                    if row['column_name'] == 'patient_id':
                        rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE folder_id = '{row['row_id']}';"
                        rollback_script.append(rollback_query)
            elif row['table_name'] == 'clinical_report':
                implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id_old']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE report_id = '{row['row_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE report_id = '{row['row_id']}';"
                rollback_script.append(rollback_query)
            elif row['table_name'] == 'individual':
                implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = 'A', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE {row['table_name']}_id = '{row['row_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = 'C', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE {row['table_name']}_id = '{row['row_id']}';"
                rollback_script.append(rollback_query)
            else:
                implementation_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id_old']}', last_modified_by = '-1000', last_modified = now(), modified_by_function = '{modified_by_function}' WHERE {row['table_name']}_id = '{row['row_id']}';"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE {row['table_name']} SET {row['column_name']} = '{row['patient_id']}', last_modified_by = '{row['last_modified_by']}', last_modified = '{row['last_modified']}', modified_by_function = '{row['modified_by_function']}' WHERE {row['table_name']}_id = '{row['row_id']}';"
                rollback_script.append(rollback_query)
        if len(implementation_script) > 0:
            implementation_script.append('COMMIT;')
            rollback_script.append('COMMIT;')
            implementation_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/{}/{}/implementation_script.sql'.format(folder_name, modified_by_function)
            os.makedirs(os.path.dirname(implementation_file_path), exist_ok=True)
            with open(implementation_file_path, 'w') as sql_file:
                for statement in implementation_script:
                    sql_file.write(statement + '\n')

            # Write rollback statements to a SQL file
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