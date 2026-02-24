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
    # print("\n")
    connection = None


if connection:
    patient_id = sys.argv[1]
    modified_by_function = sys.argv[2]
    query = text(r"""
    SELECT 
        en.emr2_encounter_id,
        en.emr2_note_id AS duplicate_note_id,
        orig.original_note_id
    FROM
        emr2_note en
            INNER JOIN
        emr2_encounter ee ON ee.emr2_encounter_id = en.emr2_encounter_id
            JOIN
        (SELECT 
            emr2_encounter_id, MIN(emr2_note_id) AS original_note_id
        FROM
            emr2_note
        GROUP BY emr2_encounter_id
        HAVING COUNT(*) > 1) orig ON en.emr2_encounter_id = orig.emr2_encounter_id
    WHERE
        en.emr2_note_id <> orig.original_note_id
            AND ee.patient_id = :patient_id;
    """)
    data = pd.read_sql(query, engine, params={"patient_id": patient_id})
    # Initialize the rollback script string
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            a = f"select emr2_note_entry_id, emr2_note_id, last_modified_by, last_modified, modified_by_function from emr2_note_entry where emr2_note_id = {row['duplicate_note_id']};"
            data1 = pd.read_sql(a, engine)
            for _, row1 in data1.iterrows():
                implementation_query = f"UPDATE `{db_name}`.`emr2_note_entry` SET `emr2_note_id` = '{row['original_note_id']}', `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE (`emr2_note_entry_id` = '{row1['emr2_note_entry_id']}');"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE `{db_name}`.`emr2_note_entry` SET `emr2_note_id` = '{row1['emr2_note_id']}', `last_modified_by` = '{row1['last_modified_by']}', `last_modified` = '{row1['last_modified']}', `modified_by_function` = '{row1['modified_by_function']}' WHERE (`emr2_note_entry_id` = '{row1['emr2_note_entry_id']}');"
                rollback_script.append(rollback_query)

            b = f"select emr2_note_id, deleted, deleted_by_id, last_modified_by, last_modified, modified_by_function from emr2_note where emr2_note_id = {row['duplicate_note_id']};"
            data2 = pd.read_sql(b, engine)
            for _, row2 in data2.iterrows():
                implementation_query = f"UPDATE `{db_name}`.`emr2_note` SET `deleted` = now(), `deleted_by_id` = '-1000', `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE (`emr2_note_id` = '{row2['emr2_note_id']}');"
                implementation_script.append(implementation_query)
                rollback_query = f"UPDATE `{db_name}`.`emr2_note` SET `deleted` = NULL, `deleted_by_id` = NULL, `last_modified_by` = '{row2['last_modified_by']}', `last_modified` = '{row2['last_modified']}', `modified_by_function` = '{row2['modified_by_function']}' WHERE (`emr2_note_id` = '{row2['emr2_note_id']}');"
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