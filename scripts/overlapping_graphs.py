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
else:
    print('Invalid client!!')

db_to_folder = {
    'bupa_production_v6': 'BUPA',
    'nuffield_live': 'Nuffield',
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
    query = f"SELECT ee.emr2_encounter_id, ee.patient_id, YEAR(ee.encounter_date) AS encounter_year, en.emr2_note_id, ene.emr2_note_entry_id, ene.created, YEAR(ene.created) AS entry_year, ene.deleted, ene.deleted_by_id, ene.last_modified_by, ene.last_modified, ene.modified_by_function, CASE WHEN YEAR(ee.encounter_date) <> YEAR(ene.created) THEN 1 ELSE 0 END AS mismatch FROM emr2_encounter ee LEFT JOIN emr2_note en ON ee.emr2_encounter_id = en.emr2_encounter_id LEFT JOIN emr2_note_entry ene ON en.emr2_note_id = ene.emr2_note_id WHERE ee.patient_id = {patient_id} AND DATEDIFF(ene.created, ee.encounter_date) NOT BETWEEN 0 AND 90 AND ene.deleted IS NULL ORDER BY ee.emr2_encounter_id , ene.created;"
    data = pd.read_sql(query, engine)

    # Initialize the rollback script string
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            implementation_query = f"UPDATE `{db_name}`.`emr2_note_entry` SET `deleted` = now(), `deleted_by_id` = '-1000', `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE (`emr2_note_entry_id` = '{row['emr2_note_entry_id']}');"
            implementation_script.append(implementation_query)
            rollback_query = f"UPDATE `{db_name}`.`emr2_note_entry` SET `deleted` = NULL, `deleted_by_id` = NULL, `last_modified_by` = '{row['last_modified_by']}', `last_modified` = '{row['last_modified']}', `modified_by_function` = '{row['modified_by_function']}' WHERE (`emr2_note_entry_id` = '{row['emr2_note_entry_id']}');"
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