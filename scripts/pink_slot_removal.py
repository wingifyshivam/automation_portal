import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
import urllib.parse
from tabulate import tabulate
import sys
import json
from datetime import datetime

# Database connection details
db_username = os.getenv('DB_USERNAME', 'shivam.a')
db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
db_host = os.getenv('DB_HOST', '172.20.131.33')
db_port = os.getenv('DB_PORT', '3306')
db_name = os.getenv('DB_NAME', 'newmedica_live')

db_to_folder = {
    'newmedica_live': 'Newmedica'
}
folder_name = db_to_folder.get(db_name, 'UnknownDB')

# Connection string for MySQL
connection_string = f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
# print(f"Connecting to database: {db_name}")

# Create a database connection
try:
    engine = create_engine(connection_string)
    connection = engine.connect()
    # print("Database connection successful.")
except Exception as e:
    # print(f"Error connecting to database: {e}")
    connection = None

if connection:
    diary_date = sys.argv[1]
    d1 = datetime.strptime(diary_date, "%Y-%m-%dT%H:%M")
    modified_by_function = sys.argv[2]
    query = text(r"""
    SELECT 
       d.diary_date,
       asec.start,
       a.appointment_id,
       asec.appointment_section_id
   FROM
       appointment_section asec
           LEFT JOIN
       appointment a ON a.appointment_id = asec.appointment_id
           LEFT JOIN
       diary d ON asec.diary_id = d.diary_id
           LEFT JOIN
       room r ON r.room_id = d.room_id
           LEFT JOIN
       location l ON l.location_id = r.location_id
           LEFT JOIN
       treatment_cycle_referral tcr ON asec.ubrn = tcr.ubrn
           LEFT JOIN
       individual pat ON pat.individual_id = tcr.patient_id
           LEFT JOIN
       individual doc ON d.doctor_id = doc.individual_id
       LEFT JOIN cab_error on a.cab_ubrn = cab_error.ubrn and cab_error.ubrn not like '%________________________________%'
       LEFT JOIN cab_error cetcr on tcr.ubrn = cetcr.ubrn
       LEFT JOIN patient ON patient.individual_id =pat.individual_id
       LEFT JOIN patient pat2 ON pat2.individual_id = a.patient_id
   WHERE
       (asec.reservation = 1 OR a.status = 'S')
  	 AND diary_date >= DATE(:diary_date)
       AND diary_date <= DATE(:diary_date)
  	 AND asec.status = 'A'
   ORDER BY d.diary_date;
    """)
    data = pd.read_sql(query, engine, params={"diary_date": diary_date})

    # Initialize the rollback script string
    implementation_script = []
    rollback_script = []

    # Iterate over each row in the dataframe to construct the SQL UPDATE statements
    try:
        for _, row in data.iterrows():
            d2 = row['start']
            if hasattr(d2, "to_pydatetime"):
                d2 = d2.to_pydatetime()
            if d1 == d2:
                a = f"select appointment_section_id, status, last_modified_by, last_modified, modified_by_function from appointment_section where appointment_section_id = {row['appointment_section_id']};"
                data1 = pd.read_sql(a, engine)
                for _, row1 in data1.iterrows():
                    implementation_query = f"UPDATE `{db_name}`.`appointment_section` SET `status` = 'C', `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE (`appointment_section_id` = '{row1['appointment_section_id']}');"
                    implementation_script.append(implementation_query)
                    rollback_query = f"UPDATE `{db_name}`.`appointment_section` SET `status` = '{row1['status']}', `last_modified_by` = '{row1['last_modified_by']}', `last_modified` = '{row1['last_modified']}', `modified_by_function` = '{row1['modified_by_function']}' WHERE (`appointment_section_id` = '{row1['appointment_section_id']}');"
                    rollback_script.append(rollback_query)
                if row['appointment_id']:
                    b = f"select appointment_id, status, last_modified_by, last_modified, modified_by_function from appointment where appointment_id = {row['appointment_id']};"
                    data2 = pd.read_sql(b, engine)
                    for _, row2 in data2.iterrows():
                        implementation_query = f"UPDATE `{db_name}`.`appointment` SET `status` = 'C', `last_modified_by` = '-1000', `last_modified` = now(), `modified_by_function` = '{modified_by_function}' WHERE (`appointment_id` = '{row2['appointment_id']}');"
                        implementation_script.append(implementation_query)
                        rollback_query = f"UPDATE `{db_name}`.`appointment` SET `status` = '{row2['status']}', `last_modified_by` = '{row2['last_modified_by']}', `last_modified` = '{row2['last_modified']}', `modified_by_function` = '{row2['modified_by_function']}' WHERE (`appointment_id` = '{row2['appointment_id']}');"
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