import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
from datetime import datetime
import urllib.parse
import sys

client = sys.argv[1]

if client == 'BUPA UAT':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.129.55')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'bupa_uat')
elif client == 'BUPA Live':
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
elif client == 'Nuffield UAT':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.130.93')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'nuffield_uat')
elif client == 'Newmedica Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.131.33')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'newmedica_live')
elif client == 'Newmedica UAT':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.131.93')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'newmedica_uat')
elif client == 'Onebright Live':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.136.63')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'onebright_live')
elif client == 'Onebright UAT':
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.136.91')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'onebright_uat')
else:
    print('Invalid client!!')

db_to_folder = {
    'bupa_production_v6': 'BUPA',
    'bupa_uat': 'BUPA',
    'nuffield_live': 'Nuffield',
    'nuffield_uat': 'Nuffield',
    'newmedica_live': 'Newmedica',
    'newmedica_uat': 'Newmedica',
    'onebright_live': 'Onebright',
    'onebright_uat': 'Onebright'
}
folder_name = db_to_folder.get(db_name, 'UnknownDB')

# Connection string for MySQL
connection_string = f'mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}'
print(f"Connecting to database: {db_name}")
print("\n")

# Create a database connection
try:
    engine = create_engine(connection_string)
    connection = engine.connect()
    print(f"Successfully connected to database: {db_name}")
    print("\n")
except Exception as e:
    print(f"Error connecting to database: {e}")
    print("\n")
    connection = None

if connection:
    modified_by_function = sys.argv[3]
    base_folder = f'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/{folder_name}/{modified_by_function}'

    
    #query = f"select * from dashboard_view where dashboard_workflow_id = 58 and dashboard_view_name = 'Cataract Post-Op to Book';"
    query = sys.argv[2]
    #print(f"Rollback script saved as: " f"C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/{folder_name}/{modified_by_function}/rollback_script.sql")
    data = pd.read_sql(text(query), engine)

    # --- Output folder ---
    output_dir = f'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/{folder_name}/{modified_by_function}/{db_name}'
    os.makedirs(output_dir, exist_ok=True)

    # --- Save each view_query as DBVID_{dashboard_view_id}.txt ---
    for _, row in data.iterrows():
        try:
            view_id = row["dashboard_view_id"]
            view_query = row["view_query"]
            count_query = row["count_query"]

            if 'uat' in db_name:
                prefix = 'uat'
            else:
                prefix = 'live'

            filename = os.path.join(output_dir, f"DBVID_{view_id}_{prefix}.sql")
            filename2 = os.path.join(output_dir, f"DBVID_{view_id}_{prefix}_count.sql")

            with open(filename, "w", encoding="utf-8") as f:
                f.write(view_query)

            with open(filename2, "w", encoding="utf-8") as f:
                f.write(count_query)
            
            print(f"Saved view query at: {filename}")
            print(f"Saved count query at: {filename2}")
        except Exception as e:
            print(f"❌ Failed for DBVID {view_id}: {e}")
            raise

else:
    print("Unable to establish database connection.")