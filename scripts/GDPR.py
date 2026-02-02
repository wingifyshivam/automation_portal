import os
import pandas as pd
from sqlalchemy import create_engine, text
from itertools import chain
from datetime import datetime
import urllib.parse
import sys

database = sys.argv[1]

if database == "BUPA Live":
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.129.12')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'bupa_production_v6')
elif database == "Nuffield Live":
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.130.107')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'nuffield_live')
elif database == "Onebright Live":
    db_username = os.getenv('DB_USERNAME', 'shivam.a')
    db_password = os.getenv('DB_PASSWORD', 'Shivam_1994')
    db_host = os.getenv('DB_HOST', '172.20.136.63')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME', 'onebright_live')
else:
    print("Unsupported DB")

db_to_folder = {
    'bupa_production_v6': 'BUPA',
    'nuffield_live': 'Nuffield',
    'onebright_live': 'Onebright'
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
    #print("Database connection successful.")
    print(f"Successfully connected to database: {db_name}")
    print("\n")
except Exception as e:
    print(f"Error connecting to database: {e}")
    print("\n")
    connection = None

if connection:
    patient_id = sys.argv[2]
    modified_by_function = sys.argv[3]
    base_folder = f'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Desktop/Scripts/{folder_name}/{modified_by_function}'

    # Replace the below with the path where the GDPR extract script is stored on your local machine
    original_file_path = 'C:/Users/ShivamAggarwal/OneDrive - Health Catalyst/Important Guides/GDPR_All_Scripts New_v5 - CPM 5.2.sql'

    
    appt_query = f"select 'Appointment ID','Appointment Type', 'Appointment Date','Start Time','End Time', 'Contract', 'Location', 'Practitioner Name','Is Primary Practitioner', 'DNA','DNA Reason', 'Cancellation','Cancellation Reason', 'Class','Discharge Date', 'Insurance Auth Code', 'Status', 'e-Referral Universal Booking Reference Number', 'Apppintment Comments','Appointment Patient Comments', 'Is Appointment Booklet', 'Charge Amount','Currency','Charge Quantity','Charge Date Raised', 'Charge Contract','Has Clinical Report', 'Payor', 'Referral ID', 'Has EMR2 Encounter', 'Has Lab Test', 'Has Physio Examination' UNION ALL select a.appointment_id 'Appointment ID',apt.appointment_type_name 'Appointment Type', DATE_FORMAT(a.start,'%d/%m/%y') 'Appointment Date', ifnull(DATE_FORMAT(aps.start,'%H:%i'),DATE_FORMAT(a.start,'%H:%i')) 'Start Time', ifnull(DATE_FORMAT(aps.end,'%H:%i'),DATE_FORMAT(a.end,'%H:%i')) 'End Time', c.contract_name 'Contract',l.location_name 'Location', ifnull(CONCAT(di.title,' ',di.forename,' ',di.surname),CONCAT(dr.forename,' ',dr.surname)) 'Practitioner Name', case when a.primary_doctor_id = aps.doctor_id then 'Yes' else 'No' End 'Is Primary Practitioner', case when a.dna = 1 then 'Yes' else 'No' End 'DNA',cdr.description 'DNA Reason', case when a.status = 'N' then 'Yes' else 'No' End 'Cancellation',ccr.description 'Cancellation Reason', clt.class_type_name 'Class',DATE_FORMAT(a.discharge,'%d/%m/%y') 'Discharge Date', a.insurance_authorisation_code 'Insurance Auth Code', case when a.status in ('A','H') then 'Active' when a.status in ('C','N') then 'Cancelled' else 'N/A' End 'Status', a.cab_ubrn 'e-Referral Universal Booking Reference Number', a.comments 'Apppintment Comments',patient_comments 'Appointment Patient Comments', case when a.appointment_booklet_id is not null then 'Yes' else 'No' End 'Is Appointment Booklet', ch.price 'Charge Amount',ch.currency 'Currency',ch.quantity 'Charge Quantity',DATE_FORMAT(ch.effective,'%d/%m/%y') 'Charge Date Raised', ch_c.contract_name 'Charge Contract',case when cr.report_id is not null then 'Yes' else 'No' End 'Has Clinical Report', case when patient_payor.individual_id is not null then 'PATIENT' when company_payor.individual_id is not null then company_payor.company_name when ic_payor.individual_id is not null then ic_payor.insurance_company_name else null End 'Payor', a.treatment_cycle_referral_id 'Referral ID', case when e.appointment_id is not null then 'Yes' else 'No' End 'Has EMR2 Encounter', case when oo.appointment_id is not null then 'Yes' else 'No' End 'Has Lab Test', case when pe.appointment_id is not null then 'Yes' else 'No' End 'Has Physio Examination' from appointment a JOIN appointment_type apt ON (apt.appointment_type_id = a.appointment_type_id) LEFT JOIN appointment_section aps ON (aps.appointment_id = a.appointment_id and aps.status = 'A') JOIN contract c ON (c.contract_id = a.contract_id) JOIN location l ON (l.location_id = a.location_id) LEFT JOIN individual di ON (di.individual_id = aps.doctor_id) LEFT JOIN individual dr ON (dr.individual_id = a.primary_doctor_id) LEFT JOIN charge ch ON (a.appointment_id = ch.appointment_id) LEFT JOIN contract ch_c ON (ch_c.contract_id = ch.contract_id) LEFT JOIN cab_dna_reason cdr ON (cdr.cab_dna_reason_code = a.cab_dna_reason_code) LEFT JOIN cab_cancellation_reason ccr ON (ccr.cab_cancellation_reason_code = a.cab_cancellation_reason_code) LEFT JOIN class cl ON (cl.class_id = a.class_id) LEFT JOIN class_type clt ON (cl.class_type_id = clt.class_type_id) LEFT JOIN clinical_report cr ON (a.appointment_id = cr.appointment_id) LEFT JOIN individual patient_payor ON (patient_payor.individual_id = ch.charge_to_id) LEFT JOIN company company_payor ON (company_payor.individual_id = ch.charge_to_id) LEFT JOIN insurance_company ic_payor ON (ic_payor.individual_id = ch.charge_to_id) LEFT JOIN emr2_encounter e ON (e.appointment_id = a.appointment_id) LEFT JOIN observation_order oo ON (oo.appointment_id = a.appointment_id) LEFT JOIN physio_examination pe ON (pe.appointment_id = a.appointment_id) LEFT JOIN treatment_cycle_referral tcr ON (tcr.treatment_cycle_referral_id = a.treatment_cycle_referral_id) where a.patient_id = {patient_id} group by a.appointment_id,aps.appointment_section_id;"
    appt_data = pd.read_sql(text(appt_query), engine)
    
    emr_query = f"select 'Appointment ID','Appointment Type', 'Appointment Date','Start Time', 'Practitioner Name', 'Entry Date', 'Entry Date Description', 'EMR Description', 'EMR Notes', 'EMR Subjective', 'EMR Subjective Description', 'EMR Objective', 'EMR Objective Description', 'EMR Analysis', 'EMR Plan', 'EMR OCICS Description' UNION ALL select a.appointment_id 'Appointment ID',apt.appointment_type_name 'Appointment Type', DATE_FORMAT(a.start,'%d/%m/%y') 'Appointment Date',DATE_FORMAT(a.start,'%H:%i') 'Start Time', CONCAT(di.title,' ',di.forename,' ',di.surname) 'Practitioner Name', er.entry_date 'Entry Date', er.entry_date_description    'Entry Date Description', er.description    'EMR Description', er.notes 'EMR Notes', er.subjective 'EMR Subjective', er.subjective_description 'EMR Subjective Description', er.objective 'EMR Objective', er.objective_description 'EMR Objective Description', er.analysis 'EMR Analysis', er.plan 'EMR Plan', er.ocics_description 'EMR OCICS Description' from emr_history er LEFT JOIN appointment a ON (a.appointment_id = er.appointment_id) LEFT JOIN appointment_type apt ON (apt.appointment_type_id = a.appointment_type_id) JOIN individual di ON (di.individual_id = er.practitioner_id) where er.patient_id = {patient_id};"
    emr_data = pd.read_sql(text(emr_query), engine)
    
    clinical_reports_query = f"select 'Appointment ID','Report ID','Date Created','Published Date','Printed Date','Version Number','Creator of Report','Clinical Report Code' UNION ALL select a.appointment_id 'Appointment ID', crv.report_id 'Report ID', crv.created_date 'Date Created', crv.published_date 'Published Date', crv.printed_date 'Printed Date', crv.version_number 'Version Number', CONCAT(pr.title,' ',pr.forename,' ',pr.surname) 'Creator of Report', report_xml 'Clinical Report Code' from clinical_report cr JOIN appointment a ON (a.appointment_id = cr.appointment_id) JOIN  (select report_id,max(report_version_id) max_report_version_id from clinical_report_version crv where crv.status = 'A' group by report_id)x ON (cr.report_id = x.report_id) JOIN clinical_report_version crv ON (crv.report_version_id = x.max_report_version_id) JOIN individual pr ON (pr.individual_id = crv.created_by_id) where cr.patient_id = {patient_id};"
    clinical_reports_data = pd.read_sql(text(clinical_reports_query), engine)
    
    emr2_query = f"select 'Appointment ID', 'Appointment Date', 'Appointment Type', 'Encounter Date', 'Practitioner Name', 'Has Order', 'Order Completed Status', 'Completed Date', 'Order Status', 'Order Reviewed By', 'Entry Name', 'Entry Response', 'Entry Response Units', 'Entry Notes', 'Entry Text', 'Full Entry Response', 'Problem Description', 'Observation Description', 'Observation Result', 'Full Observation Result', 'Observation Result Status', 'Procedure Type', 'Procedure Type Category' UNION ALL select a.appointment_id 'Appointment ID', DATE_FORMAT(a.start,'%d/%m/%y') 'Appointment Date', apt.appointment_type_name 'Appointment Type', DATE_FORMAT(a.start,'%d/%m/%y') 'Encounter Date', CONCAT(di.title,' ',di.forename,' ',di.surname) 'Practitioner Name', case when eo.emr2_order_id is not null then 'Yes' else 'No' End 'Has Order', order_completion_type_name 'Order Completed Status', DATE_FORMAT(eo.complete_by,'%d/%m/%y') 'Completed Date', os.name    'Order Status', CONCAT(di.title,' ',di.forename,' ',di.surname) 'Order Reviewed By', ne.entry_name 'Entry Name', ne.entry_value 'Entry Response', ne.entry_value_units 'Entry Response Units', ne.entry_notes 'Entry Notes', ne.entry_text 'Entry Text', CONCAT(ifnull(ne.entry_value,''),' ',ifnull(ne.entry_value_units,'')) 'Full Entry Response', ep.problem_name 'Problem Description', o.observation_name 'Observation Description', o.observation_value 'Observation Result', CONCAT(ifnull(o.observation_value,''),' ',ifnull(o.units,'')) 'Full Observation Result', oaf.description    'Observation Result Status', ept.procedure_type_name    'Procedure Type', eptc.procedure_type_category_name 'Procedure Type Category' from emr2_encounter e JOIN appointment a ON (a.appointment_id = e.appointment_id) JOIN appointment_type apt ON (apt.appointment_type_id = a.appointment_type_id) JOIN emr2_note n ON (n.emr2_encounter_id = e.emr2_encounter_id) JOIN individual di ON (di.individual_id = e.practitioner_id) JOIN emr2_note_entry ne ON (ne.emr2_note_id = n.emr2_note_id) LEFT JOIN emr2_order eo ON (eo.emr2_encounter_id = e.emr2_encounter_id) LEFT JOIN emr2_order_completion_type oct ON (oct.emr2_order_completion_type_id = eo.emr2_order_completion_type_id) LEFT JOIN emr2_order_status os ON (os.emr2_order_status_id = eo.emr2_order_status_id) LEFT JOIN individual oi ON (oi.individual_id = eo.reviewed_by_id) LEFT JOIN emr2_assessment ea ON (ea.emr2_note_entry_id = ne.emr2_note_entry_id) LEFT JOIN emr2_problem ep ON (ep.emr2_problem_id = ea.emr2_problem_id) LEFT JOIN observation o ON (o.observation_id = ne.observation_id) LEFT JOIN observation_abnormal_flag oaf ON (oaf.abnormal_flag = o.abnormal_flag) LEFT JOIN emr2_procedure epr ON (epr.emr2_note_entry_id = ne.emr2_note_entry_id) LEFT JOIN emr2_procedure_type ept ON (ept.emr2_procedure_type_id = epr.emr2_procedure_type_id) LEFT JOIN emr2_procedure_type_category eptc ON (eptc.emr2_procedure_type_category_id = ept.emr2_procedure_type_category_id) where e.patient_id = {patient_id};"
    emr2_data = pd.read_sql(text(emr2_query), engine)
    
    form_response_by_appt_query = f"select 'Appointment ID','Created Date','Question Name','Response (Answer)','Response Notes' UNION ALL select fr.appointment_id 'Appointment ID', DATE_FORMAT(fr.created_date,'%d/%m/%y') 'Created Date', question_name 'Question Name', ifnull(fr.response,response_long) 'Response (Answer)', fr.notes 'Response Notes' from form_response fr JOIN (select appointment_id,fr.form_question_id,max(form_response_id) max_form_response_id from form_response fr where fr.deleted_by_id is null AND fr.patient_id = {patient_id} and fr.appointment_id is not null AND fr.form_type_category_id = 1 group by fr.patient_id,fr.appointment_id,fr.form_question_id)x_fr ON (x_fr.max_form_response_id = fr.form_response_id) JOIN (select fqv.form_question_id,fqv.question_name from form_question_version fqv JOIN (select form_question_id,max(form_question_version_id) max_form_question_version_id from form_question_version where status = 'A' group by form_question_id)x ON (x.max_form_question_version_id = fqv.form_question_version_id))fqv ON (fqv.form_question_id = fr.form_question_id) where fr.patient_id = {patient_id};"
    form_response_by_appt_data = pd.read_sql(text(form_response_by_appt_query), engine)
    
    form_response_by_referral_query = f"select 'Referral ID','Created Date','Question Name','Response (Answer)','Response Notes' UNION ALL select fr.treatment_cycle_referral_id 'Referral ID', DATE_FORMAT(fr.created_date,'%d/%m/%y') 'Created Date', question_name 'Question Name', ifnull(fr.response,response_long) 'Response (Answer)', fr.notes 'Response Notes' from form_response fr JOIN (select fr.treatment_cycle_referral_id,fr.form_question_id,max(form_response_id) max_form_response_id from form_response fr JOIN form_question_instance fqi ON (fqi.form_question_instance_id = fr.form_question_instance_id) JOIN form f ON (f.form_id = fqi.form_id) where fr.deleted_by_id is null AND fr.patient_id = {patient_id} AND fr.form_type_category_id = 1 group by fr.treatment_cycle_referral_id,fr.form_question_id)x_fr ON (x_fr.max_form_response_id = fr.form_response_id) JOIN (select fqv.form_question_id,fqv.question_name from form_question_version fqv JOIN (select form_question_id,max(form_question_version_id) max_form_question_version_id from form_question_version where status = 'A' group by form_question_id)x ON (x.max_form_question_version_id = fqv.form_question_version_id))fqv ON (fqv.form_question_id = fr.form_question_id) where fr.patient_id = {patient_id};"
    form_response_by_referral_data = pd.read_sql(text(form_response_by_referral_query), engine)
    
    patient_details_query = f"SELECT 'Patient ID' column_1, 'Name' column_2, 'Middle Name' column_2_1, 'Date Of Birth' column_3,'Daytime Number' column_4,'Evening Number' column_5, 'Mobile Number' column_6,'Fax Number' column_7,'Email Address' column_8, 'Address' column_9, 'Next of Kin Name' column_10,'Relation to Next of Kin' column_11, 'Date Of Death' column_12,'Preferred Name' column_13, 'NHS Number' column_14, 'Medical Record Number' column_15, 'Referral Method Comments' column_16, 'PESEL Number' column_17, 'Insurance Company Name' column_18, 'Insurance Policy Reference' column_19, 'Employee Company Reference' column_20, 'Primary Company Name' column_21, 'Primary Employee Reference' column_22, 'Primary Job Title' column_23, 'Primary Cost Code' column_24, 'Primary Cost Centre' column_25, 'Additional Company Details (Employee Ref,Job Title,Cost Code & Cost Centre)' column_26, 'Patient Notes' column_27, 'Patient Comments' column_28, 'Important Comments' column_29, 'Alternative Email Address' column_31 UNION ALL select pi.individual_id 'Patient ID', CONCAT(pi.title,' ',pi.forename,' ',pi.surname) 'Name', pi.middlename 'Middle Name',DATE_FORMAT(pi.dob,'%d/%m/%Y') 'Date Of Birth',itnd.number 'Daytime Number',itne.number 'Evening Number', itnm.number 'Mobile Number',pi.fax 'Fax Number',pi.email 'Email Address', replace(replace(CONCAT(ifnull(address_1,''),',',ifnull(address_2,''),',',ifnull(address_3,''),',',ifnull(address_4,''),',',ifnull(address_5,''),',', ifnull(town,''),',',ifnull(county,''),',',ifnull(postcode,'')),',,',','),',,',',') 'Address', CONCAT(pi.title,' ',pi.forename,' ',pi.surname) 'Next of Kin Name',next_of_kin_relation 'Relation to Next of Kin', DATE_FORMAT(p.date_of_death,'%d/%m/%Y') 'Date Of Death', CONCAT(ifnull(p.preferred_title,''),' ',ifnull(p.preferred_forename,''),' ',ifnull(p.preferred_middlename,''),' ',ifnull(p.preferred_surname,'')) 'Preferred Name', ifnull(p.nhs_number,'N/A') 'NHS Number', ifnull(p.medical_record_number,'N/A') 'Medical Record Number', ifnull(p.referral_method_comments,'N/A') 'Referral Method Comments', ifnull(p.pesel_number,'N/A') 'PESEL Number', ifnull(ic.insurance_company_name,'N/A') 'Insurance Company Name', ifnull(p.insurance_policy_reference,'N/A') 'Insurance Policy Reference', ifnull(p.employee_company_reference,'N/A') 'Employee Company Reference', primary_c.company_name 'Primary Company Name', ifnull(p.employee_reference,'N/A') 'Primary Employee Reference', ifnull(p.job_title,'N/A') 'Primary Job Title', ifnull(p.cost_code,'N/A') 'Primary Cost Code', ifnull(p.cost_centre,'N/A') 'Primary Cost Centre', GROUP_CONCAT(DISTINCT CONCAT('Company Name: ',ifnull(c.company_name,'N/A')),' \n ',CONCAT('Employee Reference: ',ifnull(pac.employee_reference,'N/A')),' \n ', CONCAT('Job Title: ',ifnull(pac.job_title,'N/A')),' \n ',CONCAT('Cost Code: ',ifnull(pac.cost_code,'N/A')),' \n ', CONCAT('Cost Centre: ',ifnull(pac.cost_centre,'N/A')) SEPARATOR ';') as 'Additional Company Details (Employee Ref,Job Title,Cost Code & Cost Centre)', ifnull(pi.notes,'N/A') 'Patient Notes', ifnull(p.comments,'N/A') 'Patient Comments', ifnull(p.comments_important,'N/A') 'Important Comments', ifnull(p.comments_critical,'N/A') 'Critical Comments' from individual pi JOIN patient p ON (pi.individual_id = p.individual_id) LEFT JOIN address a ON (a.individual_id = pi.individual_id) LEFT JOIN individual ni ON (p.next_of_kin_id = ni.individual_id) LEFT JOIN insurance_company ic ON (p.insurance_company_id = ic.individual_id) LEFT JOIN patient_additional_company pac ON (pac.patient_id = pi.individual_id) LEFT JOIN company c ON (c.individual_id = pac.company_id) LEFT JOIN company primary_c ON (primary_c.individual_id = p.company_id) LEFT JOIN individual nok ON (nok.individual_id = p.next_of_kin_id) LEFT JOIN individual_telephone_number itnd ON pi.individual_id = itnd.individual_id AND itnd.label = 'day' LEFT JOIN individual_telephone_number itne ON pi.individual_id = itne.individual_id AND itne.label = 'evening' LEFT JOIN individual_telephone_number itnm ON pi.individual_id = itnm.individual_id AND itnm.label = 'mobile' where pi.individual_id = {patient_id} GROUP BY pi.individual_id;"
    patient_details_data = pd.read_sql(text(patient_details_query), engine)
    
    labs_query = f"select observation_date, ordered_by_name, observation_name, observation_value, units, reference_range, abnormal_flag, result_status from observation_set left join observation on observation_set.observation_set_id = observation.observation_set_id where observation_set.patient_id = {patient_id} and observation_set.status = 'A' order by observation_date;"
    labs_data = pd.read_sql(text(labs_query), engine)

    physio_query = f"SELECT physio_examination_id 'Physio Examination ID', appointment_id 'Appointment ID', created_date 'Created Date', treatment_plan 'Treatment Plan', observations 'Observations', clinical_impression 'Clinical Impression', functional_tests 'Functional Tests', range_of_motion 'Range of Motion', muscle_testing_control 'Muscle Testing Control', special_tests 'Special Tests', palpation 'Palpation', problem_list 'Problem List' FROM physio_examination WHERE patient_id = {patient_id};"
    physio_data = pd.read_sql(text(physio_query), engine)

    paper_archive_query = f"SELECT file_id, patient_id, folder_id, given_name, extension, generated_name, upload_id FROM paper_file where patient_id in ({patient_id});"
    paper_archive_data = pd.read_sql(text(paper_archive_query), engine)

    letters_query = f"select pl.letter_id as `Letter Id`, pl.patient_id as `Patient Id`, plv.subject as `Letter Subject`, plv.file_name as `File name` from patient_letter pl left join patient_letter_revision plv on pl.letter_id = plv.letter_id where pl.patient_id in ({patient_id});"
    letters_data = pd.read_sql(text(letters_query), engine)
        
    if appt_data.shape[0] > 1 or emr_data.shape[0] > 1 or clinical_reports_data.shape[0] > 1 or emr2_data.shape[0] > 1 or form_response_by_appt_data.shape[0] > 1 or form_response_by_referral_data.shape[0] > 1 or patient_details_data.shape[0] > 1 or labs_data.shape[0] > 0 or physio_data.shape[0] > 0 or paper_archive_data.shape[0] > 0 or letters_data.shape[0] > 0:
        os.makedirs(base_folder, exist_ok=True)
        replacement = patient_id
        new_file_path = f'{base_folder}/GDPR_Data_Export_{patient_id}.sql'

        # Read original file
        with open(original_file_path, 'r', encoding='utf-8') as file:
            sql_content = file.read()

        # Replace the text
        modified_content = sql_content.replace('1851523', str(replacement))

        # Save modified content to a new file
        with open(new_file_path, 'w', encoding='utf-8') as file:
            file.write(modified_content)
    else:
        print('It is likely that you have selected the wrong DB or patient ID !!!')
        exit()
    
    if appt_data.shape[0] > 1:
        appointment_file_path = f'{base_folder}/Appointments.xlsx'
        appt_data.to_excel(appointment_file_path, index=False)
    else:
        print("No data in Appointments")

    if emr_data.shape[0] > 1:
        emr_file_path = f'{base_folder}/EMR.xlsx'
        emr_data.to_excel(emr_file_path, index=False)
    else:
        print("No data in EMR")

    if clinical_reports_data.shape[0] > 1:
        clinical_reports_file_path = f'{base_folder}/Clinical Reports.xlsx'
        clinical_reports_data.to_excel(clinical_reports_file_path, index=False)
    else:
        print("No data in Clinical Reports")

    if emr2_data.shape[0] > 1:
        emr2_file_path = f'{base_folder}/EMR2.xlsx'
        emr2_data.to_excel(emr2_file_path, index=False)
    else:
        print("No data in EMR2")

    if form_response_by_appt_data.shape[0] > 1:
        form_response_by_appt_file_path = f'{base_folder}/Form Response by Appointment.xlsx'
        form_response_by_appt_data.to_excel(form_response_by_appt_file_path, index=False)
    else:
        print("No data in form response by appointment")

    if form_response_by_referral_data.shape[0] > 1:
        form_response_by_referral_file_path = f'{base_folder}/Form Response by Referral.xlsx'
        form_response_by_referral_data.to_excel(form_response_by_referral_file_path, index=False)
    else:
        print("No data in form response by referral")

    if patient_details_data.shape[0] > 1:
        patient_details_file_path = f'{base_folder}/Patient Details.xlsx'
        patient_details_data.to_excel(patient_details_file_path, index=False)
    else:
        print("No data in form response by referral")

    if labs_data.shape[0] > 0:
        labs_file_path = f'{base_folder}/Labs.xlsx'
        labs_data.to_excel(labs_file_path, index=False)
    else:
        print("No data in labs")

    if physio_data.shape[0] > 0:
        physio_file_path = f'{base_folder}/Physio Examination.xlsx'
        physio_data.to_excel(physio_file_path, index=False)
    else:
        print("No data in physio examination")

    if paper_archive_data.shape[0] > 0:
        paper_archive_file_path = f'{base_folder}/Paper Archive.xlsx'
        paper_archive_data.to_excel(paper_archive_file_path, index=False)
        print("There is some data in paper archive. Please raise a TOPS")
    else:
        print("No data in paper archive")

    if letters_data.shape[0] > 0:
        letters_file_path = f'{base_folder}/Letters.xlsx'
        letters_data.to_excel(letters_file_path, index=False)
        print("There is some data in letters. Please raise a TOPS")
    else:
        print("No data in letters")

    print("Data saved at:",base_folder)

else:
    print("Unable to establish database connection.")