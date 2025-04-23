import os
import psycopg2.extras
import uuid
import boto3
import urllib3
from db_conn import *
import common_functions
from care_now_24_logger import *

logger = get_logger("care_now_24_clinic_edit_staff_info")
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """edit staff by doctor_id"""
    conn = connect_to_db(os.environ.get("DB_ENVIORONMENT_PREFIX"))

    try:
        if event["authorizer-principal-id"]["user_type"] != "1":
            return {"statusCode": 403, "message": "Permission denied"}
    except KeyError as e:
        return {"statusCode": 403, "message": "Permission denied"}
    try:
        if event["doctor_id"] in ["", None]:
            return {"statusCode": 549, "message": "doctor_id can not be blank"}
    except KeyError as e:
        return {"statusCode": 550, "message": "doctor_id is mandatory"}
    try:
        if event["staff_id"] in ["", None]:
            return {"statusCode": 501, "message": "staff_id can not be blank"}
    except KeyError as e:
        return {"statusCode": 502, "message": "staff_id is mandatory"}
    try:
        if event["first_name"] in ["", None]:
            return {"statusCode": 503, "message": "first_name of staff can not be blank"}
    except KeyError as e:
        return {"statusCode": 504, "message": "first_name of staff is mandatory"}
    try:
        if event["clinic_id"] in ["", None]:
            return {"statusCode": 549, "message": "clinic_id can not be blank"}
    except KeyError as e:
        return {"statusCode": 550, "message": "clinic_id is mandatory"}
    

    doctor_id = event['doctor_id']
    clinic_id = event["clinic_id"]

    doctor_query = """
        SELECT *
        FROM doctor_master
        WHERE id = '{doctor_id}'
    """.format(doctor_id=doctor_id)
    doctor_query = common_functions.fetch_data(conn, doctor_query)

    if len(doctor_query) < 1:
        return {
            "statusCode": 422,
            "message": "Invalid doctor_id!"
        }

    
    get_staff_query = """
        SELECT bu.id AS staff_id, bu.phone_no, bu.first_name,
            bu.middle_name, bu.last_name
        FROM base_user AS bu
        INNER JOIN staff_clinic_mapping AS sdm ON bu.id = sdm.base_user_id
        INNER JOIN user_type AS ut ON bu.user_type_id = ut.id
        WHERE sdm.clinic_id = '{clinic_id}'::uuid AND
            sdm.is_staff_verify = true AND
            bu.is_enable = true AND
            ut.role = 'staff' AND
            bu.id = '{staff_id}'::uuid
    """.format(clinic_id=clinic_id,
               staff_id=event.get('staff_id'))
    
    get_staff_query = common_functions.fetch_data(conn, get_staff_query)

    if len(get_staff_query) < 1:
        return {
            "statusCode": 422,
            "message": "Invalid staff_id!"
        }

    # If the particular staff is also a staff for other doctors and is not deleted by them
    get_common_staff_query = """
        SELECT base_user_id, clinic_id, is_staff_verify
        FROM staff_clinic_mapping
        WHERE base_user_id = '{base_user_id}'
        AND is_staff_verify = true
        AND clinic_id <> '{clinic_id}'""".format(base_user_id=event['staff_id'], clinic_id=clinic_id)
    
    get_common_staff = common_functions.fetch_data(conn, get_common_staff_query)
    
    if len(get_common_staff) > 0:
        staff_phone_no = get_staff_query['phone_no'].replace("+91", "")
        staff_name = str(get_staff_query['first_name'] + " " + get_staff_query['middle_name'] \
                     + " " + get_staff_query['last_name']).replace("  ", " ").strip()
        if get_staff_query["first_name"].lower() != event['first_name'].lower()\
            or get_staff_query["middle_name"].lower() != event['middle_name'].lower()\
            or get_staff_query["last_name"].lower() != event['last_name'].lower():
            return {
                "statusCode": 500, 
                "message": f"You will not be able to edit staff name and mobile number as this staff with {staff_phone_no} is associated with other doctors with the name of {staff_name}.",
                "staff_phone_no": staff_phone_no,
                "staff_name": staff_name
            }
        else:
            logger.info("staff name matches")
            return {
            "statusCode": 200,
            "message": "Staff info updated successfully"
            }
    else:
        staff_update_query = """
            UPDATE base_user bu
            SET first_name = '{first_name}',
                middle_name = '{middle_name}',
                last_name = '{last_name}',
                updated_at = NOW()
            FROM staff_clinic_mapping sdm, user_type ut
            WHERE bu.id = sdm.base_user_id and
                  bu.user_type_id = ut.id AND
                  sdm.clinic_id = '{clinic_id}'::uuid AND
                  sdm.is_staff_verify = true AND
                  bu.is_enable = true AND
                  ut.role = 'staff' and
                  bu.id = '{staff_id}'::uuid
        """.format(clinic_id=clinic_id,
                   staff_id=event['staff_id'],
                   first_name=event.get('first_name'),
                   middle_name=event.get('middle_name', ''),
                   last_name=event.get('last_name', ''))
    
        response = common_functions.query_exec(conn, staff_update_query)
        if response['statusCode'] != 200:
            return {
                'statusCode': 540,
                'message': 'a problem occured while executing your request.'
            }
        
        return {
            "statusCode": 200,
            "message": "Staff info updated successfully"
        }
