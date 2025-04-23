import json
import datetime
import db_conn
import common_functions
import psycopg2.extras
import uuid
import os
import copy


def lambda_handler(event, context):
    
    # start   validation
    try:
        if str(event['authorizer-principal-id']['user_type']) != '1':
            return {
                "statusCode": 403,
                "message": "Permission denied"
            }
    except KeyError as e:
        return {
            "statusCode": 403,
            "message": "Permission denied"
        }
    try:
        if event['day_of_week'] in ['', None] or int(event['day_of_week']) not in list(range(0, 7)):
            return {
                'statusCode': 501,
                'message': 'day_of_week must be between 0-6'
            }
    except KeyError as e:
        return {
            'statusCode': 502,
            'message': 'day_of_week is mandatory'
        }
    except ValueError as e:
        return {
            'statusCode': 502,
            'message': 'day_of_week must be between 0-6'
        }
    try:
        if event['slot_type'] in ['', None] or int(event['slot_type']) not in list(range(0, 4)):
            return {
                'statusCode': 501,
                'message': 'slot_type is not valid'
            }
    except KeyError as e:
        return {
            'statusCode': 502,
            'message': 'slot_type is mandatory'
        }
    try:
        if event['update_for_week'] not in ['true', True, False, 'false']:
            return {
                'statusCode': 503,
                'message': 'update_for_week is not valid'
            }
    except KeyError as e:
        return {
            'statusCode': 504,
            'message': 'update_for_week is mandatory'
        }
    try:
        if not isinstance(event['regular_slots'], list):
            return {
                'statusCode': 504,
                'message': 'regular_slots has to be array'
            }
    except KeyError as e:
        return {
            'statusCode': 504,
            'message': 'regular_slots is mandatory'
        }
    try:
        if not isinstance(event['follow_up_slots'], list):
            return {
                'statusCode': 504,
                'message': 'follow_up_slots is has to be array'
            }
    except KeyError as e:
        return {
            'statusCode': 504,
            'message': 'follow_up_slots is mandatory'
        }

    # Done input validation
    # define resources
    conn = db_conn.connect_to_db(os.environ.get('DB_ENVIORONMENT_PREFIX'))
    psycopg2.extras.register_uuid()

    # create a variable for storing error days
    no_change_list = []
    error_days_list = []
    query_get_slot_type = """Select slot_type, consultation_type from doctor_slot_details 
                        where doctor_id='{doctor_id}'
                        and consultation_type <> '4' 
                        and is_active ='true'
                        ORDER BY day_of_week
                        """.format(doctor_id=event['doctor_id'],
                                   day_of_week=event['day_of_week'])
    getSlotType = common_functions.fetch_data(conn, query_get_slot_type, True)
    print(len(getSlotType))
    regular_slot_type_list = []
    follow_up_slot_type_list = []
    if len(getSlotType) == 14:
        # doctor_slot_details has follow_up + regular
        follow_up_found = True
    elif len(getSlotType) == 7:
        # doctor_slot_details has regular
        follow_up_found = False
    else:
        return {
            'statusCode': 506,
            'message': 'Your doctor slot details is not configured.'
        }
    for elem in getSlotType:
        if str(elem['consultation_type']) == '0':
            regular_slot_type_list.append({'slot_type': elem['slot_type']})
        else:
            follow_up_slot_type_list.append({'slot_type': elem['slot_type']})

    regular_slots = list(set(event['regular_slots']))
    follow_up_slots = list(set(event['follow_up_slots']))

    all_slots = regular_slots + follow_up_slots
    all_slots_str = "'" + "', '".join(str(id) for id in all_slots) + "'"
    if len(all_slots) == 0:
        get_all_slot_timing = []
    else:
        # getting all slots
        query = """select id, start_time, end_time from appointment_slot_master where id in ({all_slots})""".format(
            all_slots=all_slots_str)
        get_all_slot_timing = common_functions.fetch_data(conn, query, True)

    dict = {}
    for time in get_all_slot_timing:
        each_entry = {time.get("id", None): {'start_time': time['start_time'],
                                             'end_time': time['end_time']
                                             }}
        dict.update(each_entry)

    for index in range(0, 7):
        if event['update_for_week'] in ['true', True]:
            event['day_of_week'] = index
        else:
            pass
        # begin new transaction for each day
        insert_query_cursor = conn.cursor()
        # Business logic start
        # get current data for selected day and doctor
        response = []
        if event['slot_type'] == regular_slot_type_list[int(event['day_of_week'])]['slot_type']:
            # when slot type remain same
            query = """
                SELECT id, slot_id, status, consultation_type FROM doctor_appointment_slots 
                WHERE doctor_id='{doctor_id}'::uuid AND 
                status in (0,1) AND 
                day_of_week='{day_of_week}'
            """.format(
                doctor_id=event['doctor_id'],
                day_of_week=event['day_of_week'],
                slot_type=event['slot_type']
            )
            query = common_functions.fetch_data(conn, query, True)

            deleteable = []
            updateable = []
            regular_slots_day = copy.deepcopy(regular_slots)
            follow_up_slots_day = copy.deepcopy(follow_up_slots)
            
            if len(query) > 0:
                # entries found check them
                for elem in query:

                    if elem['slot_id'] in regular_slots_day and elem['status'] == '1' and elem[
                        'consultation_type'] == '0':
                        # item is in db and in request so no change
                        
                        del regular_slots_day[regular_slots_day.index(elem['slot_id'])]
                        
                    elif elem['status'] == '1' and elem['consultation_type'] == '0' and elem[
                        'slot_id'] not in regular_slots_day:
                        # item in db but not in new request so delete those request
                        
                        deleteable.append(elem['id'])
                    elif elem['slot_id'] in regular_slots_day and elem['status'] == '0' and elem[
                        'consultation_type'] == '0':
                        # item in db but inactive need to activate it
                        
                        del regular_slots_day[regular_slots_day.index(elem['slot_id'])]
                        updateable.append(elem['id'])
                        # REGULAR CONSULATION CHECK ENDS HERE
                        # FOLLOW CHECK STARTS
                    elif elem['slot_id'] in follow_up_slots_day and elem['status'] == '1' and elem[
                        'consultation_type'] == '1':
                        # item is in db and in request so no change
                        del follow_up_slots_day[follow_up_slots_day.index(elem['slot_id'])]
                    elif elem['status'] == '1' and elem['consultation_type'] == '1' and elem[
                        'slot_id'] not in follow_up_slots_day:
                        # item in db but not in new request so delete those request
                        
                        deleteable.append(elem['id'])
                    elif elem['slot_id'] in follow_up_slots_day and elem['status'] == '0' and elem[
                        'consultation_type'] == '1':
                        # item in db but inactive need to activate it
                        del follow_up_slots_day[follow_up_slots_day.index(elem['slot_id'])]
                        updateable.append(elem['id'])
                    else:
                        
                        pass
            response = []
            if len(deleteable) == 0 and len(regular_slots_day) == 0 and len(updateable) == 0 and len(
                    follow_up_slots_day) == 0:
                if event['update_for_week'] in ['false', False]:
                    return {
                        'statusCode': 200,
                        'message': 'no changes required.'
                    }
                else:
                    pass

            if len(regular_slots_day) > 0:
                insert_query = """
                    INSERT INTO doctor_appointment_slots (
                        id, 
                        day_of_week, 
                        slot_id, 
                        slot_type,
                        status,
                        consultation_type,
                        start_time,
                        end_time,
                        created_at, 
                        doctor_id
                    ) VALUES
                    """

                for each in regular_slots_day:
                    print('$$$$$$$$$$$$$$$$$$', each)
                    # all new entries not in db but in request
                    insert_query += """(
                                '{id}', 
                                '{day_of_week}', 
                                '{slot_id}',
                                '{slot_type}',
                                '{status}', 
                                '{consultation_type}', 
                                '{start_time}',
                                '{end_time}',
                                '{created_at}', 
                                '{doctor_id}'
                            ),""".format(
                        id=uuid.uuid4(),
                        day_of_week=event['day_of_week'],
                        slot_id=each,
                        slot_type=event['slot_type'],
                        status='1',
                        consultation_type='0',
                        start_time=dict.get(each)['start_time'],
                        end_time=dict.get(each)['end_time'],
                        created_at=str(datetime.datetime.now()),
                        doctor_id=event['doctor_id']
                    )

                insert_query = insert_query.strip(',')
                responseData = common_functions.query_exec(conn, insert_query,
                                                           insert_query_cursor,
                                                           False if len(deleteable) > 0 or len(updateable) > 0 or len(
                                                               follow_up_slots_day) > 0 else True,
                                                           None,
                                                           'update', "dev")
                response.append(responseData)

            if len(follow_up_slots_day) > 0:
                insert_query = """
                    INSERT INTO doctor_appointment_slots (
                        id, 
                        day_of_week, 
                        slot_id, 
                        slot_type,
                        status,
                        consultation_type,
                        start_time,
                        end_time,
                        created_at, 
                        doctor_id
                    ) VALUES
                    """

                for each in follow_up_slots_day:
                    # all new entries not in db but in request
                    insert_query += """(
                                '{id}', 
                                '{day_of_week}', 
                                '{slot_id}',
                                '{slot_type}',
                                '{status}', 
                                '{consultation_type}', 
                                '{start_time}',
                                '{end_time}',
                                '{created_at}', 
                                '{doctor_id}'
                            ),""".format(
                        id=uuid.uuid4(),
                        day_of_week=event['day_of_week'],
                        slot_id=each,
                        slot_type=event['slot_type'],
                        status='1',
                        consultation_type='1',
                        start_time=dict.get(each)['start_time'],
                        end_time=dict.get(each)['end_time'],
                        created_at=str(datetime.datetime.now()),
                        doctor_id=event['doctor_id']
                    )

                insert_query = insert_query.strip(',')
                responseData = common_functions.query_exec(conn, insert_query,
                                                           insert_query_cursor,
                                                           False if len(deleteable) > 0 or len(
                                                               updateable) > 0 else True,
                                                           None,
                                                           'update', "dev")
                response.append(responseData)
            if len(deleteable) > 0:
                select_query = """
                            SELECT pa.* FROM patient_appointments pa 
                            INNER JOIN appointment_slot_master asm ON pa.appointment_slot_master_id=asm.id
                            WHERE 
                            pa.doctor_id='{doctor_id}'::uuid AND 
                            pa.doctor_appointment_id in ({id_array}) AND
                            pa.status in (0, 1) AND 
                            CONCAT(pa.appointment_date,' ',asm.end_time)::timestamptz >= now()::timestamptz 
                        """.format(
                    doctor_id=event['doctor_id'],
                    id_array="'" + "', '".join(str(id) for id in deleteable) + "'"
                )
                select_query = common_functions.fetch_data(conn, select_query, True)
                
                if len(select_query) > 0:
                    if event['update_for_week'] in ['false', False]:
                        return {
                            'statusCode': 508,
                            'message': 'you are trying to remove pending patient appointment slots. please unselect the fields that has appointment'
                        }
                    else:
                        error_days_list.append(event['day_of_week'])
                        continue
                delete_query = """
                            UPDATE doctor_appointment_slots SET status='0' where id in ({id_array})
                        """.format(
                    id_array="'" + "', '".join(str(id) for id in deleteable) + "'"
                )
                responseData = common_functions.query_exec(conn, delete_query,
                                                           insert_query_cursor, False if len(updateable) > 0 else True,
                                                           None,
                                                           'delete', "dev")
                response.append(responseData)
            if len(updateable) > 0:
                reactivate_query = """
                            UPDATE doctor_appointment_slots SET status='1' where id in ({id_array})
                        """.format(
                    id_array="'" + "', '".join(str(id) for id in updateable) + "'"
                )
                responseData = common_functions.query_exec(conn, reactivate_query,
                                                           insert_query_cursor, True, None, 'update', "dev")
                response.append(responseData)

        # when slot_type is different
        elif event['slot_type'] != regular_slot_type_list[int(event['day_of_week'])]['slot_type']:
            
            if event['update_for_week'] in ['false', False]:
                return {
                    'statusCode': 509,
                    'message': 'Slot type did not match with your consulatation setting.',
                    'details': response
                }
            else:
                error_days_list.append(event['day_of_week'])
                continue

        
        for each_respone in response:
            if each_respone['statusCode'] != 200:
                if event['update_for_week'] in ['false', False]:
                    return {
                        'statusCode': 509,
                        'message': 'a problem occured while executing your request.',
                        'details': response
                    }
                else:
                    error_days_list.append(event['day_of_week'])
                    continue
        if event['update_for_week'] in ['false', False]:
            break
    if event['update_for_week'] in ['false', False]:
        return {
            'statusCode': 200,
            'message': 'Schedule Updated Successfully'
        }
    else:
        days_list = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        if len(error_days_list) == 0:
            return {
                'statusCode': 200,
                'message': 'Schedule Updated Successfully'
            }
        else:
            days = []
            for elem in error_days_list:
                days.append(days_list[elem])
            message = 'Schedule Updated Successfully Except ' + ', '.join(
                days) + ' since you have prior consultations booked for these days.'
            return {
                'statusCode': 201,
                'message': message
            }