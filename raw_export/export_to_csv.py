# Requires epcpy module for EPC decoding - pip install epcpy
# Requires requests module for HTTP requests - pip install requests

import sys
import os
from datetime import datetime, timedelta, date

import pandas as pd
import requests
from epcpy.epc_schemes import SGTIN, sgtin


API_KEY = '' #Define API key in here or set in environment variable as CRAVE_API_KEY

if not API_KEY:
    API_KEY = os.environ.get('CRAVE_API_KEY')

HEADERS = {'Authorization': f'api-token {API_KEY}',
           'Content-Type': 'application/json'}
START_DATE = (date.today() - timedelta(1)).strftime('%Y-%m-%d')  # Using yesterday's date, yyyy-MM-dd

BASE_URL_LIST = {'shopper': '/v2/archive/shopper',
                 'item': '/v2/archive/shopper_item',
                 'requests': '/v2/archive/request',
                 'feedback': '/v2/archive/feedback',
                 'suspicious_activity_alert': '/v2/archive/suspicious_activity_alert',
                 'suspicious_activity': '/v2/archive/suspicious_activity'}

KEY_LIST = {
    'shopper': ['id', 'name', 'storeId', 'createdAt', 'itemCount', 'deletedAt', 'dwellMilliseconds', 'shopperId',
                'type', 'associateId', 'changingRoomId'],
    'item': ['id', 'shopperArchiveId', 'sku', 'price', 'storeId', 'createdAt', 'itemId',
             'productId', 'serial', 'title', 'size', 'size2', 'category', 'color', 'epc', 'rfidStatus'],
    'requests': ['id', 'status', 'createdBy', 'storeId', 'changingRoomId', 'assignedUserId', 'sku', 'createdAt',
                 'assignedAt', 'completedAt', 'size', 'color', 'price', 'timeTaken', 'type', 'itemId', 'productId',
                 "originalRequestId", 'title', 'category', 'size2', 'origin'],
    'feedback': ['id', 'shopperName', 'rating', 'storeId', 'deviceRating', 'createdAt'],
    'suspicious_activity_alert': ['id', 'changingRoomId', 'shopperArchiveId', 'storeId', 'score', 'assignedUserId',
                                  'status', 'createdAt', 'completedAt', 'archivedAt'],
    'suspicious_activity': ['id', 'suspiciousActivityAlertArchiveId', 'type', 'score', 'count', 'createdAt',
                            'archivedAt', 'changingRoomId', 'shopperArchiveId', 'storeId', 'note'],
    'room': ['storeId', 'areaId', 'roomId', 'areaName', 'roomName'],
    'store': ['externalId', 'storeId', 'storeName'],
    'user': ['id', 'externalId', 'username', 'email', 'firstName', 'lastName']}

FILE_NAME_LIST = {'shopper': 'shopper.csv',
                  'item': 'item.csv',
                  'requests': 'requests.csv',
                  'feedback': 'feedback.csv',
                  'suspicious_activity_alert': 'suspicious_activity_alert.csv',
                  'suspicious_activity': 'suspicious_activity.csv',
                  'room': 'changingRoom.csv',
                  'store': 'store.csv',
                  'user': 'user.csv'}

OPTION_MENU = ['shopper', 'item', 'requests', 'feedback', 'suspicious_activity_alert', 'suspicious_activity', 'room',
               'user']

REGION_LOOKUP = {
    'na': 'https://na.crave-cloud.com',
    'eu': 'https://eu.crave-cloud.com',
    'china': 'https://api.crave-cloud.cn'
}


def get_stores(base_url):
    store_response = requests.get(f'{base_url}/store', headers=HEADERS).json()
    valid_stores = []
    for store in store_response['data']:
        if 'group' in store and store['group'] != 'DEMO' and store['group'] != 'TEST':
            valid_stores.append({'storeId': store['id'],
                                 'externalId': store['externalId'],
                                 'storeName': store['name']})
    return valid_stores


"""
* This is a helper function that requests the data from a given api URL, the requests is
* the requests is fitted with params specifically storeId and startDate
* 
* @param url This is a URL for a specific API used
* @param next_cursor A cursor used to indicate the data being read 
* @return response This is the response got from the requests 
"""


def get_store_name(store):
    if store is not None:
        return store['storeName']
    return None


def get_response(url, store_id, next_cursor):
    response = requests.get(url,
                            headers=HEADERS,
                            params={'storeId': store_id,
                                    'startDate': START_DATE,
                                    'next': next_cursor}).json()
    return response


"""
* This is a helper function that parses the response into set data per store id
* specifically for shopper_items it creates a pure_uri and tag_uri
*
* @param store_id Store ID obtained from the first request API
* @param id Parameter to signify which different set of data is wanted
* @return shoppers A list that contains all the values fetched and parsed
"""


def has_more_fun(base_url, store_id, fun_name):
    has_more = True
    next_cursor = None
    values = []
    while has_more:
        BASE_URL = base_url + BASE_URL_LIST[fun_name]
        response = get_response(BASE_URL, store_id, next_cursor)
        if response['metadata']['code'] != 200:
            print(f'Error fetching shoppers. Error: {response["metadata"]["code"]} {response["metadata"]["message"]}')
            has_more = False
        else:
            enhanced_shopper_items = []
            if fun_name == 'item':
                for shopper_item in response['data']['values']:
                    try:
                        epc = SGTIN(shopper_item['epc'])
                        enhanced_shopper_items.append(shopper_item.update({'pure_uri': epc.epc_uri,
                                                                           'tag_uri': epc.tag_uri(
                                                                               SGTIN.BinaryCodingScheme.SGTIN_96,
                                                                               sgtin.SGTINFilterValue.POS_ITEM)}))
                    except Exception as e:  # Remove the try/except block when gs1/epc problem is fixed
                        pass

            if len(enhanced_shopper_items) != 0:
                values.extend(enhanced_shopper_items)
            else:
                values.extend(response['data']['values'])
            next_cursor = response['data']['next']
            has_more = response['data']['hasMore']
    return values


"""
* This is a helper function that will output the files on a csv file
* 
* @param req_dict Store ID obtained from the first request API
* @param id Parameter to signify which different set of data is wanted
"""


def to_csv(data_list, fun_name):
    keys = KEY_LIST[fun_name]
    file_name = FILE_NAME_LIST[fun_name]

    # initialize dataframe with all columns because null values don't appear in data when fetched
    df = pd.DataFrame(columns=keys)
    df = pd.concat([df, pd.DataFrame(data_list)])

    # convert floats to dtype Int64 (to handle nulls) to remove floating points
    for column in df.columns:
        print(column)
        if df[column].dtype == 'float64' and column != 'price':
            df[column] = df[column].astype('Int64')

    df[keys].to_csv(file_name, index=False)


"""
* This function calls the helper functions above to process the data regarding shoppers by store
* creates a csv file
*
"""


def fetch_shoppers(base_url, stores):
    print(f'Starting Shopper Export')
    all_shoppers = []
    for store in stores:
        print(f'Fetching shoppers for store {get_store_name(store)}')
        shoppers = has_more_fun(base_url, store['storeId'], 'shopper')
        all_shoppers.extend(shoppers)
    to_csv(all_shoppers, 'shopper')
    print('Shopper Export Done')


""" 
* This function calls the helper functions above to process the data regarding shoppers items by store
*
"""


def fetch_shopper_items(base_url, stores):
    print(f'Starting Shopper Item Export')
    all_shopper_items = []
    for store in stores:
        print(f'Fetching shopper_items for store {get_store_name(store)}')
        shopper_items = has_more_fun(base_url, store['storeId'], 'item')
        all_shopper_items.extend(shopper_items)
    to_csv(all_shopper_items, 'item')
    print(f'Shopper Item Export Done')


"""
* This function calls the helper functions above to process the data regarding requests by store
*
"""


def fetch_requests(base_url, stores):
    print(f'Starting Requests Export')
    all_requests = []
    for store in stores:
        print(f'Fetching requests for store {get_store_name(store)}')
        shopper_requests = has_more_fun(base_url, store['storeId'], 'requests')
        all_requests.extend(shopper_requests)
    to_csv(all_requests, 'requests')
    print(f'Requests Export Done')


"""
* This function calls the helper functions above to process the data regarding feedback by store
*
"""


def fetch_feedback(base_url, stores):
    print(f'Starting Feedback Export')
    all_feedback = []  # key = store_id, values = list of feedback
    for store in stores:
        print(f'Fetching feedback for store {get_store_name(store)}')
        feedback = has_more_fun(base_url, store['storeId'], 'feedback')
        all_feedback.extend(feedback)
    to_csv(all_feedback, 'feedback')
    print(f'Feedback Export Done')


def fetch_suspicious_activity_alerts(base_url, stores):
    print(f'Starting Suspicious Activity Alert Export')
    all_alerts = []
    for store in stores:
        print(f'Fetching suspicious activity alerts for store {get_store_name(store)}')
        alerts = has_more_fun(base_url, store['storeId'], 'suspicious_activity_alert')
        all_alerts.extend(alerts)
    to_csv(all_alerts, 'suspicious_activity_alert')
    print(f'Suspicious Activity Alert Export Done')


def fetch_suspicious_activities(base_url, stores):
    print(f'Starting Suspicious Activity Export')
    all_activities = []
    for store in stores:
        print(f'Fetching suspicious activities for store {get_store_name(store)}')
        activities = has_more_fun(base_url, store['storeId'], 'suspicious_activity')
        all_activities.extend(activities)
    to_csv(all_activities, 'suspicious_activity')
    print(f'Suspicious Activity Export Done')


def fetch_changing_rooms(base_url, stores):
    print(f'Starting Changing Room Export')
    all_rooms = []

    for store in stores:
        print(f'Fetching changing rooms for store {get_store_name(store)}')
        areas = requests.get(f'{base_url}/changingRoomGroup/store/{store["storeId"]}', headers=HEADERS).json()['data']
        for area in areas:
            for room in area['changingRooms']:
                room_view = {'storeId': store['storeId'],
                             'areaId': area['changingRoomGroup']['id'],
                             'roomId': room['id'],
                             'areaName': area['changingRoomGroup']['name'],
                             'roomName': room['name']}
                all_rooms.append(room_view)

    to_csv(all_rooms, 'room')
    print('Changing Room Export Done')


def fetch_users(base_url, stores):
    print(f'Starting User Export')
    all_users = []
    for store in stores:
        print(f'Fetching users for store {get_store_name(store)}')
        users = requests.get(f'{base_url}/user', headers=HEADERS, params={'storeId': store["storeId"]}).json()['data']
        all_users.extend(users)
    to_csv(all_users, 'user')
    print('User Export Done')


if __name__ == '__main__':
    selected_region = ''
    selected_stores = []

    if len(sys.argv) < 2:
        print('No region specified. Region must be one of: na, eu, or china')
    else:
        selected_region = sys.argv[1]
        if selected_region not in REGION_LOOKUP:
            print('Invalid region selected. Region must be one of: na, eu, or china')
            sys.exit(-1)
        else:
            print(f'Selected Region: {selected_region}')
            region_url = REGION_LOOKUP[selected_region]
            selected_stores = get_stores(region_url)
            to_csv(selected_stores, 'store')
            print(f'Fetching data for stores: {list(map(get_store_name, selected_stores))}')

            flag_first = True
            for i in range(2, len(sys.argv)):
                if flag_first:
                    try:
                        START_DATE = datetime.strptime(sys.argv[i], "%Y-%m-%d")
                        START_DATE = START_DATE.strftime("%Y-%m-%d")
                        if START_DATE > (date.today() - timedelta(1)).strftime('%Y-%m-%d'):
                            print('Invalid Date, please input a date at least yesterday or prior in format yyyy-MM-dd')
                            break
                        print(f'Date selected: {START_DATE}')
                        flag_first = False
                        continue
                    except ValueError:
                        if sys.argv[i] in OPTION_MENU:
                            print(f'Date not specified, defaulting start date to {START_DATE}')
                            flag_first = False
                        else:
                            msg = f"'{sys.argv[i]}' not a valid date, please use format yyyy-MM-dd"
                            print(msg)
                            flag_first = False
                            break
                if sys.argv[i] == 'shopper':
                    fetch_shoppers(region_url, selected_stores)
                elif sys.argv[i] == 'item':
                    fetch_shopper_items(region_url, selected_stores)
                elif sys.argv[i] == 'requests':
                    fetch_requests(region_url, selected_stores)
                elif sys.argv[i] == 'feedback':
                    fetch_feedback(region_url, selected_stores)
                elif sys.argv[i] == 'suspicious_activity_alert':
                    fetch_suspicious_activity_alerts(region_url, selected_stores)
                elif sys.argv[i] == 'suspicious_activity':
                    fetch_suspicious_activities(region_url, selected_stores)
                elif sys.argv[i] == 'room':
                    fetch_changing_rooms(region_url, selected_stores)
                elif sys.argv[i] == 'user':
                    fetch_users(region_url, selected_stores)
                else:
                    print('''
                    Not a valid argument
                    Valid arguments are: shopper, item, requests, feedback, suspicious_activity_alert, suspicious_activity, room, user
                    If you want to specify a date please enter the date first and then the requests''')
