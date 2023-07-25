# Requires epcpy module for EPC decoding - pip install epcpy
# Requires requests module for HTTP requests - pip install requests

import requests
from datetime import datetime, timedelta, date
import sys
import csv
from csv import writer
import pandas as pd

from epcpy.epc_schemes import SGTIN, sgtin

BASE_URL = 'https://eu.crave-cloud.com'
API_KEY = '##API_KEY_HERE##'
HEADERS = {'Authorization': f'api-token {API_KEY}',
           'Content-Type': 'application/json'}
START_DATE = (date.today() - timedelta(1)).strftime('%Y-%m-%d')  # Using yesterday's date, yyyy-MM-dd

STORE_RESPONSE = requests.get(f'{BASE_URL}/store', headers=HEADERS).json()  # Fetching the Store Response
STORE_IDS = [store['id'] for store in STORE_RESPONSE['data']]  # Store ID from the Store Response
STORE_IDS.remove(12)  # Remove demo store

BASE_URL_LIST = {'shopper': f'{BASE_URL}/v2/archive/shopper', 'item': f'{BASE_URL}/v2/archive/shopper_item',
                 'requests': f'{BASE_URL}/v2/archive/request',
                 'feedback': f'{BASE_URL}/v2/archive/feedback'}  # The different API URLS

KEY_LIST = {
    'shopper': ['id', 'name', 'storeId', 'createdAt', 'itemCount', 'deletedAt', 'dwellMilliseconds', 'shopperId',
                'type', 'associateId', 'changingRoomId'],
    'item': ['id', 'shopperArchiveId', 'sku', 'price', 'storeId', 'createdAt', 'itemId',
             'productId', 'serial', 'title', 'size', 'category', 'color', 'epc'],
    'requests': ['id', 'status', 'createdBy', 'storeId', 'changingRoomId', 'assignedUserId', 'sku', 'createdAt',
                 'assignedAt', 'completedAt', 'size', 'color', 'price', 'timeTaken', 'type', 'itemId', 'productId',
                 "originalRequestId", 'title', 'category'],
    'feedback': ['id', 'shopperName', 'rating', 'storeId', 'deviceRating', 'createdAt']}

O_FILE_NAME_LIST = {'shopper': 'shopper.csv',
                    'item': 'item.csv',
                    'requests': 'requests.csv',
                    'feedback': 'feedback.csv'}

DEL_KEY_LIST = {'shopper': ['phoneNumber', 'engaged'],
                'item': ['state'],
                'requests': [],
                'feedback': ['planningPurchase', 'email', 'dwellMilliseconds', 'message']}

OPTION_MENU = ['shopper', 'item', 'requests', 'feedback']

# Remove the following fields
# Shopper: phoneNumber, engaged
# Shopper Item: state
# Feedback: planningPurchase, email, dwellMilliseconds, message

"""
* This is a helper function that requests the data from a given api URL, the requests is
* the requests is fitted with params specifically storeid and startdate
* 
* @param url This is a URL for a specific API used
* @param next_cursor A cursor used to indicate the data being read 
* @return response This is the response got from the requests 
"""


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


def has_more_fun(store_id, fun_name):
    has_more = True
    next_cursor = None
    shoppers = []
    while has_more:
        response = get_response(BASE_URL_LIST[fun_name], store_id, next_cursor)
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
                        None
            if len(enhanced_shopper_items) != 0:
                shoppers.extend(enhanced_shopper_items)
            else:
                shoppers.extend(response['data']['values'])
            next_cursor = response['data']['next']
            has_more = response['data']['hasMore']
    return shoppers


"""
* This is a helper function that will output the files on a csv file
* 
* @param req_dict Store ID obtained from the first request API
* @param id Parameter to signify which different set of data is wanted
"""


def to_csv(req_dict, fun_name):
    keys = KEY_LIST[fun_name]
    del_keys = DEL_KEY_LIST[fun_name]
    o_file = O_FILE_NAME_LIST[fun_name]
    flag = False
    for k, v in req_dict.items():
        if len(v) == 0:
            continue
        for items in v:
            for col_name in keys:
                if not col_name in items.keys():
                    items[col_name] = None
            for col_name in del_keys:
                if col_name in items.keys():
                    del items[col_name]
            sorted_dict = {i: items[i] for i in keys}
            df = pd.DataFrame.from_dict([sorted_dict])
            if flag == False:
                flag = True
                df.to_csv(o_file, mode='w', header=True, index=False)
            else:
                df.to_csv(o_file, mode='a', header=False, index=False)


"""
* This function calls the helper functions above to process the data regarding shoppers by store
* creates a csv file
*
"""


def fetch_shopper():
    shoppers_by_store = {}  # key = store_id, values = list of shoppers
    for store_id in STORE_IDS:
        print("Fetching store", store_id)
        shoppers = has_more_fun(store_id, 'shopper')
        shoppers_by_store[store_id] = shoppers
    to_csv(shoppers_by_store, 'shopper')


""" 
* This function calls the helper functions above to process the data regarding shoppers items by store
*
"""


def fetch_shopper_item():
    shopper_items_by_store = {}  # key = store_id, values = list of shopper items
    for store_id in STORE_IDS:
        shopper_items = has_more_fun(store_id, 'item')
        shopper_items_by_store[store_id] = shopper_items
    to_csv(shopper_items_by_store, 'item')


"""
* This function calls the helper functions above to process the data regarding requests by store
*
"""


def fetch_archive_request():
    requests_by_store = {}  # key = store_id, values = list of requests
    for store_id in STORE_IDS:
        shopper_requests = has_more_fun(store_id, 'requests')
        requests_by_store[store_id] = shopper_requests
    to_csv(requests_by_store, 'requests')


"""
* This function calls the helper functions above to process the data regarding feedback by store
*
"""


def fetch_archive_feedback():
    feedback_by_store = {}  # key = store_id, values = list of feedback
    for store_id in STORE_IDS:
        feedback = has_more_fun(store_id, 'feedback')
        feedback_by_store[store_id] = feedback
    to_csv(feedback_by_store, 'feedback')


if __name__ == '__main__':
    flag_first = True
    if len(sys.argv) == 1:
        print("shopper done")
        print("item done")
        print("request done")
        print("feedback done")
    else:
        for i in range(1, len(sys.argv)):
            if flag_first == True:
                try:
                    START_DATE = datetime.strptime(sys.argv[i], "%Y-%m-%d")
                    START_DATE = START_DATE.strftime("%Y-%m-%d")
                    if (START_DATE > (date.today() - timedelta(1)).strftime('%Y-%m-%d')):
                        print("Invalid Date, please input a date atleast yesterday")
                        break
                    print("Date Specified:", START_DATE)
                    flag_first = False
                    continue
                except ValueError:
                    if sys.argv[i] in OPTION_MENU:
                        msg = "Date not specified"
                        print(msg)
                        flag_first = False
                    else:
                        msg = "not a valid date: {0!r}".format(sys.argv[i])
                        print(msg)
                        flag_first = False
                        break
            if sys.argv[i] == 'shopper':
                fetch_shopper()
                print("shopper done")
            elif sys.argv[i] == 'item':
                fetch_shopper_item()
                print("item done")
            elif sys.argv[i] == 'requests':
                fetch_archive_request()
                print("requests done")
            elif sys.argv[i] == 'feedback':
                fetch_archive_feedback()
                print("feedback done")
            else:
                print("Not a valid argument")
                print("Valid arguments are: shopper, item, requests, feedback")
                print("If you want to specify a date please enter the date first and then the requests")

