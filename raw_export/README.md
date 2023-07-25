# Raw Export
This script has the capability to retrieve all accessible raw store data and save it into CSV files.

## Setup
This script requires python 3

```pip install -r requirements.txt```

## Usage
This script has two inputs the start date and the data to fetch.

The Date must be in yyyy-mm-dd format. The available items to fetch are `shopper` `item` `requests` `feedback`
 
Sample Call
``` python export_to_csv.py 2023-07-01 shopper item requests feedback```