# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from collections import OrderedDict # AJ
from src.CSVDataTable import CSVDataTable
import logging
import os


# The logging level to use should be an environment variable, not hard coded.
logging.basicConfig(level=logging.DEBUG)

# Also, the 'name' of the logger to use should be an environment variable.
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# This should also be an environment variable.
# Also not the using '/' is OS dependent, and windows might need `\\`
data_dir = os.path.abspath("../Data/Baseball")


def t_load():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    csv_tbl = CSVDataTable("people", connect_info, None)

    print("Created table = " + str(csv_tbl))

def t_find_by_primary_key():

    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = list(range(19617))
    csv_tbl = CSVDataTable("people", connect_info, key_columns, debug=False)
    print(csv_tbl.find_by_primary_key(key_columns, [40, 52]))

def t_find_by_template():
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = list(range(19617))
    csv_tbl = CSVDataTable("people", connect_info, key_columns, debug=False)

    template = OrderedDict([('playerID', 'abbotda01'), ('birthYear', '1862'), ('birthMonth', '3'), ('birthDay', '16'), ('birthCountry', 'USA'), ('birthState', 'OH'), ('birthCity', 'Portage'), ('deathYear', '1930'), ('deathMonth', '2'), ('deathDay', '13'), ('deathCountry', 'USA'), ('deathState', 'MI'), ('deathCity', 'Ottawa Lake'), ('nameFirst', 'Dan'), ('nameLast', 'Abbott'), ('nameGiven', 'Leander Franklin'), ('weight', '190'), ('height', '71'), ('bats', 'R'), ('throws', 'R'), ('debut', '1890-04-19'), ('finalGame', '1890-05-23'), ('retroID', 'abbod101'), ('bbrefID', 'abbotda01')])
    field_list = ['nameFirst', 'nameLast', 'weight', 'height']

    print(csv_tbl.find_by_template(template, field_list=field_list))

def print_row(i):
    connect_info = {
        "directory": data_dir,
        "file_name": "People.csv"
    }

    key_columns = list(range(19617))
    csv_tbl = CSVDataTable("people", connect_info, key_columns, debug=False)

    print(csv_tbl.get_row(10))



#t_find_by_primary_key()
#t_load()
print_row(10)
#t_find_by_template()
