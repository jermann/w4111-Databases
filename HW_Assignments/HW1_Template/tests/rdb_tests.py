from src.RDBDataTable import RDBDataTable
import pymysql

import src.SQLHelper as dbutils

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# AJ: Change this
def t1():
    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "almamater2021",
        "db": "lahman2019"
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print("RDB table = ", r_dbt)

def t_find_by_key():

    # modified cursorclass

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "almamater2021",
        "db": "lahman2019",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print(r_dbt.find_by_primary_key(key_fields=['willite01'], field_list=['nameFirst', 'nameLast']))
    #r_dbt.find_by_primary_key(key_fields=['loosers'])
    #print("RDB table = ", r_dbt)

def t_find_by_template():

    # modified cursorclass

    c_info = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "almamater2021",
        "db": "lahman2019",
        "cursorclass": pymysql.cursors.DictCursor
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print(r_dbt.find_by_template(template={'nameFirst':'Ted'}, field_list=['nameFirst', 'nameLast']))

def t_create_delete():
    table_name = 'hello'
    template = {"field1": 'value1', "field2": 'value2'}
    dbutils.create_delete(table_name, template)

#t1()
#t_find_by_key()
t_find_by_template()
#t_create_delete()