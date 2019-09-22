from src.RDBDataTable import RDBDataTable

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
        "password": "dbuserdbuser",
        "db": "lahman2019clean"
    }

    r_dbt = RDBDataTable("People", connect_info=c_info, key_columns=['playerID'])
    print("RDB table = ", r_dbt)

t1()