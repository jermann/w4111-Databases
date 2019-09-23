from src.BaseDataTable import BaseDataTable
#import src.dbutils as dbutils
import src.SQLHelper as dbutils
import json
import pandas as pd
import pymysql

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        if table_name is None or connect_info is None:
            raise ValueError("Invalid input.")

        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns
        }

        cnx = dbutils.get_connection(connect_info)
        if cnx is not None:
            self._cnx = cnx
        else:
            raise Exception("Could not get a connection.")

    def __str__(self):

        result = "RDBDataTable:\n"
        result += json.dumps(self._data, indent=2)

        row_count = self.get_row_count()
        result += "\nNumber of rows = " + str(row_count)

        some_rows = pd.read_sql(
            "select * from " + self._data["table_name"] + " limit 10",
            con=self._cnx
        )
        result += "First 10 rows = \n"
        result += str(some_rows)

        return result

    def get_row_count(self):

        row_count = self._data.get("row_count", None)
        if row_count is None:
            sql = "select count(*) as count from " + self._data["table_name"]
            res, d = dbutils.run_q(sql, args=None, fetch=True, conn=self._cnx, commit=True)
            row_count = d[0][0]
            self._data['"row_count'] = row_count

        return row_count

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        tmp = None

        # Var Declarations
        table_name = self._data['table_name']
        keys = self._data['key_columns']
        values = key_fields
        template = dict(zip(keys, values))

        # Form SQL Command
        sql, args = dbutils.create_select(table_name=table_name, template=template, fields=field_list)

        # Run SQL Command
        res, data = dbutils.run_q(sql=sql, args=args, fetch=True, conn=self._cnx) # AJ: conn OR self.data connect info

        if res != 0:
            tmp = data[0]

        return tmp

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """

        a = list()

        table_name = self._data['table_name']

        sql, args = dbutils.create_select(table_name=table_name, template=template, fields=field_list)
        res, data = dbutils.run_q(sql=sql, args=args, fetch=True, conn=self._cnx)

        return data

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """

        table_name = self._data['table_name']
        keys = self._data['key_columns']
        values = key_fields
        template = dict(zip(keys, values))

        sql, args = dbutils.create_delete(table_name=table_name, template=template)
        res, data = dbutils.run_q(sql=sql, args=args, fetch=True, conn=self._cnx)

        # AJ: is res = count?
        return res

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        table_name = self._data['table_name']

        sql, args = dbutils.create_delete(table_name=table_name, template=template)
        res, data = dbutils.run_q(sql=sql, args=args, fetch=True, conn=self._cnx)

        return res

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        table_name = self._data['table_name']
        keys = self._data['key_columns']
        values = key_fields
        template = dict(zip(keys, values))

        sql, args = dbutils.create_update(table_name=table_name, new_values=new_values, template=template)
        res, data = dbutils.run_q(sql=sql, args=args, fetch=True, conn=self._cnx)

        return res


    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        table_name = self._data['table_name']

        sql, args = dbutils.create_update(table_name=table_name, new_values=new_values, template=template)
        res, data = dbutils.run_q(sql=sql, args=args, fetch=True, conn=self._cnx)

        return res

    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """

        table_name = self._data['table_name']
        sql, args = dbutils.create_insert(table_name=table_name, row=new_record)
        dbutils.run_q(sql=sql, args=args, fetch=True, conn=self._cnx)

    def get_rows(self):
        return self._rows


