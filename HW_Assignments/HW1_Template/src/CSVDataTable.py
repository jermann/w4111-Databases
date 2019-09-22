
from src.BaseDataTable import BaseDataTable
import copy
import csv
import logging
import json
import os
import pandas as pd

pd.set_option("display.width", 256)
pd.set_option('display.max_columns', 20)

class CSVDataTable(BaseDataTable):
    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    _rows_to_print = 10
    _no_of_separators = 2

    def __init__(self, table_name, connect_info, key_columns, debug=True, load=True, rows=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
            "debug": debug
        }

        self._logger = logging.getLogger()

        self._logger.debug("CSVDataTable.__init__: data = " + json.dumps(self._data, indent=2))

        if rows is not None:
            self._rows = copy.copy(rows)
        else:
            self._rows = []
            self._load()

    def __str__(self):

        result = "CSVDataTable: config data = \n" + json.dumps(self._data, indent=2)

        no_rows = len(self._rows)
        if no_rows <= CSVDataTable._rows_to_print:
            rows_to_print = self._rows[0:no_rows]
        else:
            temp_r = int(CSVDataTable._rows_to_print / 2)
            rows_to_print = self._rows[0:temp_r]
            keys = self._rows[0].keys()

            for i in range(0,CSVDataTable._no_of_separators):
                tmp_row = {}
                for k in keys:
                    tmp_row[k] = "***"
                rows_to_print.append(tmp_row)

            rows_to_print.extend(self._rows[int(-1*temp_r)-1:-1])

        df = pd.DataFrame(rows_to_print)
        result += "\nSome Rows: = \n" + str(df)

        return result

    def _add_row(self, r):
        if self._rows is None:
            self._rows = []
        self._rows.append(r)

    def _load(self):

        dir_info = self._data["connect_info"].get("directory")
        file_n = self._data["connect_info"].get("file_name")
        full_name = os.path.join(dir_info, file_n)

        with open(full_name, "r") as txt_file:
            csv_d_rdr = csv.DictReader(txt_file)
            for r in csv_d_rdr:
                self._add_row(r)

        self._logger.debug("CSVDataTable._load: Loaded " + str(len(self._rows)) + " rows")

    def save(self):
        """
        Write the information back to a file.
        :return: None
        """

    @staticmethod
    def matches_template(row, template):

        result = True
        if template is not None:
            for k, v in template.items():
                if v != row.get(k, None):
                    result = False
                    break

        return result

    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """

        assert key_fields != None, "Key field is None"
        assert key_fields != [], "Key field is an empty list"

        # key_fields will give the values for the key columns: so if key column played ID and year, then will be ('apj','2021')
        # key columns are data attribute, so use that!!!

        keys = self._data['key_columns']
        values = key_fields

        assert len(keys) == len(values), "key columns and key fields are not the same length"

        template = dict(zip(keys, values))
        tmp_dict = self.find_by_template(template, field_list=field_list)[0]

        return tmp_dict

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
        a = None

        for row in self.get_rows():
            if self.matches_template(row, template):
                if a == None:
                    a = list()
                if field_list == None:
                    a.append(row)
                else:
                    a.append(self.get_columns(row, field_list))
        return a


    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param template: A template.
        :return: A count of the rows deleted.
        """

        count = 0
        keys = self._data['key_columns']
        values = key_fields

        assert len(keys) == len(values), "key columns and key fields are not the same length"
        assert key_fields != None, "Key field is None"
        assert key_fields != [], "Key field is an empty list"

        template = dict(zip(keys, values))
        count += self.delete_by_template(template, by_key=True)

        return count

    def delete_by_template(self, template, by_key=False):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """

        count = 0

        for row in self.get_rows():
            if self.matches_template(row, template):
                count += self.delete_row(row)
                if by_key:
                    break

        return count

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """

        count = 0

        keys = self._data['key_columns']
        values = key_fields

        assert len(keys) == len(values), "key columns and key fields are not the same length"
        assert key_fields != None, "Key field is None"
        assert key_fields != [], "Key field is an empty list"

        template = dict(zip(keys, values))
        count += self.update_by_template(template, new_values, by_key=True)

        return count



    def update_by_template(self, template, new_values, by_key=False):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """

        count = 0

        for row in self.get_rows():
            if self.matches_template(row, template):
                self.delete_row(row) # AJ inefficient because going through list twice? Can directly delete by
                self._add_row(new_values)
                count += 1
                if by_key:
                    break

        return count


    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        # AJ: verify that similar
        self._add_row(new_record)

        return None

    def get_rows(self):
        return self._rows

    # AJ
    def get_row(self, index):
        return self._rows[index]

    # AJ
    def delete_row_index(self, index):
        deleted = self._rows.pop(index)
        return deleted

    # AJ
    def delete_row(self, row):
        self._rows.remove(row)
        return 1

    # AJ
    def get_columns(self, row, col_list):
        result = {}
        for c in col_list:
            result[c] = row[c]
        return result

