from typing import Callable, Any
import pandas as pd # type: ignore
import numpy as np
import json


# TODO:
# Make this configurable per Field or Table
# Default python types to numpy types
PTNDType: dict[str, str] = {
    "int": 'i8',    # 64-bit integer
    # "str": 'U256',  # By default strings will have a 512 character limit
    "str": 'U256',  # By default strings will have a 512 character limit
    "float": 'f8'   # 64-bit floating point
}


SQL_to_py_types: dict[str, str] = {
    "text": "str",
    "real": "float",
    "integer": "int",
    "null": "None",
}


def def_query_fnc(fval: Any) -> tuple[str, tuple]:
    return ('{0} = ?', (fval, ))


def load_json_file(json_path: str) -> dict:
    json_src = ''
    with open(json_path, 'r') as f:
        json_src = f.read()

    return json.loads(json_src)

"""
Contains Table information

Attributes
----------

name : str
    Table's name
fields : dict[str, dict]
    dictionary with format -
        <field name>: {
            "type"          - sql type of the field(e.g., text, integer, real)
            "is_pickfield"  - whether a field is pickfield or not
            "choices"       - all the possible values that the field can have,
                              only applies if the field is a pickfield.
        }

con : sqlite3.Connection, optional
    Connection to the database in question; for security reasons this connection
    should always be readonly.
"""
class Table:
    @staticmethod
    def load_from_json(json_path):
        table_info = load_json_file(json_path)

        table_name = list(table_info.keys())[0]
        fields_info = table_info[table_name]

        return Table(table_name, fields_info)

    def __init__(self, name: str, fields: dict[str, dict], con=None):
        # TODO:
        # Check if table named `name` exists in the `con` database
        self.name = name
        self.con = con

        # TODO:
        # Check if fields are in the `name` table
        self.fields: dict[str, dict] = fields

        self.dtype = np.dtype([(name,
                                PTNDType[SQL_to_py_types[f["type"]]])
                               for name, f in self.fields.items()])

    def fetch_num_rows(self, use_rowid: bool = True):
        # Fast query but requires rowid
        f_query = f"SELECT COALESCE(MAX(rowid), 0) FROM {self.name}" 
        # Slow query but doesn't require rowid
        s_query = f"SELECT Count(*) FROM {self.name}"

        query = f_query if use_rowid else s_query
        cur = self.con.cursor()
        cur.execute(query)

        return cur.fetchone()

    def from_excel_table(self, path: str, engine='openpyxl'):
        import sqlite3
        import pandas as pd     # type: ignore
        import os

        # TEMP
        db_path = '/tmp/dlucheck_temp_table.db' 

        # TODO:
        # cache table
        # Remove table if already exists
        try:
            os.remove(db_path)
        except OSError:
            pass

        self.con = sqlite3.connect(db_path)

        sql_dtypes = {name: finfo["type"]
                  for name, finfo in self.fields.items()} # type: ignore

        dtypes = {n: t.str for n, (t, s) in self.dtype.fields.items()} # type: ignore

        # names = list(self.fields.keys())
        # TODO:
        # Try reading excel in chunks if possible
        data_frame = pd.read_excel(path, dtype=dtypes, engine=engine)

        data_frame.to_sql(self.name, self.con, index=False,
                          dtype=sql_dtypes, method='multi')

    def iterate(self):
        cur = self.con.cursor()

        fields = ', '.join([f'"{f}"' for f in self.fields.keys()])
        query = f'SELECT {fields} FROM {self.name}'
        for i, row in enumerate(cur.execute(query)):
            yield (i, row)


    # TODO:
    # - Add a way to define a limit
    # - Use numpy.mmemap for big arrays
    def to_numpy_array(self) -> np.ndarray:
        num_rows = self.fetch_num_rows() 
        if num_rows is None:
            print(f"Failed to fetch `{self.name}` table's number of rows")
            return np.array([])

        arr = np.empty(num_rows, dtype=self.dtype)
        for i, row in self.iterate():
            arr[i] = row

        return arr;

    def __repr__(self):
        return f'Table({self.name}, {self.fields}' 



def array_to_excel(array: np.ndarray, excel_path: str):
    pd.DataFrame(array).to_excel(excel_path, index=False)

