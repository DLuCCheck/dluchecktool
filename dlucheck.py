"""
Library for table serialization && crosschecking
"""
# NOTE:
# Varius optimizations could be made to make this code faster.
# rewriting this code in more performant language like
# C++, D or rust would be advisable
import re
import numpy as np
import time
from typing import Any, Callable
from dataclasses import dataclass
from functools import reduce
import sqlite3

from table import Field, Common, Table


SQLITE3_Types: dict[str, str] = {
    "int": "INTEGER",
    "str": "TEXT",
    "float": "real",
    "double": "real",
}


"""
Create a query to search the `r1` row from the `t1` table inside the `t2` table.

Parameters
----------
t1 : Table
    `r1` table's info
t2 : Table
    Table to query for similar rows to `r1`
common : Common
    common format
r1 : tuple
    Row to query for

Returns
-------
str
    Query and values to search for the `r1` row
"""
def create_related_rows_query(t1: Table, t2: Table,
                              common: Common, r1: tuple):
    f1_names = list(t1.fields.keys())
    f2_names = list(t1.fields.keys())
    cfield_names =  list(common.fields.keys())
    valid_fields = [k for k in cfield_names if common.fields[k][1] > 0]

    # convert rows from format 1 to common format
    # and then from common to format 2
    common_row: tuple = ()
    t2_row: tuple = ()
    for i, k in enumerate(f1_names):
        f1_i = t1.fields[k]
        f2_i = t2.fields[t2.cnames[f1_i.common_name]]

        # Convert from f1 to common format
        c_f = f1_i.norm_fnc(r1[i])
        # Convert from common to f2 format
        t2_f = f2_i.denorm_fnc(c_f)
        common_row += (c_f,)
        t2_row += (t2_f,)

    # List of conditions and values
    query_vals = t2.create_queries(
        [t2.cnames[k] for k in valid_fields], t2_row
    )

    # Separate conditions and values into tuples
    conditions: tuple = ()
    values: tuple = ()
    for i, (c, v) in enumerate(query_vals):
        fname = t2.cnames[valid_fields[i]]

        conditions += (c.format(fname),)
        values += v

    condition = ' OR '.join(conditions)

    columns = ', '.join(t2.fields.keys())
    query = f'SELECT rowid, {columns} FROM {t2.name} WHERE {condition}'

    return query, values

"""
Find rows similar to `row` in table

Parameters
----------
row : tuple
    Row to find similar fields with
table : int
    The number of the `row`'s table. This can be either 0 or 1
    which will match with self.t1 and self.t2 respectively

Returns
-------
List
    List of similar rows to `row` in the `table` table
"""
def find_related_rows(t1: Table, t2: Table, common: Common, row: tuple):
    cursor = t2.con.cursor();

    query, values = create_related_rows_query(t1, t2,
                                              common, row)

    return [r for r in cursor.execute(query, values)]


def array_find_similar(array: np.ndarray, row, common: Common, similarity: float):
    candidates: list[np.void] = [cand for cand in array
                                 if common.check_fnc(cand, row) >= similarity] 
    return candidates


"""Normalize tables into a `common` format, save them into numpy arrays
provide functions to crosscheck, and search for duplicates and incongruencies
"""
class TConnection:
    def __init__(self, tables: list[Table], common: Common):
        self.tables = tables
        # TODO: change to numpy.memmap
        self.normalized: list[np.ndarray] = []
        self.common = common

    """Search for duplicates
    """
    def find_duplicates(self, row: tuple):
        pass

    """
    Run a user define function(Machine Learning if necessary) for each
    normalized pair of values in table1 and table2.
    """
    def crosscheck(self):
        pass

