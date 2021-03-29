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


SQLITE3_Types: dict[str, str] = {
    "int": "INTEGER",
    "str": "TEXT",
    "float": "real",
    "double": "real",
}


def def_condition(fval: Any) -> tuple[str, tuple]:
    return ('{0} = ?', (fval, ))


class FieldFormat:
    """
    Attributes
    ----------
    name : str
        Field name

    common_name : str
        Field's common name

    _type : str
        Field's type

    query_fnc : function
        Function that takes a field's value as argument and
        returns a tuple with a query and said query values.

        Example
        -------
        # Return a query to search for rows which contains all words in `words`
        def search_by_words_all(words):
            # Here {0} stands for the field's name
            query = ' AND '.join(['{0} LIKE ?' for word in words])
            return (query, tuple([f'%{word}%' for word in words]))

    norm_fnc : function
        Function that takes a field's value and converts it
        from it's format to the common format.

    denorm_fnc : function
        Functino that takes field's value in the common format
        and converts it to this format
    """
    def __init__(self, names: list[str], _type: str,
                 query_fnc: Callable[[Any], tuple] = def_condition,
                 norm_fnc: Callable[[Any], Any] = lambda a : a,
                 denorm_fnc: Callable[[Any], Any] = lambda a : a):
        self.name = names[0]
        self.common_name = names[0] if len(names) == 1 else names[1]
        self._type = _type
        self.query_fnc = query_fnc
        self.norm_fnc = norm_fnc
        self.denorm_fnc = denorm_fnc

    def __repr__(self):
        return f'FieldFormat({self.name}, {self.common_name}, {self._type})' 


class TableFormat:
    def __init__(self, name: str, fields: list[FieldFormat]):
        self.name = name
        # Make field dict
        self.fields: dict[str, FieldFormat] = {}
        self.cnames: dict[str, str] = {}
        for field in fields:
            self.fields[field.name] = field
            self.cnames[field.common_name] = field.name

    def create_queries(self, fields, row):
        field_names = list(self.fields.keys())
        query_vals: list[tuple] = []
        for k in fields:
            f = self.fields[k]
            f2_index = field_names.index(f.name)
            # print(f2_index, f.name)
            query_vals.append(f.query_fnc(row[f2_index]))

        return query_vals

    def __repr__(self):
        return f'TableFormat({self.name}, {self.fields}, {self.cnames})' 

class Common:
    def __init__(self, fields: list[tuple]):
        self.fields: dict[str, tuple] = {}
        for field in fields:
            self.fields[field[0]] = field[1:]

    def __repr__(self):
        return f'Common({self.fields})' 


"""
Create a query to search the `r1` row from the `t1` table inside the `t2` table.

Parameters
----------
t1 : TableFormat
    `r1` table's info
t2 : TableFormat
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
def create_related_rows_query(t1: TableFormat, t2: TableFormat,
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


"""Transform table from the `table` TableFormat to common format
"""
def normalize_table(table: TableFormat, common: Common, con):
    pass


# Helper function, links tables with database connections
class THandler:
    import sqlite3

    def __init__(self, t1: TableFormat, t2: TableFormat, common: Common, con1, con2):
        self.t1 = t1
        self.t2 = t2
        self.common = common
        self.cursor1 = con1.cursor() if con1 is not None else None
        self.cursor2 = con2.cursor() if con2 is not None else None

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
    def find_related_rows(self, row: tuple, table: int = 0):
        t1 = self.t1 if table == 0 else self.t2
        t2 = self.t2 if table == 0 else self.t1
        cursor = self.cursor2 if table == 0 else self.cursor1

        query, values = create_related_rows_query(t1, t2,
                                                  self.common, row)
    
        return [r for r in cursor.execute(query, values)]

    """
    Search for duplicates
    """
    def find_duplicates(self, row: tuple, table: int = 0):
        pass

    """
    Run a user define function(Machine Learning if necessary) for each
    normalized pair of values in table1 and table2.
    """
    def crosscheck(self):
        pass

