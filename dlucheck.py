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


class FInfo:
    def __init__(self, names: list[str], _type: str,
                 condition_fnc: Callable[[Any], tuple] = def_condition,
                 norm_fnc: Callable[[Any], Any] = lambda a : a,
                 denorm_fnc: Callable[[Any], Any] = lambda a : a):
        self.name = names[0]
        self.common_name = names[0] if len(names) == 1 else names[1]
        self._type = _type
        # TODO:
        # Change the way condition functions work
        self.condition_fnc = condition_fnc
        self.norm_fnc = norm_fnc
        self.denorm_fnc = denorm_fnc

    def __repr__(self):
        return f'FInfo({self.name}, {self.common_name}, {self._type})' 


class TInfo:
    def __init__(self, name: str, fields: list[FInfo]):
        self.name = name
        # Make field dict
        self.fields: dict[str, FInfo] = {}
        self.cnames: dict[str, str] = {}
        for field in fields:
            self.fields[field.name] = field
            self.cnames[field.common_name] = field.name

    def create_conditions(self, fields, row):
        field_names = list(self.fields.keys())
        condition_vals: list[tuple] = []
        for k in fields:
            f = self.fields[k]
            f2_index = field_names.index(f.name)
            # print(f2_index, f.name)
            condition_vals.append(f.condition_fnc(row[f2_index]))

        return condition_vals

    def __repr__(self):
        return f'TInfo({self.name}, {self.fields}, {self.cnames})' 

class Common:
    def __init__(self, fields: list[tuple]):
        self.fields: dict[str, tuple] = {}
        for field in fields:
            self.fields[field[0]] = field[1:]

    def __repr__(self):
        return f'Common({self.fields})' 


# Helper
# Check if fields are valid, find related fields and crosscheck
# In construction...
class THandler:
    def __init__(self, t1: TInfo, t2: TInfo, common: Common, con2):
        pass


def find_related_fields(t1: TInfo, t2: TInfo, common: Common, r1: tuple):
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
    condition_vals = t2.create_conditions(
        [t2.cnames[k] for k in valid_fields], t2_row
    )
    # condition_vals: list[tuple] = []
    # for cf_name in valid_fields:
    #     # F1 field
    #     f2 = t2.fields[t2.cnames[cf_name]]
    #     f2_index = f2_names.index(f2.name)
    #     condition_vals.append(f2.condition_fnc(t2_row[f2_index]))

    # Separate conditions and values into tuples
    conditions: tuple = ()
    values: tuple = ()
    for i, (c, v) in enumerate(condition_vals):
        fname = t2.cnames[valid_fields[i]]

        conditions += (c.format(fname),)
        values += v

    condition = ' OR '.join(conditions)

    columns = ', '.join(t2.fields.keys())
    query = f'SELECT rowid, {columns} FROM {t2.name} WHERE {condition}'

    return query
