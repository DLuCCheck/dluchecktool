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


# NOTE:
# Converting table 1 fields to table 2 format is now a pipe dream.
# It's much better to convert everything to a single format and back.
@dataclass
class FieldInfo:
    alt_name: str
    common_name: str
    type_name: str
    # Return condtion, and values
    # Here {0} stands for the name of the field.
    # Make sure that the query doesn't use any user input
    # as it could result in an injection attack(fvar could be user input)
    condition_fnc: Callable[[Any], tuple] = lambda fval : ('{0} = ?', (fval, ))
    # Convert value from format a to b, this function is called before queries
    preprocess_fnc: Callable[[Any], Any] = lambda a : a
    init_val: Any = None
    init_fnc: 'function' = lambda a : a


class FInfo:
    def __init__(self, names: list[str], _type: str,
                 condition_fnc: Callable[[Any], tuple] = def_condition,
                 norm_fnc: Callable[[Any], Any] = lambda a : a,
                 denorm_fnc: Callable[[Any], Any] = lambda a : a):
        self.name = names[0]
        self.common_name = names[0] if len(names) == 1 else names[1]
        self._type = _type
        self.condition_fnc = condition_fnc
        self.norm_fnc = norm_fnc
        self.denorm_fnc = denorm_fnc

    def __repr__(self):
        return f'FInfo({self.name}, {self.common_name}, {self._type})' 


class TInfo:
    def __init__(self, name: str, fields: list[FInfo]):
        self.name = name
        # Make field dict
        self.fields = {}
        for field in fields:
            self.fields[field.name] = field

    def __repr__(self):
        return f'TInfo({self.name}, {self.fields})' 

class Common:
    def __init__(self, fields: list[tuple]):
        self.fields: dict[str, tuple] = {}
        for field in fields:
            self.fields[field[0]] = field[1:]

    def __repr__(self):
        return f'Common({self.fields})' 


@dataclass
class TabInfo:
    name: str
    fields: dict[str, FieldInfo]


class RTInfo:
    def __init__(self, tab1: TabInfo, tab2: TabInfo,
                 common: dict[str, tuple[str, float]]):
        self.tab1 = tab1
        self.tab2 = tab2
        self.common = common

    def __repr__(self):
        return f'RTInfo(%s, %s, %s)' % (str(self.tab1), str(self.tab2), self.common)


# TODO:
# check if tables exist
# check if everyone of the fields exist in table, prevent injection
class QHandler:
    def __init__(self, rt: RTInfo,
                 read_con: sqlite3.Connection):
        self.rt = rt
        self.cur = read_con.cursor()
        self.results = np.array([])

    # Find rows similar to r1 of t1 table inside the t2 table.
    # t1  - r1's table
    # t2  - Table to find related fields.
    # threshold - How similar have two fields to be.
    def find_related_rows(self,
                          t1: TabInfo, t2: TabInfo, r1: tuple, threshold: float):
        common = self.rt.common
        # fields: dict[str, tuple] = {}
        field_keys = list(t1.fields.keys())
        field_vals = list(t1.fields.values())
        valid_fields = [k for k in field_keys if common[k][1] > 0]

        # Normalize row
        nrow = tuple([t1.fields[k].preprocess_fnc(r1[i + 1])
                      for i, k in enumerate(field_keys)])

        # Generate list of tuples with condtions and values
        condition_vals = [t1.fields[f].condition_fnc(nrow[field_keys.index(f)])
                          for f in valid_fields]

        conditions: tuple = ()
        values: tuple = ()
        for i, (c, v) in enumerate(condition_vals):
            # format condition with the fields name
            f = t1.fields[valid_fields[i]].alt_name
            conditions += (c.format(f),)
            values += v

        condition = ' OR '.join(conditions)

        # Create query
        columns = ', '.join(t2.fields.keys())
        query = f'SELECT rowid, {columns} FROM {t2.name} WHERE {condition}'

        related_fields: list[tuple] = []
        # Iterate through cadidates
        for row in self.cur.execute(query, values):
            related_fields.append(row)

        return related_fields

    def get_first_row(self) -> Any:
        tab1, tab2 = self.rt.tab1, self.rt.tab2
        fields1 = ', '.join(tab1.fields.keys())

        iter1 = self.cur.execute(f'SELECT rowid, {fields1} FROM {tab1.name}')
        for row in iter1:
            return row


def def_name_condition(fval: str) -> tuple[str, tuple]:
    # Assuming name has at least a first name and a surname
    condition = '{0} LIKE ? AND {0} LIKE ?'
    names = fval.split()
    return (condition, (f'%{names[0]}%', f'%{names[1]}%'))

# Convert date to time in epoch
def date_to_epoch(fval: str):
    time_tuple = time.strptime(fval, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(time_tuple))


if __name__ == '__main__':
    id_to_tab2 = lambda fval : int(fval.replace('customer_', ''))

    # Read only connection
    rcon = sqlite3.connect('file:./example.db?mode=ro', uri=True)

    t1_info = TabInfo("example1", {
        "id":    FieldInfo("id", "id",              "str", def_condition, id_to_tab2),
        "name":  FieldInfo("name", "name",          "str", def_name_condition),
        "date":  FieldInfo("created_date", "date",  "str", def_condition, date_to_epoch)
    })

    t2_info = TabInfo("example2", {
        "id":            FieldInfo("id",   "id",   "int", def_condition),
        "name":          FieldInfo("name", "name", "str", def_name_condition),
        "created_date":  FieldInfo("date", "date", "int", def_condition)
    })
    """
    t1_info = TInfo("table_name", [
        FInfo("id",             "int",  def_condition),
        FInfo("name",           "int",  def_condition),
        FInfo("created_date",   "int",  def_condition)
    ])

    t2_info = TInfo("table_name2", [
        FInfo("id",             "str",  def_condition),
        FInfo("name",           "str",  def_condition),
        FInfo("date",           "str",  def_condition),
    ])

    common = Common([
        ("id",      "int", 0),
        ("name",    "str", 0),
        ("date",    "str", 0),
    ])
    """

    mt1_info = TInfo("table_name", [
        FInfo(["id"],                   "int",  def_condition),
        FInfo(["name"],                 "str",  def_condition),
        FInfo(["created_date", "date"], "int",  def_condition)
    ])
    common = Common([
        ("id",      "int", 0),
        ("name",    "str", 0),
        ("date",    "str", 0),
    ])
    print(mt1_info)
    print(common)

    # rt = RTInfo(
    #     t1_info,
    #     t2_info,
    #     {
    #         "id":    ("int", 0.0),
    #         "name":  ("str", 0.6),
    #         "date":  ("str", 0.4)
    #     }
    # )

    # rtable = QHandler(rt, rcon)
    # row = rtable.get_first_row()
    # print(rtable.find_related_rows(rt.tab1, rt.tab2, row, 0.7))
    # rtable.check()
    rcon.close()
