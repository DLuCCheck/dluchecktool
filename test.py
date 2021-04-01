import sqlite3
import time

# from dlucheck import (
#     FieldFormat, TableFormat, Common, THandler,
#     def_condition)
from dlucheck import THandler, def_condition
from table import *
from common_functions import epoch_to_date, search_by_words_all


if __name__ == '__main__':
    search_by_names = lambda full_name : search_by_words_all(full_name.split())
    # # Read only connection
    # con2 = sqlite3.connect('file:./example.db?mode=ro', uri=True)
    con = sqlite3.connect('file:./example.db?mode=ro', uri=True)

    t1 = Table("example1", [
        Field(["id"],       "int", def_condition),
        Field(["name"],     "str", search_by_names),
        Field(["date"],     "str", def_condition),
    ], con)

    t2 = Table("example2", [
        Field(["id"],                   "int", def_condition),
        Field(["name"],                 "str", search_by_names),
        Field(["created_date", "date"], "int", def_condition),
    ], con)

    common = Common([
        ("id",   "int", 0.4),
        ("name", "str", 0.6),
        ("date", "str", 0.0),
    ])

    handler = THandler(t1, t2, common, None, con)

    r1 = (1, 'John Wick', '2021-01-18 15:10:47')
    related_fields = handler.find_related_rows(r1)
    print(related_fields)

    # table = Table("example2", [
    #     Field(["id"],                   "int", def_query_fnc),
    #     Field(["name"],                 "str", search_by_names),
    #     Field(["created_date", "date"], "int", def_query_fnc, epoch_to_date),
    # ], con2)

    # common = Common([
    #     ("id",   "int", 0.4),
    #     ("name", "str", 0.6),
    #     ("date", "str", 0.0),
    # ])

    # print(table.to_normalized_numpy_array(common))
