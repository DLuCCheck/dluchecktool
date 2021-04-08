import sqlite3
import time

# from dlucheck import (
#     FieldFormat, TableFormat, Common, THandler,
#     def_condition)
from dlucheck import find_related_rows, array_find_similar
from table import *
from common_functions import epoch_to_date, search_by_words_all, fuzz_compare


if __name__ == '__main__':
    search_by_names = lambda full_name : search_by_words_all(full_name.split())
    # # Read only connection
    # con2 = sqlite3.connect('file:./example.db?mode=ro', uri=True)
    con = sqlite3.connect('file:./example.db?mode=ro', uri=True)

    # t1 = Table("example1", [
    #     Field(["id"],       "int"),
    #     Field(["name"],     "str", search_by_names),
    #     Field(["date"],     "str"),
    # ], con)

    # t2 = Table("example2", [
    #     Field(["id"],                   "int"),
    #     Field(["name"],                 "str", search_by_names),
    #     Field(["created_date", "date"], "int"),
    # ], con)

    # common = Common([
    #     ("id",   "int", 0.0, ),
    #     ("name", "str", 1.0, fuzz_compare),
    #     ("date", "str", 0.0, ),
    # ])

    # r1 = (1, 'John Wick', '2021-01-18 15:10:47')
    # related_fields = find_related_rows(t1, t2, common, r1)
    # print(related_fields)

    table = Table("example2", [
        Field(["id"],                   "int", def_query_fnc),
        Field(["name"],                 "str", search_by_names),
        Field(["created_date", "date"], "int", def_query_fnc, epoch_to_date),
    ], con)

    common = Common([
        ("id",   "int", 0.0),
        ("name", "str", 1.0, fuzz_compare),
        ("date", "str", 0.0),
    ])

    table_array = table.to_numpy_array()
    row = (0, 'John Wick', 1616074247)
    print(array_find_similar(table_array, row, common, 1.0))
