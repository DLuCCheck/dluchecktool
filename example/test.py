import sqlite3
import time

# from dlucheck import (
#     FieldFormat, TableFormat, Common, THandler,
#     def_condition)
from dlucheck import find_related_rows, array_find_similar, array_find_duplicates
from table import *
from common_functions import epoch_to_date, search_by_words_all, fuzz_compare

import pandas as pd     # type: ignore


if __name__ == '__main__':
    search_by_names = lambda full_name : search_by_words_all(full_name.split())
    # # Read only connection
    # con2 = sqlite3.connect('file:./example.db?mode=ro', uri=True)
    # con = sqlite3.connect('file:./example.db?mode=ro', uri=True)
    # table, common = load_from_json
    # SDH_register("fuzz compare", fuzz_compare)

    table = Table("example2", [
        Field(["Name"],                     "str"),
        Field(["Surname"],                  "str"),
        Field(["Personal code"],            "str"),
        Field(["Address"],                  "str"),
        Field(["Bank account residence"],   "str"),
        Field(["Bank"],                     "str"),
        Field(["Account type"],             "str"),
    ])

    common = Common([
        ("Name",                    "str", 0.4, fuzz_compare),
        ("Surname",                 "str", 0.4, fuzz_compare),
        ("Personal code",           "str", 0.1),
        ("Address",                 "str", 0.1, fuzz_compare),
        ("Bank account residence",  "str", 0.0),
        ("Bank",                    "str", 0.0),
        ("Account type",            "str", 0.0),
    ])

    table_array = table.from_excel_table('Fake_data.xlsx')

    # TODO: Add a way to check for a specific field
    row = ('Jānis', 'Ozols', '100100-21500', 'Upes iela 4a, Valmiera, LV-4201', None, None, None)

    similar = find_similar(table_array, row, common, 0.9)
    print(f'rows similar to {row}:')
    print(pd.DataFrame(similar))
    print("#" * 64)
    print()

    similar_all = find_all_similar(table_array, common, 1.0)
    print("Rows similar to any other rows")
    print(similar_all)
    print("#" * 32)
    print()

    duplicate = find_duplicates(table_array, row, common)
    print(f"duplicates of {row}")
    print(pd.DataFrame(duplicate))
    print("#" * 32)
    print()

    array_to_table(table_array, "res_array.xlsx")
