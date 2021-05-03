import sqlite3
import time

from dlucheck import * # type: ignore

import pandas as pd     # type: ignore


if __name__ == '__main__':
    # con2 = sqlite3.connect('file:./example.db?mode=ro', uri=True)
    # con = sqlite3.connect('file:./example.db?mode=ro', uri=True)

    check_manager = CheckManager.load_from_json("checks.json")
    table = Table.load_from_json("common.json")
    # table.con = sqlite3.connect('/tmp/dlucheck_temp_table.db')
    table.from_excel_table('Fake_data.xlsx')

    print(check_manager.run(table))
