import sqlite3

from dlucheck import (
    FInfo, TInfo, Common, THandler,
    def_condition)


if __name__ == '__main__':
    # Read only connection
    con2 = sqlite3.connect('file:./example.db?mode=ro', uri=True)

    t1 = TInfo("example1", [
        FInfo(["id"],                   "int", def_condition),
        FInfo(["name"],                 "str", def_condition),
        FInfo(["created_date", "date"], "str", def_condition),
    ])

    t2 = TInfo("example1", [
        FInfo(["id"],   "int", def_condition),
        FInfo(["name"], "str", def_condition),
        FInfo(["date"], "int", def_condition),
    ])

    common = Common([
        ("id",   "int", 1),
        ("name", "str", 1),
        ("date", "str", 0),
    ])

    handler = THandler(t1, t2, common, None, con2)

    r1 = (1, 'John Wick', '2021-01-18 15:10:47')
    related_fields = handler.find_related_rows(r1)
    print(related_fields)
