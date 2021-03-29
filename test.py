import sqlite3

from dlucheck import (
    FInfo, TInfo, Common, THandler,
    def_condition)

def search_by_words(full_name):
    names = full_name.split()
    # Here {0} stands for the field's name
    query = ' AND '.join(['{0} LIKE ?' for name in names])
    return (query, tuple([f'%{name}%' for name in names]))

if __name__ == '__main__':
    # Read only connection
    con2 = sqlite3.connect('file:./example.db?mode=ro', uri=True)

    t1 = TInfo("example1", [
        FInfo(["id"],       "int", def_condition),
        FInfo(["name"],     "str", search_by_words),
        FInfo(["date"],     "str", def_condition),
    ])

    t2 = TInfo("example2", [
        FInfo(["id"],                   "int", def_condition),
        FInfo(["name"],                 "str", search_by_words),
        FInfo(["created_date", "date"], "int", def_condition),
    ])

    common = Common([
        ("id",   "int", 0.0),
        ("name", "str", 1.0),
        ("date", "str", 0.0),
    ])

    handler = THandler(t1, t2, common, None, con2)

    r1 = (1, 'John Wick', '2021-01-18 15:10:47')
    related_fields = handler.find_related_rows(r1)
    print(related_fields)
