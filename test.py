import sqlite3

from dlucheck import (
    FieldFormat, TableFormat, Common, THandler,
    def_condition)

def search_by_words_all(words):
    # Here {0} stands for the field's name
    query = ' AND '.join(['{0} LIKE ?' for word in words])
    return (query, tuple([f'%{word}%' for word in words]))


if __name__ == '__main__':
    search_by_names = lambda full_name : search_by_words_all(full_name.split())
    # Read only connection
    con2 = sqlite3.connect('file:./example.db?mode=ro', uri=True)

    t1 = TableFormat("example1", [
        FieldFormat(["id"],       "int", def_condition),
        FieldFormat(["name"],     "str", search_by_names),
        FieldFormat(["date"],     "str", def_condition),
    ])

    t2 = TableFormat("example2", [
        FieldFormat(["id"],                   "int", def_condition),
        FieldFormat(["name"],                 "str", search_by_names),
        FieldFormat(["created_date", "date"], "int", def_condition),
    ])

    common = Common([
        ("id",   "int", 0.4),
        ("name", "str", 0.6),
        ("date", "str", 0.0),
    ])

    handler = THandler(t1, t2, common, None, con2)

    r1 = (1, 'John Wick', '2021-01-18 15:10:47')
    related_fields = handler.find_related_rows(r1)
    print(related_fields)
