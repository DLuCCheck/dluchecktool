from dlucheck import (
    FInfo, TInfo, Common,
    def_condition, find_related_fields)


if __name__ == '__main__':
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

    r1 = (0, 'Victor Fernandez', '2021-01-18 15:10:47')
    print(find_related_fields(t1, t2, common, r1))
