from typing import Callable, Any
import numpy as np


# TODO:
# Make this configurable per Field or Table
# Default python types to numpy types
PTNDType: dict[str, str] = {
    "int": 'i8',    # 64-bit integer
    "str": 'U512',  # By default strings will have a 512 character limit
    "float": 'f8'   # 64-bit floating point
}

def def_query_fnc(fval: Any) -> tuple[str, tuple]:
    return ('{0} = ?', (fval, ))


class Field:
    """
    Attributes
    ----------
    name : str
        Field name

    common_name : str
        Field's common name

    _type : str
        Field's type

    query_fnc : function
        Function that takes a field's value as argument and
        returns a tuple with a query and said query values.

        Example
        -------
        # Return a query to search for rows which contains all words in `words`
        def search_by_words_all(words):
            # Here {0} stands for the field's name
            query = ' AND '.join(['{0} LIKE ?' for word in words])
            return (query, tuple([f'%{word}%' for word in words]))

    norm_fnc : function
        Function that takes a field's value and converts it
        from it's format to the common format.

    denorm_fnc : function
        Functino that takes field's value in the common format
        and converts it to this format
    """
    def __init__(self, names: list[str], _type: str,
                 query_fnc: Callable[[Any], tuple] = def_query_fnc,
                 norm_fnc: Callable[[Any], Any] = lambda a : a,
                 denorm_fnc: Callable[[Any], Any] = lambda a : a):
        self.name = names[0]
        self.common_name = names[0] if len(names) == 1 else names[1]
        self._type = _type
        self.query_fnc = query_fnc
        self.norm_fnc = norm_fnc
        self.denorm_fnc = denorm_fnc

    def __repr__(self):
        return f'Field({self.name}, {self.common_name}, {self._type})' 


class Common:
    def __init__(self, fields: list[tuple]):
        self.fields: dict[str, tuple] = {}
        for field in fields:
            self.fields[field[0]] = field[1:]

    def __repr__(self):
        return f'Common({self.fields})' 


class Table:
    def __init__(self, name: str, fields: list[Field], con):
        # TODO:
        # Check if table named `name` exists in the `con` database
        self.name = name
        self.con = con

        # TODO:
        # Check if fields are in the `name` table
        self.fields: dict[str, Field] = {}
        self.cnames: dict[str, str] = {}
        for field in fields:
            self.fields[field.name] = field
            self.cnames[field.common_name] = field.name

    def fetch_num_rows(self, use_rowid: bool = True):
        # Fast query but requires rowid
        f_query = f"SELECT COALESCE(MAX(rowid), 0) FROM {self.name}" 
        # Slow query but doesn't require rowid
        s_query = f"SELECT Count(*) FROM {self.name}"

        query = f_query if use_rowid else s_query
        cur = self.con.cursor()
        cur.execute(query)

        return cur.fetchone()

    # TODO:
    # Add limit
    def to_numpy_array(self) -> np.ndarray:
        dtype = np.dtype(
            [(f.name, PTNDType[f._type]) for f in self.fields.values()])

        num_rows = self.fetch_num_rows() 
        if num_rows is None:
            print(f"Failed to fetch `{self.name}` table's number of rows")

        cur = self.con.cursor()

        fields = ', '.join(self.fields.keys())
        query = f'SELECT {fields} FROM {self.name}'
        arr = np.empty(num_rows, dtype=dtype)
        for i, row in enumerate(cur.execute(query)):
            arr[i] = row

        return arr;

    def to_normalized_numpy_array(self, common: Common) -> np.ndarray:
        dtype = np.dtype(
            [(k, PTNDType[common.fields[k][0]]) for k, v in self.cnames.items()])

        num_rows = self.fetch_num_rows() 
        if num_rows is None:
            print(f"Failed to fetch `{self.name}` table's number of rows")

        cur = self.con.cursor()

        fnames = list(self.fields.keys())
        fields = ', '.join(fnames)
        query = f'SELECT {fields} FROM {self.name}'
        arr = np.empty(num_rows, dtype=dtype)
        for i, row in enumerate(cur.execute(query)):
            arr[i] = tuple([self.fields[fnames[i]].norm_fnc(field)
                      for i, field in enumerate(row)])

        return arr;

    def create_queries(self, fields, row):
        field_names = list(self.fields.keys())
        query_vals: list[tuple] = []
        for k in fields:
            f = self.fields[k]
            f2_index = field_names.index(f.name)
            # print(f2_index, f.name)
            query_vals.append(f.query_fnc(row[f2_index]))

        return query_vals

    def __repr__(self):
        return f'Table({self.name}, {self.fields}, {self.cnames})' 
