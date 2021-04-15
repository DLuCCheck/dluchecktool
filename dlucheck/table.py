from typing import Callable, Any
import pandas as pd # type: ignore
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

    check_fnc : function
        Function that compares to values.
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


# TODO:
# - Add CommonField class
class Common:
    # TODO: potentially change type to np.void
    @staticmethod
    def __def_check_fnc(a: Any, b: Any) -> float:
        return a == b

    def __init__(self, fields: list[tuple]):
        self.fields: dict[str, tuple] = {}
        self.check_fncs: list[Callable[[Any, Any], float]] = []
        for field in fields:
            self.fields[field[0]] = field[1:3]
            self.check_fncs\
                .append(field[3] if len(field) == 4 else Common.__def_check_fnc)

    def check_fnc(self, a: Any, b: Any) -> float:
        fields = list(self.fields.values())
        similarity: float = 0
        for i, check_fnc in enumerate(self.check_fncs):
            similarity += check_fnc(a[i], b[i]) * fields[i][1]

        return similarity;

    def __repr__(self):
        return f'Common({self.fields})' 


"""
Contains Table information

Attributes
----------

name : str
    Table's name
fields : list[Field]
    Contains all relevant fields in the table
con : sqlite3.Connection, optional
    Connection to the database in question or None
array: np.memmap, optional
    Array containing all the table's data or None
"""
class Table:
    def __init__(self, name: str, fields: list[Field], con=None, array=None):
        # TODO:
        # Check if table named `name` exists in the `con` database
        self.name = name
        self.con = con
        self.data = array
        self.norm_data: Any = None

        # TODO:
        # Check if fields are in the `name` table
        self.fields: dict[str, Field] = {}
        self.cnames: dict[str, str] = {}
        for field in fields:
            self.fields[field.name] = field
            self.cnames[field.common_name] = field.name

        self.dtype = np.dtype([(f.name, PTNDType[f._type])
                               for f in self.fields.values()])

    def fetch_num_rows(self, use_rowid: bool = True):
        # Fast query but requires rowid
        f_query = f"SELECT COALESCE(MAX(rowid), 0) FROM {self.name}" 
        # Slow query but doesn't require rowid
        s_query = f"SELECT Count(*) FROM {self.name}"

        query = f_query if use_rowid else s_query
        cur = self.con.cursor()
        cur.execute(query)

        return cur.fetchone()

    def from_excel_table(self, path: str) -> np.ndarray:
        import pandas as pd     # type: ignore
        # column_dtypes = self.dtype.fields
        dtypes = {n: t.str for n, (t, s) in self.dtype.fields.items()} # type: ignore

        names = self.fields.keys()
        data_frame = pd.read_excel(path, header=None, names=names,
                                   dtype=dtypes)

        table_array = data_frame.to_records(index=False,
                                            column_dtypes=dtypes)

        self.data = np.array(table_array)
        self.norm_data = self.data # type: ignore
        return self.data
        # return np.asarray(table_array, dtype=column_dtypes) # type: ignore

    # TODO:
    # - Add a way to define a limit
    # - Use numpy.mmemap for big arrays
    def to_numpy_array(self) -> np.ndarray:
        num_rows = self.fetch_num_rows() 
        if num_rows is None:
            print(f"Failed to fetch `{self.name}` table's number of rows")

        cur = self.con.cursor()

        fields = ', '.join(self.fields.keys())
        query = f'SELECT {fields} FROM {self.name}'
        arr = np.empty(num_rows, dtype=self.dtype)
        for i, row in enumerate(cur.execute(query)):
            arr[i] = row

        self.data = arr;
        return arr;

    """Save all the table's rows into a numpy array
    """
    def to_normalized_numpy_array(self, common: Common) -> np.ndarray:
        # Create structured array
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

        self.norm_data = arr
        return arr;

    def find_all_similar(self, common: Common, sim: float):
        data = self.norm_data

        if data is None:
            print("Warning: No table data available")
            return

        candidates: list[tuple[int, list[int]]] = []
        # Really slow
        for i, row in enumerate(data):
            similar = find_similar_indices(data, row, common, sim)
            if len(similar) > 0:
                candidates.append((i, similar))

        return candidates

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


def find_similar_indices(data: np.ndarray, row: tuple,
                         common: Common, sim: float):
    indices: list[int] = [] 

    for i, cand in enumerate(data):
        s = common.check_fnc(cand, row)
        if s >= sim:
            indices.append(i)
    return indices


def find_all_similar(data, common: Common, sim: float):
    candidates: list[tuple[int, list[int]]] = []
    # Really slow
    for i, row in enumerate(data):
        similar = find_similar_indices(data, row, common, sim)
        if len(similar) > 0:
            candidates.append((i, similar))

    return candidates

def find_similar(data: np.ndarray, row: tuple,
                 common: Common, sim: float) -> np.ndarray:

    dtype = data.dtype.descr + [('Similarity', np.float64)] # type: ignore
    indices = find_similar_indices(data, row, common, sim)

    candidates: np.ndarray = np.empty(len(indices), dtype=dtype)

    for i, indx in enumerate(indices):
        s = common.check_fnc(data[indx], row)
        candidates[i] = tuple(data[indx]) + (round(s, 5),)
    return candidates


# On the works
def find_duplicate_indices(array: np.ndarray, row: tuple, common: Common):
    candidates = array
    for i, (n, v) in enumerate(common.fields.items()):
        if v[1] > 0.0:
            candidates = candidates[np.where(candidates[n] == row[i])]

    return candidates

def find_duplicates(array: np.ndarray, row: tuple, common: Common):
    candidates = array
    for i, (n, v) in enumerate(common.fields.items()):
        if v[1] > 0.0:
            candidates = candidates[np.where(candidates[n] == row[i])]

    return candidates

def array_to_table(array: np.ndarray, excel_path: str):
    pd.DataFrame(array).to_excel(excel_path, index=False)