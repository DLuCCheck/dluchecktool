import json
import numpy as np
from .table import Table

def load_json_file(json_path: str) -> dict:
    json_src = ''
    with open(json_path, 'r') as f:
        json_src = f.read()

    return json.loads(json_src)

class Check:
    def __init__(self, check_name: str, check_data: dict):
        self.name = check_name
        self.repeat = check_data["repeat"]
        self.query = check_data["query"]
        self.success = check_data["success"]
        self.label_fnc = check_data["label"]

    def run(self, table: Table):
        results: list = []

        # In construction
        if self.repeat:
            # num_rows = table.fetch_num_rows()
            # for row in table.iterate():
            #     """
            #     String formating will be necessary to figure out
            #     where to replace the `{}`. Additionally `{}` may cause
            #     problems so perhaps a simbol like %% or something similar would
            #     be a better replacment
            #     """
            #     query = self.query.format()
            pass
        else:
            """"
            TODO:
                security checks before executing the query.
            """
            cur = table.con.cursor()
            fields = ', '.join([f'"{f}"' for f in table.fields.keys()])
            query = f"SELECT {fields} FROM {table.name} WHERE {self.query}"
            results = [row for row in cur.execute(query)]
        return np.array(results, dtype=table.dtype)

    # TODO: add ability to add custom python function as success function
    # Predefined success function
    def success_fnc(self, results: np.ndarray) -> int:
        if self.success == "NO_SUCCESS":
            return -1

        elif self.success == "SUCCESS_ANY":
            return results.size > 0

        elif self.success == "SUCCESS_NONE":
            return results.size == 0

        return -1

    def __repr__(self):
        return f"Check({self.name},"\
                f"{self.repeat},"\
                f"{self.query}"

class CheckManager:
    @staticmethod
    def load_from_json(json_path: str):
        check_info = load_json_file(json_path)

        checks = list(check_info.items())

        return CheckManager([Check(name, check) for (name, check) in checks])

    def __init__(self, checks: list[Check]):
        self.checks = checks

    def run(self, table: Table):
        results: dict[int, list] = {
            -1: [],
            0: [],
            1: []
        }

        for check in self.checks:
            result = check.run(table)
            success = check.success_fnc(result)

            results[success].append(result)

        return results
