from typing import Callable, Any
import time

# Convert date to time in epoch
def date_to_epoch(fval: str):
    time_tuple = time.strptime(fval, "%Y-%m-%d %H:%M:%S")
    return int(time.mktime(time_tuple))

def epoch_to_date(fval: int):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(fval))

def search_by_words_all(words):
    # Here {0} stands for the field's name
    query = ' AND '.join(['{0} LIKE ?' for word in words])
    return (query, tuple([f'%{word}%' for word in words]))

def search_by_words_any(words):
    # Here {0} stands for the field's name
    query = ' OR '.join(['{0} LIKE ?' for word in words])
    return (query, tuple([f'%{word}%' for word in words]))

def def_query_fnc(fval: Any) -> tuple[str, tuple]:
    return ('{0} = ?', (fval, ))


def fuzz_compare(a: str, b: str) -> float:
    from fuzzywuzzy import fuzz # type: ignore
    return fuzz.token_set_ratio(a, b) * 0.01
