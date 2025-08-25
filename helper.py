# import logging
import re
from typing import Any, Dict, Iterable, List, Tuple

from termcolor import colored    # type: ignore


LOG_FORMAT_FULL = colored('[%(asctime)s][%(process)d:%(processName)s]', 'green', attrs=['bold', 'dark']) + colored('[%(filename)s#%(funcName)s:%(lineno)d]', 'white', attrs=['bold', 'dark']) + colored('[%(levelname)s]', 'magenta', attrs=['bold', 'dark']) + ' %(message)s'
LOG_FORMAT_DEBUG = colored('[%(filename)s#%(funcName)s:%(lineno)d]', 'white', attrs=['bold', 'dark']) + colored('[%(levelname)s]', 'magenta', attrs=['bold', 'dark']) + ' %(message)s'  # noqa: E501
LOG_FORMAT_SIMPLE = colored('[%(levelname)s]', 'magenta', attrs=['bold', 'dark']) + ' %(message)s'  # noqa: E501
DENGUE_COLUMNS: List[str] = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'longitude'
]


def text_to_list(el: Any, delimiter: str = '|') -> List[Any]:
    """
    el: str = an element to be parsed
    delimitator: str = a delimitator that will help split text
    return element's text splited by delimitator
    """
    return el.split(delimiter)


def list_to_dict(el: List[Any], columns: List[Any]) -> Dict[str, List[Any]]:
    """
    el: List[Any] = an element to be parsed
    columns: List[Any] = all columns from file
    return dict
    """
    return dict(zip(columns, el))


def parse_date(el: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Process an date in el and return YYYY-mm
    """
    el["year_month"] = "-".join(el["data_iniSE"].split("-")[:2])

    return el


def key_uf(el: Dict[Any, Any]) -> Tuple[str, Dict[Any, Any]]:
    """
    Return an tuple with key and element
    """
    key = el['uf']

    return (key, el)


def dengue_process(el: Tuple[str, Iterable[Dict[Any, Any]]]):
    """
    Receives a tuple of element and simplify it.
    """
    uf, registers = el
    for reg in registers:
        if bool(re.search(r"\d", reg["casos"])):
            yield (f"{uf}-{reg['year_month']}", float(reg["casos"]))
        else:
            yield (f"{uf}-{reg['year_month']}", 0.0)


def key_uf_year_month_list(el: List[Any]) -> Tuple[Any, Any]:
    """
    Receive a list of elements (date[YYY-mm-dd], mm, uf)
    Returns a Tuple ('UF-YYYY-mm', mm)
    """
    date, mm, uf = el
    year_month: str = "-".join(date.split("-")[:2])
    key: str = f"{uf}-{year_month}"
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)

    return key, mm


def round_me(el: Tuple[Any, Any], decimal_places: int = 1) -> Tuple[Any, Any]:  # noqa: E501
    """
    Receives a Tuple[key, mm] and round mm to decimal_places
    """
    key, mm = el

    return (key, round(mm, decimal_places))


def filter_empty_fields(el: Tuple[str, Dict[str, List[Any]]]) -> bool:
    """
    Remove all elements with empty key
    """
    key, data = el
    if all([
            data["rains"],
            data["dengue"]
    ]):
        return True

    return False


def unpack_elements(els: Tuple[str, Dict[str, List[Any]]]) -> Tuple[Any, Any, Any, Any, Any]:  # noqa: E501
    """
    Convert a Tuple from ('UF-YYYY-mm', {'rains': List[float], 'dengue': List[float]}) to Tuple['UF', 'YYYY', 'mm', rains, dengue] # noqa: E501
    PS: mm means month not milimeters in this context
    """
    key, data = els
    uf, year, month = key.split("-")
    rains: str = data["rains"][0]
    dengue: str = data["dengue"][0]

    return uf, year, month, rains, dengue


def prepare_csv(elements: Tuple[Any, Any, Any, Any, Any], delimiter: str = ";") -> str:  # noqa: E501
    """
    Receive a Tuple[UF, YYYY, mm, rains, dengue] and parse it to csv as 'UF;YYYY;mm;rains;dengue' # noqa: E501
    PS: mm means month not milimeters in this context
    """
    return f"{delimiter}".join(str(el) for el in elements)
