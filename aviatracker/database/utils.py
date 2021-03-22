from typing import List, Tuple


def column_value_to_str(fields: Tuple) -> List[str]:
    columns_str: str = ", ".join(fields)
    values_str: str = ",".join([f"%({field})s" for field in fields])
    return [columns_str, values_str]
