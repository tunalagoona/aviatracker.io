def column_value_to_str(fields):
    columns_str = ", ".join(fields)
    values_str = ",".join([f"%({field})s" for field in fields])
    return [columns_str, values_str]
