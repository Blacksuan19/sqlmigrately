dtype_mapping = {
    "int64": "INTEGER",
    "float64": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "DATETIME",
    "object": "TEXT",
    "category": "TEXT",
    "timedelta64[ns]": "INTERVAL",
    "int32": "INTEGER",
    "float32": "FLOAT",
    "datetime64[ns, UTC]": "DATETIME",
    "complex128": "FLOAT",
    "uint8": "INTEGER",
    "uint16": "INTEGER",
    "uint32": "INTEGER",
    "uint64": "INTEGER",
    "Int8": "INTEGER",
    "Int16": "INTEGER",
    "Int32": "INTEGER",
    "Int64": "INTEGER",
    "string": "TEXT",
}


def map_type(dtype: str, *, default: str = "TEXT") -> str:
    """map pandas dtype to sqlalchemy type"""
    return dtype_mapping.get(str(dtype), default)
