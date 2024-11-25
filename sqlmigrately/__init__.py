__version__ = "0.1.1"
from .main import migrate_table
from .types import map_type
from .utils import (
    ColumnDiff,
    alter_table,
    check_table,
    get_schema_diff,
    get_table_schema,
)
