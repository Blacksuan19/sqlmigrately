class TableDoesNotExistError(ValueError):
    """Raised when a table does not exist in the database"""

    def __init__(self, table_name: str):
        super().__init__(f"Table {table_name} does not exist")


class UnknownOperationError(ValueError):
    """Raised when an unknown operation is provided"""

    def __init__(self, operation: str):
        super().__init__(f"Unknown operation: {operation}")
