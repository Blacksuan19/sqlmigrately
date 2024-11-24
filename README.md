# SQLMigrately

SQLMigrately is a simple tool that performs SQL migrations live at runtime. It
is designed to be used in development environments, where you want to apply
changes to your database schema without having to stop the application or in
production if you are brave enough and know what you are doing (me).

[![view - Documentation](https://img.shields.io/badge/PyPi-0.0.7-blue?style=for-the-badge)](https://pypi.org/project/SQLMigrately "view package on PyPi")
&nbsp;&nbsp;&nbsp;
[![GitHub Actions](https://img.shields.io/badge/github%20actions-%232671E5.svg?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/Blacksuan19/sqlmigrately/actions "Build with github actions")

## Features

- Apply SQL migrations live at runtime
- Automatically infer column types
- Allow adding and removing columns
- Allow specifying column types
- Allow updating scheme with or without pushing new data

## Installation

```bash
pip install SQLMigrately
```

## Usage

The main function in this library is `migrate_table`, which given a dataframe, a
table name, and a database engine, will update the table schema to match the
dataframe schema. the function has other optional parameters that allow you to
control the behavior of the migration.

```python
import pandas as pd

from sqlmigrately import migrate_table
from sqlalchemy import create_engine

# create a connection to the database
engine = create_engine('sqlite:///test.db')

# read the current schema
df = pd.read_sql('SELECT * FROM users', engine)
df
```

| name | age | city        |
| ---- | --- | ----------- |
| John | 20  | New York    |
| Doe  | 30  | Los Angeles |

```python
# create a dataframe with the new schema
df = pd.DataFrame({
    'name': ['Jane', 'Smith'],
    'age': [23, 42],
    'city': ['Ohio', 'California'],
    'country': ['USA', 'USA']
})

# apply the migration
migrate_table(df, 'users', engine, push_data=True)

# show updated table and schema
df = pd.read_sql('SELECT * FROM users', engine)
```

| name  | age | city        | country |
| ----- | --- | ----------- | ------- |
| John  | 20  | New York    | NULL    |
| Doe   | 30  | Los Angeles | NULL    |
| Jane  | 23  | Ohio        | USA     |
| Smith | 42  | California  | USA     |

for full example on usage, check the testing notebook [here](./test.ipynb).

## Function Parameters

the full signature of the `migrate_table` function is as follows:

```python
def migrate_table(
    table_name: str,
    df: pd.DataFrame,
    db_eng: Engine,
    *,
    push_data: bool = True,
    add_cols: bool = True,
    remove_cols: bool = False,
    column_type_map: dict = None,
):
    """
    Update given `table_name` schema in the database to match the schema of the given `df`.
    Assumes minimal changes to the table schema.

    Args:
        table_name (str): name of the table
        df (pd.DataFrame): dataframe to migrate
        db_eng (Engine): sqlalchemy engine
        push_data (bool, optional): whether to push dataframe data to the table. Defaults to True.
        add_cols (bool, optional): whether to add new columns in dataframe to the table. Defaults to True.
        remove_cols (bool, optional): whether to remove removed columns from the table. Defaults to False.
        column_type_map (dict, optional): mapping of column names to their types. Defaults to None, which means that the types are inferred from the dataframe.

    Raises:
        TableDoesNotExistError: raised when the given table does not exist in the database
    """
```

The library also provides other helper functions that can be useful for dealing
with SQL databases, such as:

- `get_schema_diff`: get the difference between the dataframe and the table
  schema

```python

@dataclass
class ColumnDiff:
    """class to hold the difference between two sets of columns"""

    added: List[Dict[str, Any]] = field(default_factory=list)
    removed: List[Dict[str, Any]] = field(default_factory=list)

def get_schema_diff(table_name: str, df: pd.DataFrame, db_eng: Engine) -> ColumnDiff:
    """get the difference between the dataframe and the table schema"""

```

- `get_table_schema`: get the schema of a table in the database

```python
def get_table_schema(table_name: str, db_eng: Engine) -> Dict[str, str]:
    """get mapping of table columns and their types"""
```

- `map_type`: map pandas types to sql types

```python
def map_type(dtype: str, *, default: str = "TEXT") -> str:
    """map pandas dtype to sqlalchemy type"""
```

## Why create this?

simple answer: LLMs, long answer, we added an LLM based step that generates text
for each row in a given data to an existing data pipeline, to keep track of the
cost, a new column named `llm_cost` was added to the metrics dataframe in code
to store the total cost of generating the text, since the metrics table is
dynamically generated and it's schema can change with no notice, I needed a way
to update the schema of the table without needing to create any boilerplate
config files for alembic or whatever migration tool, so I decided to create this
library to do just that.

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE)
file for details.
