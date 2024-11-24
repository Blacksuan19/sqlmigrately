import pandas as pd
from loguru import logger
from sqlalchemy import Engine

from sqlmigrately.utils import TableOps, alter_table, get_table_diff


def migrate_table(
    table_name: str,
    df: pd.DataFrame,
    db_eng: Engine,
    *,
    push_data: bool = True,
    add_cols: bool = True,
    remove_cols: bool = False,
):
    """migrate a table to match the dataframe schema"""
    diff = get_table_diff(table_name, df, db_eng)

    if diff.added:
        logger.info(f"Detected new columns: {diff.added}")
        if add_cols:
            alter_table(table_name, diff.added, db_eng, operation=TableOps.ADD)

    if diff.removed:
        logger.info(f"Detected removed columns: {diff.removed}")
        if remove_cols:
            alter_table(table_name, diff.removed, db_eng, operation=TableOps.REMOVE)

    if push_data:
        logger.info(f"Appending data to table: {table_name}")
        df.to_sql(
            table_name,
            db_eng,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )
