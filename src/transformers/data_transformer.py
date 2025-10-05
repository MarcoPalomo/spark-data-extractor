import logging
from typing import List, Dict, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class DataTransformer:
    """Common transformation helpers for Spark DataFrames.

    Methods return a new DataFrame to allow method chaining.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def select_columns(self, df: DataFrame, columns: List[str]) -> DataFrame:
        self.logger.info("Selecting columns: %s", columns)
        return df.select(*[col(c) for c in columns])

    def with_columns(self, df: DataFrame, expressions: Dict[str, str]) -> DataFrame:
        """Add/replace columns using SQL expressions.

        expressions example: {"value_doubled": "value * 2"}
        """
        self.logger.info("Adding/replacing columns: %s", list(expressions.keys()))
        out = df
        for name, expr in expressions.items():
            out = out.withColumn(name, col(expr) if expr in out.columns else out.sql_ctx.sql(f"SELECT {expr} as expr").col("expr"))
        return out

    def drop_nulls(self, df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
        self.logger.info("Dropping nulls subset=%s", subset)
        return df.dropna(subset=subset)

    def deduplicate(self, df: DataFrame, subset: Optional[List[str]] = None) -> DataFrame:
        self.logger.info("Dropping duplicates subset=%s", subset)
        return df.dropDuplicates(subset=subset)

    def cast_columns(self, df: DataFrame, casts: Dict[str, str]) -> DataFrame:
        """Cast columns to target Spark SQL types, e.g. {"amount": "double"}."""
        self.logger.info("Casting columns: %s", casts)
        out = df
        for name, dtype in casts.items():
            if name in out.columns:
                out = out.withColumn(name, col(name).cast(dtype))
        return out

