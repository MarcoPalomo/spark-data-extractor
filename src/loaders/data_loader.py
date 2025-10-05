import logging
from typing import List, Optional, Dict, Any

from pyspark.sql import DataFrame


class DataLoader:
    """Common data loading utilities for Spark DataFrames.

    Supports file outputs (parquet/csv/json) and JDBC.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    # --------------------------
    # File sinks
    # --------------------------
    def to_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        partition_by: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.logger.info("Writing Parquet to %s (mode=%s, partition_by=%s)", path, mode, partition_by)
        writer = df.write.mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if options:
            for k, v in options.items():
                writer = writer.option(k, v)
        writer.parquet(path)

    def to_csv(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        header: bool = True,
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.logger.info("Writing CSV to %s (mode=%s, header=%s)", path, mode, header)
        writer = df.write.mode(mode).option("header", header)
        if options:
            for k, v in options.items():
                writer = writer.option(k, v)
        writer.csv(path)

    def to_json(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        options: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.logger.info("Writing JSON to %s (mode=%s)", path, mode)
        writer = df.write.mode(mode)
        if options:
            for k, v in options.items():
                writer = writer.option(k, v)
        writer.json(path)

    # --------------------------
    # JDBC sink
    # --------------------------
    def to_jdbc(
        self,
        df: DataFrame,
        url: str,
        table: str,
        mode: str = "append",
        properties: Optional[Dict[str, Any]] = None,
        batchsize: Optional[int] = None,
    ) -> None:
        """Write to a JDBC table.

        properties should include user, password, and driver.
        """
        props = dict(properties or {})
        if batchsize:
            props["batchsize"] = str(batchsize)
        self.logger.info("Writing to JDBC table %s (mode=%s, url=%s)", table, mode, url)
        df.write.mode(mode).jdbc(url=url, table=table, properties=props)

