from hashlib import md5
from typing import Sequence
from modules.delta import BaseTable
import abc
import deltalake
from modules import delta, datalake, processed_files
from pandera.typing.polars import DataFrame
import polars as pl


class Pipeline(abc.ABC):
    """Base abstract class for ETL pipelines."""

    _models: tuple[type[BaseTable], ...]
    _meta_models: tuple[type[BaseTable], ...]

    def __init_subclass__(cls, models: tuple[type[BaseTable], ...]) -> None:
        cls._models = models
        # cls._meta_models = (model.with_columns(file_id) for model in models)

    def run(self) -> None:
        self.extract()
        self.transform_and_load()

    def transform_and_load(self) -> None:
        """Glob datalake, and transform and load to deltalake any new file."""
        self.init_tables()
        for filename in datalake.glob_folder(self.__class__.__name__):
            file_content = datalake.download_file(self.__class__.__name__, filename)

            # TODO: Have file_hash as metadata in filename blabla
            file_hash = md5(file_content).digest()

            self.load_dataframes(filename, self.transform_file(filename, file_content))
            processed_files.add_processed_file(filename, file_hash)

    @abc.abstractmethod
    def extract(self) -> None:
        """Extract from source to datalake."""

    @abc.abstractmethod
    def transform_file(
        self,
        filename: str,
        file_content: bytes,
    ) -> Sequence[DataFrame[BaseTable]]:
        """Transform."""

    def save_to_bronze(self, filename: str, file_content: bytes) -> None:
        """Save extracted data to bronze layer as file."""
        datalake.upload_file(self.__class__.__name__, filename, file_content)

    def load_dataframes(
        self, filename: str, dataframes: Sequence[DataFrame[BaseTable]]
    ) -> None:
        """Load from datalake to deltalake."""
        for model, dataframe in zip(self._models, dataframes):
            table_path = f"local/silver/{model.__name__}"
            # This is for our SCD0, SCD1 would need mode="merge"
            dataframe.with_columns(pl.lit(filename).alias("file_id")).write_delta(
                table_path,
                mode="overwrite",
                delta_write_options={"predicate": f"file_id = '{filename}'"},
            )

            # Do we want that?
            deltalake.DeltaTable(table_path).optimize.compact()

            # No need since we use already partitions
            # deltalake.DeltaTable(table_path).optimize.z_order(["file_id"])

    def init_tables(self) -> None:
        """Creates tables on delta lake."""
        for model in self._models:
            table_path = f"local/silver/{model.__name__}"
            delta.create_delta_table(
                table_path,
                model,
                exists_ok=True,
                # TODO: Allow more control over partitions
                partition_by=["file_id"],
            )

            # TODO: Decide where and when to do these kind of things, we probably
            # want more control depending on cases
            deltalake.DeltaTable(table_path).vacuum(
                retention_hours=0,
                dry_run=False,
                # TODO: remove
                enforce_retention_duration=False,  # bypasses the 168-hour check FOR TESTING
            )
            # deltalake.DeltaTable(table_path).optimize().z_order()
