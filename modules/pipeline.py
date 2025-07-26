from pathlib import Path
from typing import Sequence

import pandera.polars as pa
from modules.delta import BaseModel
import abc
import deltalake
from modules import delta, datalake, processed_files
from pandera.typing.polars import DataFrame
import polars as pl
import datetime as dt

FILENAME_COLUMN = "file_name"


class Pipeline(abc.ABC):
    """Base abstract class for ETL pipelines."""

    _models: tuple[type[BaseModel], ...]

    def __init_subclass__(cls, models: tuple[type[BaseModel], ...]) -> None:
        cls._models = models

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def run(self) -> None:
        self.extract()
        self.init_tables()
        self.transform_and_load()

    def transform_and_load(self) -> None:
        """Glob datalake, and transform and load to deltalake any new file."""
        # TODO: Only process new files
        for file_name in datalake.glob_folder(self.name):
            # TODO: Have file_hash and version as metadata in filename blabla
            # file_hash = md5(file_content).digest()
            file_version = dt.datetime.fromtimestamp(
                (Path("local/bronze") / self.name / file_name).stat().st_mtime
            )

            # Current file version already processed
            file_version_in_db = processed_files.get_file_version(self.name, file_name)
            print(file_version_in_db)
            if file_version_in_db and file_version <= file_version_in_db:
                continue

            file_content = datalake.download_file(self.name, file_name)
            self.load_dataframes(
                file_name,
                self.validate_models(self.transform_file(file_name, file_content)),
            )
            processed_files.add_processed_file(self.name, file_name, file_version)

    @abc.abstractmethod
    def extract(self) -> None:
        """Extract from source to datalake."""

    @abc.abstractmethod
    def transform_file(
        self,
        filename: str,
        file_content: bytes,
    ) -> Sequence[pl.DataFrame]:
        """Transform."""

    def validate_models(
        self,
        dataframes: Sequence[pl.DataFrame],
    ) -> Sequence[DataFrame[BaseModel]]:
        """Validate dataframes against pandera models."""
        return [
            dataframe.pipe(model.validate)
            for dataframe, model in zip(dataframes, self._models, strict=True)
        ]

    def save_to_bronze(self, file_name: str, file_content: bytes) -> None:
        """Save extracted data to bronze layer as file."""
        datalake.upload_file(self.__class__.__name__, file_name, file_content)

    def load_dataframes(
        self,
        file_name: str,
        dataframes: Sequence[DataFrame[BaseModel]],
    ) -> None:
        """Load from datalake to deltalake."""
        for model, dataframe in zip(self._models, dataframes):
            table_path = f"local/silver/{model.__name__}"
            # This is for our SCD0, SCD1 would need mode="merge"
            dataframe.with_columns(
                pl.lit(file_name).alias(FILENAME_COLUMN)
            ).write_delta(
                table_path,
                mode="overwrite",
                delta_write_options={"predicate": f"{FILENAME_COLUMN} = '{file_name}'"},
            )

    def init_tables(self) -> None:
        """Creates tables on delta lake."""
        for model in self._models:
            table_path = f"local/silver/{model.__name__}"
            delta.create_delta_table(
                table_path,
                self.add_meta_to_schema(model.to_schema()),
                exists_ok=True,
                # TODO: Allow more control over partitions
                partition_by=[FILENAME_COLUMN],
            )

            # TODO: Decide where and when to do these kind of things, we probably
            # want more control depending on cases
            deltalake.DeltaTable(table_path).vacuum(
                retention_hours=0,
                dry_run=False,
                # TODO: remove
                enforce_retention_duration=False,  # bypasses the 168-hour check FOR TESTING
            )
            # Do we want that?
            deltalake.DeltaTable(table_path).optimize.compact()

    @classmethod
    def add_meta_to_schema(cls, schema: pa.DataFrameSchema) -> pa.DataFrameSchema:
        """Add metadata columns to the table schema."""
        return schema.add_columns({FILENAME_COLUMN: pa.Column(str)})
