import deltalake
from modules import delta
import polars as pl
from modules.delta import BaseModel
from pandera.typing.polars import DataFrame
import datetime as dt

import pandera.polars as pa

_TABLE_PATH = "local/silver/meta_processed_files"


# TODO: switch to pydantic for nice insert?
class MetaProcessedFile(BaseModel):
    """Table with list of processed from files."""

    file_name: str
    pipeline_name: str

    # file_hash: bytes = pa.Field()
    file_version: dt.datetime = pa.Field(
        description="Version of original file extract, as a datetime in UTC.",
    )

    # TODO: How to have tz dt? Should we have custom type with nice conversion?
    # TODO: try polars datetime or pandas
    processed_at: dt.datetime = pa.Field(
        description="When the file was processed from bronze to silver, in UTC."
    )


def init_table() -> None:
    delta.create_delta_table(
        _TABLE_PATH,
        MetaProcessedFile.to_schema(),
        exists_ok=True,
        partition_by=[MetaProcessedFile.pipeline_name],
    )


def add_processed_file(
    pipeline_name: str,
    file_name: str,
    file_version: dt.datetime,
) -> None:
    (
        DataFrame[MetaProcessedFile](
            {
                MetaProcessedFile.pipeline_name: [pipeline_name],
                MetaProcessedFile.file_name: [file_name],
                MetaProcessedFile.file_version: [file_version],
                MetaProcessedFile.processed_at: [
                    dt.datetime.now(dt.UTC).replace(tzinfo=None)
                ],
            }
        )
        .write_delta(
            _TABLE_PATH,
            mode="merge",
            delta_merge_options={
                "predicate": (
                    f"s.{MetaProcessedFile.file_name} = t.{MetaProcessedFile.file_name} "
                    f"AND s.{MetaProcessedFile.pipeline_name} = t.{MetaProcessedFile.pipeline_name}"
                ),
                "source_alias": "s",
                "target_alias": "t",
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    deltalake.DeltaTable(_TABLE_PATH).optimize.z_order([MetaProcessedFile.file_name])


def get_file_version(pipeline_name: str, file_name: str) -> dt.datetime | None:
    """Get last version of processed file in deltalake."""
    r = (
        pl.scan_delta(_TABLE_PATH)
        .filter(
            pl.col(MetaProcessedFile.pipeline_name) == pipeline_name,
            pl.col(MetaProcessedFile.file_name) == file_name,
        )
        .select(MetaProcessedFile.file_version)
        .first()
        .collect()
    )

    return r.item() if r.shape == (1,1) else None