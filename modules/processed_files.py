import deltalake
from modules import delta
from modules.delta import BaseModel
from pandera.typing.polars import DataFrame
import datetime as dt

import pandera.polars as pa

_TABLE_PATH = "local/silver/meta_processed_files"


# TODO: switch to pydantic for nice insert?
class MetaProcessedFile(BaseModel):
    """Table with list of processed from files."""

    file_id: str
    file_hash: bytes = pa.Field()
    # TODO: How to have tz dt? Should we have custom type with nice conversion?
    processed_at: dt.datetime = pa.Field(
        description="When the file was processed from bronze to silver, in UTC."
    )


def init_table() -> None:
    delta.create_delta_table(
        _TABLE_PATH,
        MetaProcessedFile,
        exists_ok=True,
    )


def add_processed_file(file_id: str, file_hash: bytes) -> None:
    (
        DataFrame[MetaProcessedFile](
            {
                "file_id": [file_id],
                "file_hash": [file_hash],
                "processed_at": [dt.datetime.now(dt.UTC).replace(tzinfo=None)],
            }
        )
        .write_delta(
            _TABLE_PATH,
            mode="merge",
            delta_merge_options={
                "predicate": "s.file_id = t.file_id",
                "source_alias": "s",
                "target_alias": "t",
            },
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    deltalake.DeltaTable(_TABLE_PATH).optimize.z_order(["file_id"])
