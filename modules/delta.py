import pandera.polars as pa
import typing

# from pandera.api.pandas.model_config import BaseConfig
import deltalake
import pyarrow
import polars as pl


class BaseModel(pa.DataFrameModel):
    """Base pandera model with default configuration."""


BaseModel.Config.strict = "filter" # Remove extra columns on validation
BaseModel.Config.coerce = True # Parse types on validation


def empty_dataframe(schema: pa.DataFrameSchema) -> pl.DataFrame:
    """Generate empty dataframe from schema."""
    return typing.cast(pl.DataFrame, schema.coerce_dtype(pl.DataFrame(schema=[*schema.columns])))


def create_delta_table(
    table_path: str,
    pandera_schema: pa.DataFrameSchema,
    # pandera_model: type[pa.DataFrameModel],
    *,
    partition_by: list[str] | str | None = None,
    exists_ok: bool = False,
) -> None:
    """Create a new delta table from pandera model, inheriting the metadata.

    We need some manual conversion because pandera doesn't use the schema to
    create the delta table, but rather create a polars dataframe, which is then
    converted to delta table. The polars dataframe doesn't carry the metadata.
    """
    pyarrow_table = empty_dataframe(pandera_schema).to_arrow()
    # pyarrow_table: pyarrow.Table = pandera_model.empty().to_arrow()
    # pandera_schema = pandera_model.to_schema()

    deltalake.DeltaTable.create(
        table_path,
        schema=pyarrow.schema(
            [
                _merge_arrow_and_pandera_fields(
                    arrow_field, pandera_schema.columns[arrow_field.name]
                )
                for arrow_field in pyarrow_table.schema
            ]
        ),
        description=pandera_schema.description,
        mode="ignore" if exists_ok else "error",
        partition_by=partition_by,
    )


def _merge_arrow_and_pandera_fields(
    arrow_field: pyarrow.Field,
    pandera_field: pa.Column,
) -> pyarrow.Field:
    # Add metadata from pandera to arrow
    metadata = arrow_field.metadata or {}
    if pandera_field.metadata:
        metadata.update(pandera_field.metadata)
    if pandera_field.description:
        metadata[b"description"] = pandera_field.description.encode()

    return pyarrow.field(
        arrow_field.name,
        arrow_field.type,
        nullable=pandera_field.nullable,
        metadata=metadata,
    )
