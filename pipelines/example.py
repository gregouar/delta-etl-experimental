from pathlib import Path
from typing import Sequence, override
from modules.pipeline import Pipeline,BaseModel
import pandera.polars as pa
import polars as pl
from pandera.typing.polars import Series

import datetime as dt


class ExampleModel(BaseModel):
    """Customers from source."""

    customer_id: Series[str] = pa.Field(
        str_length={"min_value": 15, "max_value":15},
        description="Unique customer id, provided by source.",
    )
    first_name: Series[str] = pa.Field(description="Customer first name.")
    last_name: Series[str] = pa.Field(description="Customer last name.")
    subscription_date: Series[dt.date] = pa.Field(
        description="Date when the customer subscribed to the source service.",
    )


class ExamplePipeline(Pipeline, models=(ExampleModel,)):
    """Example pipeline."""

    @override
    def extract(self) -> None:
        """Extract from source to datalake."""
        for source_file in Path("input").glob("*"):
            self.save_to_bronze(source_file.name, source_file.read_bytes())

    @override
    def transform_file(
        self,
        filename: str,
        file_content: bytes,
    ) -> Sequence[pl.DataFrame]:
        """Transform."""
        dataframe = pl.read_csv(file_content)
        dataframe = dataframe.rename(
            {
                "Customer Id": ExampleModel.customer_id,
                "First Name": ExampleModel.first_name,
                "Last Name": ExampleModel.last_name,
                "Subscription Date": ExampleModel.subscription_date,
            },
        )
        return [dataframe]