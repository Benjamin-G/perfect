from pandas import DataFrame
from prefect import flow

from modules.db import get_is_brand_portal
from modules.get_csv import get_classifier_df


@flow(name="Retrieve data from all", validate_parameters=False)
def retrieve_data_from_all(name: str, df: DataFrame) -> None:
    print(name)
    print(df.info())
    df_brand: DataFrame = get_is_brand_portal()
    classifer_df: DataFrame = get_classifier_df()
    print(df_brand)
    print(classifer_df.info())
