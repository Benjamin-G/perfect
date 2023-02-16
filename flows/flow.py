from prefect import flow

from modules.db import get_is_brand_portal
from modules.get_csv import get_classifier_df


@flow(name="Retrieve data from all")
def retrieve_data_from_all():
    df_brand = get_is_brand_portal()
    classifer_df = get_classifier_df()
    print(df_brand)
    print(classifer_df.info())
