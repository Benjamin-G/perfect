import os

import pandas as pd
from pandas import DataFrame
from prefect import task


@task(name="Get Classifier Dataframe")
def get_classifier_df() -> DataFrame:
    """
    csv file will be used to find the weights associated with each term
    :return: dataframe of the csv file
    :rtype: DataFrame
    """
    classifier_df = pd.read_csv(os.path.join(os.path.dirname(__file__), '../data/testing_data.csv'), header=None,
                                encoding='latin',
                                names=["Ingredient", "Category", "Old Abbreviation", "GS_Weight", "Binary"],
                                dtype={"Ingredient": 'string', "Category": 'string', "Old Abbreviation": 'string',
                                       "GS_Weight": float,
                                       "Binary": int})
    classifier_df['Ingredient'] = classifier_df['Ingredient'].str.upper()
    return classifier_df
