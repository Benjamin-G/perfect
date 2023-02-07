from random import randrange
from typing import List

import pandas as pd
from IPython.core.display_functions import display
from faker import Faker
from pandas import DataFrame
from prefect import task
from pydantic import BaseModel, ValidationError, validator, constr, Field


class DataFrameV1(BaseModel):
    name: str
    score: int = Field(..., ge=1)
    height: float
    weight: float
    friends: list[constr(to_lower=True, regex='[A-Za-z]{2,25}( [A-Za-z]{2,25})?')] = Field(..., min_items=1)

    @validator("name")
    def name_must_have_first_and_last(cls, val: str) -> str:
        if len(val.split(' ')) == 2:
            return 'John Doe'
            # raise ValueError('Name must have first and last')
        return val


class DataFrameV1Validator(BaseModel):
    df_dict: List[DataFrameV1]


@task(name="Create Dataframe")
def create_dataframe_pydantic() -> DataFrame:
    fake = Faker()

    dicts = []
    for _ in range(10):
        friends = []
        print(fake.safe_color_name())
        for _ in range(5):
            friends.append(fake.name())

        # validate from on instantiation
        try:
            data = DataFrameV1(name=fake.name(), score=int(randrange(0, 101, 2)), height=float(randrange(0, 101, 2)),
                               weight=float(randrange(0, 101, 2)), friends=friends)
            dicts.append(data)
        except ValidationError as e:
            print(e)

    df = pd.DataFrame(map(dict, dicts))

    display(df['friends'])
    # df['name'] = df['name'].map('I am {}'.format)
    display(df)
    # print(df.dtypes)
    # display(df['friends'].str.join(','))
    # print(df['friends'].str.join(',').str.contains('Lisa|Eric|John'))

    return df
