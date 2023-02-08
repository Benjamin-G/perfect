import re
from random import randrange
from typing import List, Dict

import pandas as pd
from IPython.core.display_functions import display
from faker import Faker
from pandas import DataFrame
from prefect import task
from pydantic import BaseModel, ValidationError, validator, constr, Field, root_validator

VALID_COLORS: List[str] = ['fuchsia',
                           'purple',
                           'white',
                           'green',
                           'yellow']


class DataFrameV1(BaseModel):
    name: constr(to_lower=True)
    score: int = Field(..., ge=1)
    height: float
    weight: float
    friends: list[constr(to_lower=True, regex='[A-Za-z]{2,25}( [A-Za-z]{2,25})?')] = Field(..., min_items=1)
    best_friends: list[str] = None
    favorite_color: constr(to_lower=True) = None

    @validator("name")
    def name_must_have_first_and_last(cls, val: str, values: Dict) -> str:
        if len(val.split(' ')) == 1:
            return 'John Doe'
        elif len(val.split(' ')) == 3:
            raise ValueError('Name must have first and last only: ', val, values)
        return val

    @root_validator
    def set_best_friends(cls, values) -> Dict:
        values['best_friends'] = [s for s in values['friends'] if re.compile('lisa|eric|john|jamie|jackson').match(s)]
        return values

    @root_validator
    def set_favorite_color(cls, values) -> Dict:
        if values["favorite_color"] is None:
            values['favorite_color'] = create_color()

        return values


def create_color() -> str:
    fake = Faker()
    color = fake.safe_color_name()
    return color if color in VALID_COLORS else 'hates colors'


class DataFrameV1Validator(BaseModel):
    df_dict: List[DataFrameV1]


@task(name="Create Dataframe")
def create_dataframe_pydantic() -> DataFrame:
    fake = Faker()

    dicts = []
    for _ in range(10):
        friends = []
        for _ in range(5):
            friends.append(fake.name())

        # validate from on instantiation
        try:
            data = DataFrameV1(name=fake.name(), score=int(randrange(0, 101, 2)), height=float(randrange(0, 101, 2)),
                               weight=float(randrange(0, 101, 2)), friends=friends,
                               favorite_color=create_color())
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
