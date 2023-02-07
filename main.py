from datetime import timedelta
from random import randrange
from typing import List

import httpx
import numpy as np
import pandas as pd
from IPython.core.display_functions import display
from faker import Faker
from pandas import DataFrame
from prefect.tasks import task_input_hash

from tutorial import *


@task(retries=3)
def get_stars(repo: str, client: httpx.Client) -> None:
    """
    Fetches all stars and prints them
    """
    print(type(client))
    url = f"https://api.github.com/repos/{repo}"
    count = client.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")


@flow(name="GitHub Stars")
def github_stars(repos: List[str]) -> None:
    """
    Gets the github stars for the list of repositories given using an httpx client
    :param repos: list of repositories
    """
    with httpx.Client() as client:
        for repo in repos:
            get_stars(repo, client)


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def hello_task(name_input: str) -> None:
    """
    prints a name to say hello to
    :param name_input:
    """
    # Doing some work
    print(f"Saying hello {name_input}")
    return "hello " + name_input


@flow
def hello_flow(name_input):
    hello_task(name_input)


@task(name="Create Dataframe")
def create_dataframe() -> DataFrame:
    """
    Returns a DataFrame

    - ("name", str),
    - ("score", int),
    - ("height", float),
    - ("weight", float)
    - ('friends', list[str])
    :return: df
    """
    # Method 1
    # columns = [('name', str),
    #            ('score', int),
    #            ('height', float),
    #            ('weight', float)]
    # df = pd.DataFrame({k: pd.Series(dtype=t) for k, t in columns})

    # Method 2
    # df = pd.DataFrame({'name': pd.Series(dtype='str'),
    #                    'score': pd.Series(dtype='int'),
    #                    'height': pd.Series(dtype='float'),
    #                    'weight': pd.Series(dtype='float')})

    # Method 3
    dtypes = np.dtype(
        [
            ("name", str),
            ("score", int),
            ("height", float),
            ("weight", float),
            ('friends', list)
        ]
    )
    df = pd.DataFrame(np.empty(0, dtype=dtypes))

    # clean columns
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    fake = Faker()

    dicts = []
    for _ in range(10):
        friends = []
        print(fake.safe_color_name())
        for _ in range(5):
            friends.append(fake.name())

        d = {
            'name': fake.name(),
            'score': int(randrange(0, 101, 2)),
            'height': float(randrange(0, 101, 2)),
            'weight': float(randrange(0, 101, 2)),
            'friends': friends
        }

        dicts.append(d)

    df = df.append(dicts, ignore_index=True, sort=False)

    print('\n================================')
    # display(df)
    # df['name'] = df['name'].map('I am {}'.format)
    display(df['score'])
    # print(df.dtypes)
    # display(df['friends'].str.join(','))
    # print(df['friends'].str.join(',').str.contains('Lisa|Eric|John'))

    return df


def convert_currency(val):
    """
    Convert the string number value to a float
     - Remove $
     - Remove commas
     - Convert to float type
    """
    new_val = val.replace(',', '').replace('$', '')
    return float(new_val)


# run the flow!
github_stars(["PrefectHQ/Prefect"])
results = api_flow("https://catfact.ninja/fact")
print(results)
