from datetime import timedelta
from typing import List

import httpx
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


# run the flow!
github_stars(["PrefectHQ/Prefect"])
results = api_flow("https://catfact.ninja/fact")
print(results)
