from typing import List

import httpx

from tutorial import *


@task(retries=3)
def get_stars(repo: str, client):
    print(type(client))
    url = f"https://api.github.com/repos/{repo}"
    count = client.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")


@flow(name="GitHub Stars")
def github_stars(repos: List[str]):
    with httpx.Client() as client:
        for repo in repos:
            get_stars(repo, client)


# run the flow!
github_stars(["PrefectHQ/Prefect"])
results = api_flow("https://catfact.ninja/fact")
print(results)
