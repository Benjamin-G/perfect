import os
import time

import requests
from prefect import flow, task, unmapped
from pydantic import BaseModel

@task
def call_api(url):
    response = requests.get(url)
    print(response.status_code)
    return response.json()

@task
def parse_fact(response):
    fact = response["fact"]
    print(fact)
    return fact

@flow
def api_flow(url):
    fact_json = call_api(url)
    fact_text = parse_fact(fact_json)
    return fact_text

@flow
def common_flow(config: dict):
    print("I am a subgraph that shows up in lots of places!")
    intermediate_result = 42
    return intermediate_result

@flow
def main_flow():
    # do some things
    # then call another flow function
    data = common_flow(config={})
    # do more things


class Model(BaseModel):
    a: int
    b: float
    c: str


@task
def printer(obj):
    print(f"Received a {type(obj)} with value {obj}")
    print(obj.a, obj.b, obj.c)


@flow(name="My Example Flow",
      description="An example flow for a tutorial.",
      version=os.getenv("GIT_COMMIT_SHA"))
def model_validator(model: Model):
    printer(model)


# note that we define the flow with type hints
@flow
def validation_flow(x: int, y: str):
    printer(x)
    printer(y)


@task
def task_1():
    time.sleep(.005)
    pass

@task
def task_2():
    print("Task 2")
    pass

@flow
def my_flow():
    x = task_1()

    # task 2 will wait for task_1 to complete
    y = task_2(wait_for=[x])

@task
def print_nums(nums):
    for n in nums:
        print(n)

@task
def square_num(num):
    return num**2

@flow
def map_flow(nums):
    print_nums(nums)
    squared_nums = square_num.map(nums)
    print_nums(squared_nums)


# MAP
@task
def add_together(x, y):
    return x + y

@flow
def sum_it(numbers, static_value):
    futures = add_together.map(numbers, static_value)
    print(futures)
    return futures

@task
def sum_plus(x, static_iterable):
    return x + sum(static_iterable)
@flow
def sum_it_unmapped(numbers, static_iterable):
    futures = sum_plus.map(numbers, unmapped(static_iterable))
    print(futures)
    return futures


@task
def my_task_value_error():
    raise ValueError()

@flow
def my_flow_error():
    try:
        my_task_value_error()
    except ValueError:
        print("Oh no! The task failed.")

    state = my_task_value_error(return_state=True)

    if state.is_failed():
        print("Oh no! The task failed. Falling back to '1'. is_failed")
        result = 1
    else:
        result = state.result()

    maybe_result = state.result(raise_on_failure=False)
    if isinstance(maybe_result, ValueError):
        print("Oh no! The task failed. Falling back to '1'. isinstance")
        result += 1
    else:
        result = maybe_result

    return result
