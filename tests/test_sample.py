from random import randrange

from prefect.testing.utilities import prefect_test_harness

from main import *


def addTwo(x):
    return x + 2


def test_answer():
    assert addTwo(3) == 5


def test_add_together():
    # To test an individual task, you can access the original function using .fn:
    assert add_together.fn(2, 3) == 5


# def something(duration=0.000001, name="default"):
#     """
#     Function that needs some serious benchmarking.
#     """
#     print(name)
#     time.sleep(duration)
#     # You may return anything you want, like the result of a computation
#     return 123
#
#
# def test_my_stuff(benchmark):
#     # benchmark something
#     result = benchmark(something, 2, "benchmark")
#
#     # Extra code, to verify that the run completed correctly.
#     # Sometimes you may want to check the result, fast functions
#     # are no good if they return incorrect results :-)
#     assert result == 123


def copy_array(arr: list[str]) -> list[int]:
    res = []

    for i in arr:
        res.append(int(i))

    return res


def copy_array_list_map(arr: list[str]) -> list[int]:
    return list(map(lambda x: int(x), arr))


def copy_array_length(arr: list[str]) -> list[int]:
    res = [''] * len(arr)

    for i, v in enumerate(arr):
        res[i] = int(v)

    return res


def test_copy_array(benchmark):
    test_arr = []
    for _ in range(10_000):
        test_arr.append(str(randrange(0, 101, 2)))

    res = benchmark(copy_array, test_arr)
    assert len(res) == len(test_arr)


def test_copy_array_list_map(benchmark):
    test_arr = []
    for _ in range(10_000):
        test_arr.append(str(randrange(0, 101, 2)))

    res = benchmark(copy_array_list_map, test_arr)
    assert len(res) == len(test_arr)


def test_copy_array_length(benchmark):
    test_arr = []
    for _ in range(10_000):
        test_arr.append(str(randrange(0, 101, 2)))

    res = benchmark(copy_array_length, test_arr)
    assert len(res) == len(test_arr)


def test_github_stars(benchmark):
    with prefect_test_harness():
        benchmark(github_stars, ["PrefectHQ/Prefect"])


def test_get_stars(benchmark):
    benchmark(get_stars.fn, "PrefectHQ/Prefect", httpx.Client())
