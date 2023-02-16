import pandas as pd
from prefect.testing.utilities import prefect_test_harness

from flows.flow import retrieve_data_from_all

df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


def test_retrieve_data_from_all():
    with prefect_test_harness():
        retrieve_data_from_all('', df)
        assert True


def test_retrieve_data_from_all_bench(benchmark):
    with prefect_test_harness():
        benchmark(retrieve_data_from_all, name='', df=df)
        assert True
