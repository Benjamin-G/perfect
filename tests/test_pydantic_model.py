import functools
from random import randrange

import numpy as np
import pandas as pd
import pytest
from pandas import DataFrame
from pydantic import ValidationError

from pydantic_model import DataFrameV1Validator, create_dataframe_pydantic, sin_with_label_row, sin_with_label, \
    sin_return_df, sin_with_label_2


def test_create_dataframe_pydantic():
    df = create_dataframe_pydantic.fn()

    print(len(df.index))
    assert len(df.index) <= 10

    # validate from a dataframe
    try:
        res = DataFrameV1Validator(df_dict=df.to_dict(orient="records"))
    except ValidationError as e:
        assert False, f"'DataFrameV1Validator' raised an exception {e}"


@pytest.fixture(scope="session")
def create_df():
    arr = []
    for _ in range(10_000):
        arr.append({'num1': randrange(0, 101, 2), 'num2': randrange(0, 101, 2)})

    df = pd.DataFrame(arr)
    df = df.astype({'num1': np.float64})
    # print(df.info)
    return df


@pytest.fixture(scope="session")
def df_integrate():
    df = pd.DataFrame(
        {
            "a": np.random.randn(1000),
            "b": np.random.randn(1000),
            "N": np.random.randint(100, 1000, 1000),
            "x": "x",
        }
    )
    return df


def sin(v: float) -> float:
    return np.sin(v)


def sin_df(df: DataFrame) -> DataFrame:
    df['num1'] = np.sin(df['num1'])
    return df


def sin_df_2(df: DataFrame) -> DataFrame:
    # df['num1'] = df['num1'].apply(sin)
    df['num1'] = sin(df['num1'])
    return df


@pytest.mark.skip(reason="too slow")
def test_df_mutation_one(benchmark, create_df):
    df = benchmark(sin_df, create_df)
    assert len(create_df) == len(df)


@pytest.mark.skip(reason="too slow")
def test_df_mutation_two(benchmark, create_df):
    df = benchmark(sin_df_2, create_df)
    assert len(create_df) == len(df)


@pytest.mark.skip(reason="too slow")
def test_df_mutation_three(benchmark, create_df):
    df = pd.DataFrame.copy(create_df)
    df['num1'] = benchmark(lambda df: sin(df['num1']), df)
    assert len(create_df) == len(df)


def test_df_mutation_multiple_columns(benchmark, create_df):
    df = pd.DataFrame.copy(create_df)
    df['sin_of_num1'], df['label'] = benchmark(
        lambda df: zip(*df.apply(lambda x: sin_with_label(x['num1']), axis=1)), df)

    print('test_df_mutation_multiple_columns')
    print(df.head())
    assert len(create_df) == len(df)


def test_df_mutation_multiple_columns_two(benchmark, create_df):
    df = pd.DataFrame.copy(create_df)
    df['sin_of_num1'], df['label'] = benchmark(
        lambda df: zip(*df.apply(sin_with_label_row, axis=1)), df)

    print('test_df_mutation_multiple_columns_two')
    print(df.head())
    assert len(create_df) == len(df)


def test_df_mutation_multiple_columns_three(benchmark, create_df):
    df = pd.DataFrame.copy(create_df)
    df[['sin_of_num1', 'label']] = benchmark(
        lambda df: df.apply(sin_with_label_row, axis=1, result_type="expand"), df)

    print('test_df_mutation_multiple_columns_three')
    print(df.head())
    assert len(create_df) == len(df)


def test_df_mutation_multiple_columns_four(benchmark, create_df):
    df = pd.DataFrame.copy(create_df)
    df = benchmark(sin_return_df, df)

    print('test_df_mutation_multiple_columns_four')
    print(df.head())
    assert len(create_df) == len(df)


def test_df_mutation_multiple_columns_five(benchmark, create_df):
    df = pd.DataFrame.copy(create_df)
    df['sin_of_num1'], df['label'] = benchmark(
        lambda df: zip(*df['num1'].map(sin_with_label_2)), df)

    print('test_df_mutation_multiple_columns_five')
    print(df.head())
    assert len(create_df) == len(df)


def test_df_mutation_multiple_columns_six(benchmark, create_df):
    df = pd.DataFrame.copy(create_df)
    df['sin_of_num1'], df['label'] = benchmark(
        lambda df: zip(*df['num1'].apply(sin_with_label_2)), df)

    print('test_df_mutation_multiple_columns_six')
    print(df.head())
    assert len(create_df) == len(df)


def integrate_f(a, b, N):
    f = lambda x: x * (x - 1)
    s = 0
    dx = (b - a) / N
    for i in range(int(N)):
        s += f(a + i * dx)
    return s * dx


@pytest.mark.skip(reason="too slow")
def test_df_integrate(benchmark, df_integrate):
    df_integrate['integrate'] = benchmark(lambda df: df.apply(lambda x: integrate_f(x["a"], x["b"], x["N"]), axis=1),
                                          df_integrate)
    print(df_integrate.info)


@pytest.mark.skip(reason="too slow")
def test_df_integrate_2(benchmark, df_integrate):
    df_integrate['integrate'] = benchmark(
        lambda df: [integrate_f(a, b, N) for a, b, N in df[['a', 'b', 'N']].to_numpy().tolist()], df_integrate)
    print(df_integrate.info)


@pytest.mark.skip(reason="too slow")
def test_df_integrate_3(benchmark, df_integrate):
    df_integrate['integrate'] = benchmark(
        lambda df: [*map(integrate_f, df['a'].to_numpy(), df['b'].to_numpy(),
                         df['N'].to_numpy())], df_integrate)
    print(df_integrate.info)


def log_error(logger):
    def decorated(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception as e:
                if logger:
                    print('logging error')
                    logger.info(e)
                # raise

        return wrapped

    return decorated


import logging

logger = logging.getLogger()


@log_error(logger)
def f():
    raise Exception('I am exceptional')


def test_log_error():
    f()
    assert True
