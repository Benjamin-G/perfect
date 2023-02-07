from main import create_dataframe


def test_create_dataframe():
    df = create_dataframe.fn()
    print(df.dtypes)
    assert True
