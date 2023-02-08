from pydantic import ValidationError

from pydantic_model import create_dataframe_pydantic, DataFrameV1Validator


def test_create_dataframe_pydantic():
    df = create_dataframe_pydantic.fn()

    print(len(df.index))
    assert len(df.index) <= 10

    # validate from a dataframe
    try:
        res = DataFrameV1Validator(df_dict=df.to_dict(orient="records"))
        print('valid')
        print(res)
    except ValidationError as e:
        assert False, f"'DataFrameV1Validator' raised an exception {e}"
