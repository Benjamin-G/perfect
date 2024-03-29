import os

import pandas as pd
from pandas import DataFrame
from prefect import task
from sqlalchemy import create_engine, text
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

ENGINE = create_engine(os.environ["GC_MYSQL_URL"], echo=False)


@task(
    name="Get Brand Portal",
    retries=2,
    retry_delay_seconds=30,
)
def get_is_brand_portal() -> DataFrame:
    with ENGINE.connect() as connection:
        s = text("SELECT * FROM isBrandPortal")
        isBrandPortal = pd.read_sql(s, connection)
        return isBrandPortal


def query_text_session(query: str) -> DataFrame:
    with Session(ENGINE) as session:
        s = text(query)
        df = pd.read_sql(s, session.bind)
        return df


def query_text_connection(query: str) -> DataFrame:
    with ENGINE.connect() as connection:
        s = text(query)
        df = pd.read_sql(s, connection)
        return df


def query_db(q: str, commit=False) -> CursorResult:
    with ENGINE.connect() as connection:
        if commit:
            return connection.execute(text(q).execution_options(autocommit=True))
        return connection.execute(q)
