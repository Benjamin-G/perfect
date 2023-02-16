from prefect.testing.utilities import prefect_test_harness

from flows.flow import retrieve_data_from_all


def test_retrieve_data_from_all():
    with prefect_test_harness():
        retrieve_data_from_all()
        assert True
