import pandas as pd

from flows.flow import retrieve_data_from_all

# my_flow_error()
df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
retrieve_data_from_all('Hello World', df)
