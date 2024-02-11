import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    dfs = []

    for i in range(1, 13):
        file = "{:02d}".format(i)
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{file}.parquet'
        df = pd.read_parquet(url)
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    return final_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
