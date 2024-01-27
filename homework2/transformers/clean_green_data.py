if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    print(f'initial data size = {len(data)}')

    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    print(f'data size after removing zero passengers count and zero distance = {len(data)}')

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    data.rename(columns={'VendorID':'vendor_id',
                         'RatecodeID':'ratecode_id',
                         'PULocationId':'pu_location_id',
                         'DOLocationID':'do_location_id'}, inplace=True)

    print(f'vendor_id unique values is {list(data["vendor_id"].unique())}')

    print(f'number of partitions {data["lpep_pickup_date"].nunique()}')

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['vendor_id'].isin([1, 2]).all(), 'vendor_id contains incorrect values'
    assert (output['passenger_count'] > 0).all(), 'passengers count is zero!'
    assert (output['trip_distance'] > 0).all(), 'trip distance is zero'

