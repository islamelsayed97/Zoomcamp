import pandas as pd
from sqlalchemy import create_engine
import argparse
from time import time
import os

def main(params):
    user = params.user
    password = params .password
    host = params.host
    port = params. port
    db = params.db
    table_name = params.table_name
    green_url = params.green_url
    zones_url = params.zones_url

    green_file = 'green_file.csv'
    zones_file = 'zones_file.csv'

    os.system(f'wget {green_url} -O {green_file}')
    os.system(f'wget {zones_url} -O {zones_file}')

    print('file has been downloaded successfully')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connected to database successfully')

    print('Reading zones data...')
    zones = pd.read_csv(zones_file)
    print('Inserting zones data into database...')
    t_start = time()
    zones.to_sql(name='zones', con=engine, if_exists='replace')
    print('Zones data inserted into database successfully, took %.3f second' % (time() - t_start))
    
    # read green_trip data
    print('Reading green trip data...')
    df_iter = pd.read_csv(green_file, iterator=True, chunksize=100000)

    t_start = time()

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.to_sql(name=table_name, con=engine, if_exists='replace')

    print(f'table recreated successfully\ninserted first chunk..., took %.3f seconds, starting a loop for the reset...' % (time() - t_start))

    while True:

        t_start = time()
        try:
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('Inserted another chunk..., took %.3f seconds' % (t_end - t_start))
        except StopIteration:
            print('All data inserted into the database successfully')
            break
        except Exception as e:
            print(f'Error: {e}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--green_url', help='url of green trips csv file')
    parser.add_argument('--zones_url', help='url of zones csv file')
    args = parser.parse_args()
    main(args)

