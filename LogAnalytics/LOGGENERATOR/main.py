"""Parquet log generator - This module generates new parquet file each time it is executed.
Each new file is placed in LOGANALYTICS/logs directory."""

# pylint: disable=C0301

import os
import datetime
import random
from random import choice

import pyarrow as pa
import pyarrow.parquet as pq
import faker

fak = faker.Faker()

## Constants

BATCH_SIZE = 5000

DICT_ = {
    'request': ['GET', 'POST', 'PUT', 'DELETE'], 
    'endpoint': ['/usr', '/usr/admin', '/usr/admin/developer', '/usr/login', '/usr/register'], 
    'statuscode': ['303', '404', '500', '403', '502', '304','200'], 
    'username': ['james', 'adam', 'eve', 'alex', 'smith', 'isabella', 'david', 'angela', 'donald', 'hilary'],
    'useragent' : 
    [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0',
        'Chrome/5.0 (Android 10; Mobile; rv:84.0) Gecko/84.0 Firefox/84.0',
        'Edge/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
        'Mozilla/5.0 (Linux; Android 10; ONEPLUS A6000) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Mobile Safari/537.36',
        'Edge/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4380.0 Safari/537.36 Edg/89.0.759.0',
        'Chrome/5.0 (Linux; Android 10; ONEPLUS A6000) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.116 Mobile Safari/537.36 EdgA/45.12.4.5121',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 OPR/73.0.3856.329',
        'Mozilla/5.0 (Linux; Android 10; ONEPLUS A6000) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Mobile Safari/537.36 OPR/61.2.3076.56749',
        'Edge/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A',
        'Chrome/5.0 (iPhone; CPU iPhone OS 12_4_9 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.2 Mobile/15E148 Safari/604.1'
    ],
    'referrer' : ['-',fak.uri()]}

WEATHER_SCHEMA = pa.schema([
        ('ipadresses', pa.string()),
        ('dates', pa.timestamp('s')),
        ('requests', pa.string()),
        ('endpoints', pa.string()),
        ('statuses', pa.string()),
        ('bytes', pa.float32()),
        ('referres', pa.string()),
        ('useragents', pa.string()),
        ('responsestimes', pa.float32())
        ])

## Usefule methods

def random_date():
    """Generate random date - time between 2023 and 2024."""
    year = random.randint(2023, 2024)
    month = random.randint(1, 12)
    day = random.randint(1, 31)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    try:
        date = datetime.datetime(year, month, day, hour, minute, second)
    except ValueError:
        # invalid date
        date = datetime.datetime(year, month, day - 3, hour, minute, second)

    return date


def list_to_parquet(list_: list, type_: type):
    """Create a pyarrow array from a python list."""
    return pa.array(list_, type=type_)


def generate_parquet_log_batch():
    """Generate parquet log batch."""

    ipadresses = []
    dates = []
    requests = []
    endpoints = []
    statuses = []
    bytes_ = []
    referres = []
    useragents = []
    responsestimes = []

    for _ in range(BATCH_SIZE):
        ipadresses.append(fak.ipv4())
        dates.append(random_date())
        requests.append(choice(DICT_['request']))
        endpoints.append(choice(DICT_['endpoint']))
        statuses.append(choice(DICT_['statuscode']))
        bytes_.append(int(random.gauss(5000,50)))
        referres.append(choice(DICT_['referrer']).replace(",", ";"))
        useragents.append(choice(DICT_['useragent']).replace(",", ";"))
        responsestimes.append(random.randint(1,5000))

    columns = [(ipadresses, pa.string()), (dates, pa.timestamp('s')), (requests, pa.string()), (endpoints, pa.string()),
               (statuses, pa.string()), (bytes_, pa.float32()), (referres, pa.string()), (useragents, pa.string()),
               (responsestimes, pa.float32())]

    pa_columns = []

    for col in columns:
        pa_columns.append(list_to_parquet(col[0], col[1]))

    return pa.RecordBatch.from_arrays(pa_columns, names=WEATHER_SCHEMA.names)


def generate_parquet_log_file(batch, name: str):
    """Generate parquet log file."""
    table = pa.Table.from_batches([batch])
    pq.write_table(table, name)


if "__main__" == __name__:

    parquet_filename = f"log_{datetime.datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}.parquet"
    parquet_batch = generate_parquet_log_batch()
    generate_parquet_log_file(parquet_batch, os.path.join("logs", parquet_filename))
