import concurrent.futures
import datetime
import gc
import logging
import os

import requests
from influxdb import InfluxDBClient
from questdb.ingress import TimestampNanos
from tqdm import tqdm
import questdb.ingress

parallel_jobs = int(os.cpu_count() / 2)
batch_size = 50000

# Nastavenie InfluxDB klienta
influx_host = "localhost"
influx_port = 8086
influx_db = "studer"
influx_user = "studer"  # Nastavte, ak máte používateľa
influx_password = "studer"  # Nastavte, ak máte heslo

questdb_host = "localhost"
questdb_port = 9000
questdb_username = "admin"
questdb_password = "quest"
auto_flush_rows = 1000
auto_flush_interval = 300000
questdb_studer_table = "studer"

main_progressbar = None
do_shutdown = False

def create_influx_client():
    global influx_host, influx_port, influx_user, influx_password, influx_db
    influx_client = InfluxDBClient(host=influx_host, port=influx_port, username=influx_user, password=influx_password)
    influx_client.switch_database(influx_db)
    return influx_client

def get_influx_count(measurement, where=None):
    with create_influx_client() as influx_client:
        try:
            query = f"SELECT COUNT(*) FROM {measurement}" + f" WHERE {where}" if where else ""
            results = influx_client.query(query)
            points = list(results.get_points())
            if not points:
                return 0
            integer_values = [value for value in points[0].values() if isinstance(value, int)]
            max_value = max(integer_values, default=None)
            return max_value
        except Exception as e:
            print(f"Failed to get count for {measurement}: {e}")
            return 0


def get_questdb_oldest_timestamp(measurement):
    query = f"SELECT min(timestamp) FROM {measurement}"
    url = f"http://{questdb_host}:{questdb_port}/exec"
    response = requests.get(url, params={"query": query})
    date = datetime.datetime.now()
    if response.status_code == 200:
        results = response.json()
        if results['dataset']:
            date_str = results['dataset'][0][0]
            date = parse_timestamp(date_str)
    return date


def to_epoch(dt):
    return int(dt.timestamp() * 1_000_000_000)

def parse_timestamp(ts):
    date = None
    if '.' in ts:
        date = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S.%fZ")
    else:
        date = datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
    return date.replace(tzinfo=datetime.timezone.utc)

def split_into_chunks(array, chunk_size):
    return [array[i:i + chunk_size] for i in range(0, len(array), chunk_size)]


conf = f'http::addr={questdb_host}:{questdb_port};username={questdb_username};password={questdb_password};auto_flush_rows={auto_flush_rows};auto_flush_interval={auto_flush_interval};'
def insert_chunk_into_questdb(measurement_name, chunk):
    try:
        global questdb_studer_table
        table_name = f"{questdb_studer_table}"
        with questdb.ingress.Sender.from_conf(conf) as sender:
            for row in chunk:
                ts = row['time']
                del row['time']
                device_name = row['deviceName']
                del row['deviceName']
                sender.row(
                    table_name,
                    symbols={'device': device_name},
                    columns=row,
                    at=TimestampNanos(ts)
                )
            sender.flush()
    except Exception as e:
        logging.error(f"Failed to insert chunk into QuestDB: {e}")


def insert_to_questdb(measurement_name, data):
    num_parallalel = 5
    chunks = split_into_chunks(data, int(batch_size/num_parallalel))
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_parallalel) as executor:
        futures = [executor.submit(insert_chunk_into_questdb, measurement_name, chunk) for chunk in chunks]
        concurrent.futures.wait(futures)



def do_export(measurement):
    global questdb_studer_table
    with create_influx_client() as influx_client:
        try:
            timestamp_before = to_epoch(get_questdb_oldest_timestamp(questdb_studer_table))
            total_rows = get_influx_count(measurement, f"time < {timestamp_before}")
            pbar = tqdm(total=total_rows, desc=measurement, leave=False)
            lowest_timestamp = timestamp_before
            while True:
                global do_shutdown
                if do_shutdown:
                    break
                query = f"SELECT * FROM {measurement} WHERE time < {lowest_timestamp} ORDER BY time DESC LIMIT {batch_size}"
                results = influx_client.query(query, epoch='ns')
                points = list(results.get_points())
                if not points:
                    break
                lowest_timestamp = points[-1]['time']
                insert_to_questdb(measurement, points)
                pbar.update(len(points))
            pbar.close()

        except Exception as e:
            # todo log to file
            print(f"Failed to export {measurement}: {e}")

def main():
    do_export('solar_data')
    print("Migration done")

if __name__ == "__main__":
    main()
