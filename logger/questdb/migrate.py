import concurrent.futures
import datetime
import logging
import os

import requests
from influxdb import InfluxDBClient
from questdb.ingress import TimestampNanos
from tqdm import tqdm
import questdb.ingress

parallel_jobs = int(os.cpu_count() / 2)
batch_size = 10000

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

influx_client = None
main_progressbar = None
do_shutdown = False


def get_influx_count(measurement, where=None):
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
    try:
        questdb_oldest_timestamp = get_questdb_oldest_timestamp(measurement)
        offset = 0
        influx_time_where = f"time < {to_epoch(questdb_oldest_timestamp)}"
        total_rows = get_influx_count(measurement, influx_time_where)
        pbar = tqdm(total=total_rows, desc=measurement, leave=False)
        while True:
            global do_shutdown
            if do_shutdown:
                break
            query = f"SELECT * FROM {measurement} WHERE {influx_time_where} ORDER BY time DESC LIMIT {batch_size} OFFSET {offset}"
            results = influx_client.query(query, epoch='ns')
            points = list(results.get_points())
            if not points:
                break
            insert_to_questdb(measurement, points)
            pbar.update(len(points))
            offset += batch_size

        pbar.close()

    except Exception as e:
        # todo log to file
        print(f"Failed to export {measurement}: {e}")

    finally:
        global main_progressbar
        main_progressbar.update(1)


def main():

    global influx_client

    influx_client = InfluxDBClient(host=influx_host, port=influx_port, username=influx_user, password=influx_password)
    influx_client.switch_database(influx_db)


    do_export('solar_data')

    influx_client.close()
    print("Migration done")


if __name__ == "__main__":
    main()
