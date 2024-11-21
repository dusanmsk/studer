import logging
import threading
import os, sys
from time import sleep
import paho.mqtt.client as mqtt
import json

from questdb.ingress import Sender, TimestampNanos

auto_flush_rows = 200
auto_flush_interval = 5000
progress_interval=60        # 60

err_cnt = 0
processed_cnt = 0
mqtt_client = None

def get_env_var(name, default =None):
    value = os.environ.get(name, default)
    assert value, f"{name} environment variable is not set."
    return value

def getLogLevel():
    level=str(os.getenv('STUDER2QUESTDB_LOGLEVEL', 'info')).lower()
    if level == 'debug':
        return logging.DEBUG
    elif level == 'info':
        return logging.INFO
    elif level == 'warning':
        return logging.WARNING
    elif level == 'error':
        return logging.ERROR
    elif level == 'critical':
        return logging.CRITICAL
    else:
        return logging.INFO

# set logging level
logging.basicConfig(level=getLogLevel(), format='%(asctime)s - %(levelname)s - %(message)s')

# read environment variables
mqtt_host = get_env_var('MQTT_HOST')
mqtt_port = int(get_env_var('MQTT_PORT'))
studer_topic = "studer"

questdb_host = get_env_var('QUESTDB_HOST')
questdb_port = get_env_var('QUESTDB_PORT')
questdb_username = get_env_var('QUESTDB_USERNAME')
questdb_password = get_env_var('QUESTDB_PASSWORD')
questdb_table = get_env_var('QUESTDB_TABLE', 'studer')

def mqtt_on_connect(client, userdata, flags, rc, properties):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(f"{studer_topic}/measurements")
    else:
        logging.error("Failed to connect to MQTT broker")
        sys.exit(1)


def insert_to_questdb(device_name, fields, timestamp):
    global questdb_sender
    questdb_sender.row(
        questdb_table,
        symbols={'device': device_name},
        columns=fields,
        at = timestamp
    )

questdb_sender = None
def mqtt_on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        measurements = json.loads(payload)
        for measurement in measurements:
            device_name = measurement['tags']['deviceName']
            fields = measurement['fields']
            insert_to_questdb(device_name, fields, TimestampNanos.now())
        global processed_cnt
        processed_cnt += 1
    except Exception as e:
        logging.error(f"Error: {e}")
        global err_cnt
        err_cnt = err_cnt + 1

def mqtt_on_disconnect(a, b, c, rc, e):
    logging.error(f"Disconnected from MQTT broker with code {rc}")

def print_progress():
    global processed_cnt, err_cnt
    logging.info(f"Processed {processed_cnt} messages, errors: {err_cnt}")

mqtt_client = None
def connectToMQTTAndLoopForever():
    global mqtt_client
    logging.info(f"Connecting to mqtt at {mqtt_host}:{mqtt_port}")
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=120)
    mqtt_client.on_connect = mqtt_on_connect
    mqtt_client.on_message = mqtt_on_message
    mqtt_client.on_disconnect = mqtt_on_disconnect
    if (getLogLevel() <= logging.INFO):
        mqtt_client.enable_logger()
    mqtt_client.connect(mqtt_host, mqtt_port)
    logging.info("Starting MQTT loop")
    mqtt_client.loop_forever(retry_first_connection=True)

def reporting_handler():
    # Keep the script running
    while True:
        print_progress()
        sleep(progress_interval)


def main():
    logging.info("Starting studer2questdb")
    reporting_thread = threading.Thread(target=reporting_handler)
    reporting_thread.start()

    conf = f'http::addr={questdb_host}:{questdb_port};username={questdb_username};password={questdb_password};auto_flush_rows={auto_flush_rows};auto_flush_interval={auto_flush_interval};'
    with Sender.from_conf(conf) as sender:
        global questdb_sender
        questdb_sender = sender
        connectToMQTTAndLoopForever()

main()
