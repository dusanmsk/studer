from xcom_proto import XcomP as param
from xcom_proto import XcomLANTCP
from xcom_proto import XcomRS232
from influxdb import InfluxDBClient
from time import sleep
import os
import sys
import logging

parameters = [
    param.PV_POWER,
    param.PV_VOLTAGE,
    param.PV_ENERGY_CURR_DAY,
    param.PV_ENERGY_TOTAL,
    param.PV_SUN_HOURS_CURR_DAY,
    param.BATT_SOC,
    param.BATT_CYCLE_PHASE,
    param.BATT_CURRENT,
    param.BATTERY_CHARGE_CURR,
    param.BATT_VOLTAGE,
    param.BATT_TEMP,
    param.BATT_CHARGE,
    param.BATT_DISCHARGE,
    param.AC_ENERGY_IN_CURR_DAY,
    param.AC_ENERGY_OUT_CURR_DAY,
    param.AC_FREQ_IN,
    param.AC_FREQ_OUT,
    param.AC_POWER_IN,
    param.AC_POWER_OUT,
    param.AC_VOLTAGE_IN,
    param.AC_VOLTAGE_OUT,
    param.AC_CURRENT_IN,
    param.AC_CURRENT_OUT
]

INFLUX_DB_NAME = os.environ['STUDER2INFLUX_DB_NAME']
SAMPLING_FREQUENCY_SEC = int(os.environ.get('STUDER2INFLUX_SAMPLING_FREQUENCY_SEC', '30'))
DEBUG = os.environ['STUDER2INFLUX_DEBUG']
INFLUXDB_HOST = os.environ['STUDER2INFLUX_INFLUXDB_HOST']
INFLUXDB_PORT = os.environ['STUDER2INFLUX_INFLUXDB_PORT']
INFLUXDB_USERNAME = os.environ['STUDER2INFLUX_INFLUXDB_USERNAME']
INFLUXDB_PASSWORD = os.environ['STUDER2INFLUX_INFLUXDB_PASSWORD']
XCOMLAN_LISTEN_PORT = os.environ.get('STUDER2INFLUX_XCOMLAN_LISTEN_PORT', None)
XCOMRS232_SERIAL_PORT = os.environ.get('STUDER2INFLUX_XCOMRS232_SERIAL_PORT', None)
XCOMRS232_BAUD_RATE = os.environ.get('STUDER2INFLUX_XCOMRS232_BAUD_RATE', None)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG if DEBUG == "1" else logging.INFO)
log = logging.getLogger("Studer2Influx")

log.info("Started")

log.info("Connecting to influxdb")
influxClient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD)
influxClient.create_database(INFLUX_DB_NAME)
influxClient.switch_database(INFLUX_DB_NAME)
log.info("Connected to influxdb")

while (True):
    with XcomLANTCP(port=int(XCOMLAN_LISTEN_PORT)) if XCOMLAN_LISTEN_PORT else XcomRS232(serialDevice=XCOMRS232_SERIAL_PORT, baudrate=int(XCOMRS232_BAUD_RATE)) as xcom:
        successRounds = 0
        while True:
            try:
                influxValues = {}
                for p in parameters:
                    value = xcom.getValue(p)
                    influxValues[p.name] = value
                json_body = [
                    {
                        "measurement": "solar_data",
                        "tags": {},
                        "fields": influxValues
                    }
                ]
                influxClient.write_points(json_body)
                log.debug(f"Written {influxValues}")
                successRounds += 1
                # each 20-th measurement log at info that everything is ok
                if(successRounds % 20 == 0):
                    log.info(f"Successfully processed {successRounds} rounds")
            except Exception as e:
                log.error(f"Exception thrown: {e}")
            log.debug(f"Sleeping {SAMPLING_FREQUENCY_SEC} seconds")
            sleep(SAMPLING_FREQUENCY_SEC)
        log.info("Reconnect")
