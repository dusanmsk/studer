import traceback
from datetime import datetime

from xcom_proto import XcomP as param
from xcom_proto import XcomLANTCP
from xcom_proto import XcomRS232
from influxdb import InfluxDBClient
from time import sleep
import os
import sys
import logging

# list of parameters to be read from devices
# DAILY parameters are read every hour
# PERIODIC parameters are read every STUDER2INFLUX_SAMPLING_FREQUENCY_SEC seconds

XT_PARAMETERS_HOURLY = [
    param.AC_ENERGY_IN_PREV_DAY,
    param.AC_ENERGY_OUT_PREV_DAY,
]
XT_PARAMETERS_PERIODIC = [
    param.AC_ENERGY_IN_CURR_DAY,
    param.AC_ENERGY_OUT_CURR_DAY,
    param.AC_FREQ_IN,
    param.AC_FREQ_OUT,
    param.AC_POWER_IN,
    param.AC_POWER_OUT,
    param.AC_VOLTAGE_IN,
    param.AC_VOLTAGE_OUT,
    param.AC_CURRENT_IN,
    param.AC_CURRENT_OUT,
    param.BATT_CYCLE_PHASE_XT
]

BATTERY_PARAMETERS_HOURLY = [
    param.BATT_CHARGE_PREV_DAY,
    param.BATT_DISCHARGE_PREV_DAY
]
BATTERY_PARAMETERS_PERIODIC = [
    param.BATT_VOLTAGE,
    param.BATT_CURRENT,
    param.BATT_SOC,
    param.BATT_TEMP,
    param.BATT_CYCLE_PHASE,
    param.BATT_POWER,
    param.BATT_CHARGE,
    param.BATT_DISCHARGE
]

VT_PARAMETERS_HOURLY = [
    param.PV_ENERGY_PREV_DAY,
    param.PV_SUN_HOURS_PREV_DAY
]
VT_PARAMETERS_PERIODIC = [
    param.PV_VOLTAGE,
    param.PV_POWER,
    param.PV_ENERGY_CURR_DAY,
    param.PV_ENERGY_TOTAL,
    param.PV_SUN_HOURS_CURR_DAY
    #param.PV_OPERATION_MODE,
    #param.PV_NEXT_EQUAL
]

VS_PARAMETERS_HOURLY = [
    param.VS_PV_ENERGY_PREV_DAY
]
VS_PARAMETERS_PERIODIC = [
    param.VS_PV_POWER,
    param.VS_PV_PROD
]

AVAILABLE_VT_ADDRESSES = []
AVAILABLE_VS_ADDRESSES = []
AVAILABLE_XT_ADDRESSES = []

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

lastReportedHour = -1
def shouldReportHourly():
    global lastReportedHour
    now = datetime.now()
    return now.hour != lastReportedHour

def rememberReportHourly():
    global lastReportedHour
    now = datetime.now()
    lastReportedHour = now.hour

def findDevices(xcom):
    """
    This function finds all available devices and updates the global lists of device addresses.

    Parameters:
    xcom (object): An instance of the Xcom class, used to communicate with the devices.

    Global Variables:
    AVAILABLE_VT_ADDRESSES (list): A list of available VT device addresses.
    AVAILABLE_XT_ADDRESSES (list): A list of available XT device addresses.
    AVAILABLE_VS_ADDRESSES (list): A list of available VS device addresses.

    The function tries to get a value from each device in the range of addresses for each type of device (XT, VT, VS).
    If the getValue operation is successful, the device is considered available and its address is added to the corresponding list.
    """
    global AVAILABLE_VT_ADDRESSES
    global AVAILABLE_XT_ADDRESSES
    global AVAILABLE_VS_ADDRESSES
    AVAILABLE_VT_ADDRESSES = []
    AVAILABLE_XT_ADDRESSES = []
    AVAILABLE_VS_ADDRESSES = []

    # find all xtm/xth devices
    for i in range(101,109):
        try:
            xcom.getValue(param.AC_CURRENT_OUT, i)
            log.info(f"Found XTM/H/S device at address {i}")
            AVAILABLE_XT_ADDRESSES.append(i)
        except:
            log.debug(f"Device {i} not found")
    # find all vt devices
    for i in range(301,315):
        try:
            xcom.getValue(param.PV_POWER, i)
            log.info(f"Found VT device at address {i}")
            AVAILABLE_VT_ADDRESSES.append(i)
        except:
            log.debug(f"Device {i} not found")
    # find all vs devices
    for i in range(701,715):
        try:
            xcom.getValue(param.PV_POWER, i)
            log.info(f"Found VS device at address {i}")
            AVAILABLE_VS_ADDRESSES.append(i)
        except:
            log.debug(f"Device {i} not found")


def getValues(xcom, parameterList, deviceAddresses, deviceName, deviceAddressMask):

    json_bodies = []
    for deviceAddress in deviceAddresses:
        measurements = {}
        for p in parameterList:
            try:
                value = xcom.getValue(p, deviceAddress)
                name = p.name
                measurements[name] = value
            except:
                log.error(f"Failed to get value {p.name} for {deviceAddress}")

        if measurements:
            deviceIndex = deviceAddress - deviceAddressMask
            json_body = [
                {
                    "measurement": "solar_data",
                    "tags": { "deviceName" : f"{deviceName}-{deviceIndex}" },
                    "fields": measurements
                }
            ]
            json_bodies.extend(json_body)

    return json_bodies


def logProgress(successRounds):
    # each 20-th measurement log at info that everything is ok
    if(successRounds % 20 == 0):
        log.info(f"Successfully processed {successRounds} rounds")
    if(successRounds == 1):
        log.info(f"Started receiving studer data")


def studer2influx():
    while (True):
        with XcomLANTCP(port=int(XCOMLAN_LISTEN_PORT)) if XCOMLAN_LISTEN_PORT else XcomRS232(serialDevice=XCOMRS232_SERIAL_PORT, baudrate=int(XCOMRS232_BAUD_RATE)) as xcom:
            findDevices(xcom)
            successRounds = 0
            influxJsonBodies = []
            while True:
                try:
                    influxJsonBodies.extend(getValues(xcom, BATTERY_PARAMETERS_PERIODIC, [100], "battery", 100))
                    influxJsonBodies.extend(getValues(xcom, XT_PARAMETERS_PERIODIC, AVAILABLE_XT_ADDRESSES, "XT", 100))
                    influxJsonBodies.extend(getValues(xcom, VT_PARAMETERS_PERIODIC, AVAILABLE_VT_ADDRESSES, "VT", 300))
                    influxJsonBodies.extend(getValues(xcom, VS_PARAMETERS_PERIODIC, AVAILABLE_VS_ADDRESSES, "VS", 700))
                    if(shouldReportHourly()):
                        influxJsonBodies.extend(getValues(xcom, BATTERY_PARAMETERS_HOURLY, [100], "battery", 100))
                        influxJsonBodies.extend(getValues(xcom, XT_PARAMETERS_HOURLY, AVAILABLE_XT_ADDRESSES, "XT", 100))
                        influxJsonBodies.extend(getValues(xcom, VT_PARAMETERS_HOURLY, AVAILABLE_VT_ADDRESSES, "VT", 300))
                        influxJsonBodies.extend(getValues(xcom, VS_PARAMETERS_HOURLY, AVAILABLE_VS_ADDRESSES, "VS", 700))
                        rememberReportHourly()

                    influxJsonBodies = [entry for entry in influxJsonBodies if entry]       # remove empty lists
                    influxClient.write_points(influxJsonBodies)
                    successRounds += 1
                    logProgress(successRounds)
                except Exception as e:
                    log.error(f"Exception thrown: {e}")
                    log.error(traceback.format_exc())
                log.debug(f"Sleeping {SAMPLING_FREQUENCY_SEC} seconds")
                sleep(SAMPLING_FREQUENCY_SEC)
            log.info("Reconnect")


studer2influx()