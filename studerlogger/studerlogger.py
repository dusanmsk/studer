import socket
import threading
import traceback
from enum import Enum

import schedule
from xcom_proto import XcomP as param
from xcom_proto import XcomLANTCP
from xcom_proto import XcomRS232
from time import sleep, time
import os
import sys
import logging

from measurementprocessors import InfluxDbMeasurementProcessor, UdpMeasurementProcessor, LoggingMeasurementProcessor, MqttMeasurementProcessor, \
    QuestDbMeasurementProcessor


class Period(Enum):
    HALF_DAY = 1        # 12 hours
    HOURLY = 2          # 1 hour
    QUARTER = 3         # 15 minutes
    PERIODIC = 4        # every PERIODIC_FREQUENCY_SEC seconds
    PERIODIC_10 = 5     # every (PERIODIC_FREQUENCY_SEC * 10) seconds

# list of parameters to be read from devices
XT_PARAMETERS = [
    [ param.AC_ENERGY_IN_PREV_DAY, Period.HOURLY ],
    [ param.AC_ENERGY_OUT_PREV_DAY, Period.HOURLY ],
    [ param.AC_ENERGY_IN_CURR_DAY, Period.QUARTER ],
    [ param.AC_ENERGY_OUT_CURR_DAY, Period.QUARTER ],
    [ param.AC_FREQ_IN, Period.PERIODIC ],
    [ param.AC_FREQ_OUT, Period.PERIODIC ],
    [ param.AC_POWER_IN, Period.PERIODIC ],
    [ param.AC_POWER_OUT, Period.PERIODIC ],
    [ param.AC_VOLTAGE_IN, Period.PERIODIC ],
    [ param.AC_VOLTAGE_OUT, Period.PERIODIC ],
    [ param.AC_CURRENT_IN, Period.PERIODIC ],
    [ param.AC_CURRENT_OUT, Period.PERIODIC ],
    [ param.BATT_CYCLE_PHASE_XT, Period.PERIODIC ]
]

BATTERY_PARAMETERS = [
    [ param.BATT_CHARGE_PREV_DAY, Period.HOURLY ],
    [ param.BATT_DISCHARGE_PREV_DAY, Period.HOURLY ],
    [ param.BATT_VOLTAGE, Period.PERIODIC ],
    [ param.BATT_CURRENT, Period.PERIODIC ],
    [ param.BATT_POWER, Period.PERIODIC ],
    [ param.BATT_SOC, Period.PERIODIC_10 ],
    [ param.BATT_TEMP, Period.PERIODIC_10 ],
    [ param.BATT_CYCLE_PHASE, Period.PERIODIC ],
    [ param.BATT_CHARGE, Period.PERIODIC_10 ],
    [ param.BATT_DISCHARGE, Period.PERIODIC_10 ]
]

VT_PARAMETERS = [
    [ param.PV_ENERGY_PREV_DAY, Period.HOURLY ],
    [ param.PV_SUN_HOURS_PREV_DAY, Period.HOURLY ],
    [ param.PV_VOLTAGE, Period.PERIODIC ],
    [ param.PV_POWER, Period.PERIODIC ],
    [ param.PV_ENERGY_CURR_DAY,  Period.QUARTER ],
    [ param.PV_ENERGY_TOTAL, Period.QUARTER ],
    [ param.PV_SUN_HOURS_CURR_DAY, Period.HOURLY ],
    #param.PV_OPERATION_MODE,
    #param.PV_NEXT_EQUAL
]

VS_PARAMETERS = [
    [ param.VS_PV_ENERGY_PREV_DAY, Period.HOURLY ],
    [ param.VS_PV_POWER, Period.PERIODIC ],
    [ param.VS_PV_PROD, Period.PERIODIC ]
]

AVAILABLE_VT_ADDRESSES = []
AVAILABLE_VS_ADDRESSES = []
AVAILABLE_XT_ADDRESSES = []

SAMPLING_FREQUENCY_SEC = int(os.environ.get('PERIODIC_FREQUENCY_SEC'))
EXIT_AFTER_FAILED_READS = int(os.environ.get('EXIT_AFTER_FAILED_READS'))

DEBUG = os.environ.get('DEBUG')

# influxdb properties
INFLUXDB_HOST = os.environ.get('INFLUXDB_HOST')
INFLUXDB_PORT = os.environ.get('INFLUXDB_PORT')
INFLUXDB_USERNAME = os.environ.get('INFLUXDB_USERNAME')
INFLUXDB_PASSWORD = os.environ.get('INFLUXDB_PASSWORD')
INFLUX_DB_NAME = os.environ.get('INFLUX_DB_NAME')

# udp properties
UDP_HOST = os.environ.get('UDP_HOST')
UDP_PORT = int(os.environ.get('UDP_PORT', -1))
UDP_DELIMITER = os.environ.get('UDP_DELIMITER', '\n')

MQTT_HOST = os.environ.get('MQTT_HOST')
MQTT_PORT = int(os.environ.get('MQTT_PORT', -1))
MQTT_TOPIC = os.environ.get('MQTT_TOPIC')
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'studer')

QUESTDB_HOST = os.environ.get('QUESTDB_HOST')
QUESTDB_PORT = os.environ.get('QUESTDB_PORT')
QUESTDB_USERNAME = os.environ.get('QUESTDB_USERNAME')
QUESTDB_PASSWORD = os.environ.get('QUESTDB_PASSWORD')
QUESTDB_TABLE = os.environ.get('QUESTDB_TABLE', 'studer')

# xcom properties
XCOMLAN_LISTEN_PORT = os.environ.get('XCOMLAN_LISTEN_PORT')
XCOMLAN_SOCKET_TIMEOUT = int(os.environ.get('XCOMLAN_SOCKET_TIMEOUT'))
XCOMRS232_SERIAL_PORT = os.environ.get('XCOMRS232_SERIAL_PORT')
XCOMRS232_BAUD_RATE = os.environ.get('XCOMRS232_BAUD_RATE')

LOG_PAYLOADS = os.environ.get('LOG_PAYLOADS', 0)


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG if DEBUG == "1" else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("studerlogger")
log.info("Started")

last_successful_operation = time()


measurementProcessors = []
if INFLUXDB_HOST and INFLUXDB_PORT:
    measurementProcessors.append(InfluxDbMeasurementProcessor(INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUX_DB_NAME))
if UDP_HOST and UDP_PORT:
    measurementProcessors.append(UdpMeasurementProcessor(UDP_HOST, UDP_PORT, UDP_DELIMITER))
if MQTT_HOST and MQTT_PORT and MQTT_TOPIC:
    measurementProcessors.append(MqttMeasurementProcessor(MQTT_HOST, MQTT_PORT, MQTT_TOPIC, MQTT_CLIENT_ID))
if QUESTDB_HOST and QUESTDB_PORT:
    measurementProcessors.append(QuestDbMeasurementProcessor(QUESTDB_HOST, QUESTDB_PORT, QUESTDB_USERNAME, QUESTDB_PASSWORD, QUESTDB_TABLE))
if LOG_PAYLOADS == 1:
    measurementProcessors.append(LoggingMeasurementProcessor())

log.info("Registered measurement processors: %s", ", ".join([type(mp).__name__ for mp in measurementProcessors]))

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


def getValues(xcom, parameterList, deviceAddresses, periodType, deviceName, deviceAddressMask):
    """
       This function retrieves values of specified parameters from devices and prepares them for storage in InfluxDB.

       Parameters:
       xcom (object): An instance of the Xcom class, used to communicate with the devices.
       parameterList (list): A list of parameters to be read from the devices. Each parameter is represented as a list where the first element is the parameter and the second element is the type of period.
       deviceAddresses (list): A list of device addresses from which the parameters are to be read.
       periodType (Period): The type of period for which the parameters are to be read.
       deviceName (str): Device name that will be represented in influx json
       deviceAddressMask (int): The mask value to be subtracted from the device address when reporting device index (for example querying devices 301, 302, 303, so deviceAddressMask is 300 and devices will be reported as 1,2,3).

       Returns:
       json_bodies (list): A list of JSON objects ready to be stored in InfluxDB. Each JSON object contains the measurement name, tags, and fields. The measurement name is set to "solar_data", tags contain the device name, and fields contain the measurements.
       """
    json_bodies = []
    for deviceAddress in deviceAddresses:
        measurements = {}
        for i in parameterList:
            try:
                param = i[0]
                period = i[1]
                name = param.name
                if period == periodType:
                    value = xcom.getValue(param, deviceAddress)
                    measurements[name] = value
            except socket.timeout:      # on socket timeout, do not wait for timeout on every parameter and fail fast
                log.error(f"Failed to get value {name} for device at address {deviceAddress}, socket timeout")
                return []
            except:
                log.error(f"Failed to get value {name} for device at address {deviceAddress}")

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
        log.info(f"Successfully processed {successRounds} periods")
    if(successRounds == 1):
        log.info(f"Started receiving studer data")

def readParameters(xcom, periodType):
    global last_successful_operation
    influxJsonBodies = []
    influxJsonBodies.extend(getValues(xcom, BATTERY_PARAMETERS, [100], periodType, "battery", 100))
    influxJsonBodies.extend(getValues(xcom, XT_PARAMETERS, AVAILABLE_XT_ADDRESSES, periodType, "XT", 100))
    influxJsonBodies.extend(getValues(xcom, VT_PARAMETERS, AVAILABLE_VT_ADDRESSES, periodType, "VT", 300))
    influxJsonBodies.extend(getValues(xcom, VS_PARAMETERS, AVAILABLE_VS_ADDRESSES, periodType, "VS", 700))

    influxJsonBodies = [entry for entry in influxJsonBodies if entry]       # remove empty lists
    if (influxJsonBodies):      # distribute data in threads, do not wait for all threads to finish (to prevent blocking other processors for example with dead mqtt server etc...)
        for measurementProcessor in measurementProcessors:
            thread = threading.Thread(target=measurementProcessor.processMeasurements, args=(influxJsonBodies,))
            thread.start()
        last_successful_operation = time()

def process15min(xcom):
    log.info("Processing 15 minutes parameters")
    readParameters(xcom, Period.QUARTER)

def processHourly(xcom):
    log.info("Processing hourly parameters")
    readParameters(xcom, Period.HOURLY)

def processHalfDay(xcom):
    log.info("Processing half day parameters")
    readParameters(xcom, Period.HALF_DAY)

def main():
    socket.setdefaulttimeout(XCOMLAN_SOCKET_TIMEOUT)
    with XcomLANTCP(port=int(XCOMLAN_LISTEN_PORT)) if XCOMLAN_LISTEN_PORT else XcomRS232(serialDevice=XCOMRS232_SERIAL_PORT, baudrate=int(XCOMRS232_BAUD_RATE)) as xcom:
        schedule.every(15).minutes.do(process15min, xcom=xcom)
        schedule.every(1).hours.do(processHourly, xcom=xcom)
        schedule.every(12).hours.do(processHalfDay, xcom=xcom)
        while (True):
            findDevices(xcom)
            successRounds = 0
            while True:
                try:
                    readParameters(xcom, Period.PERIODIC)
                    # when started, process all scheduled tasks
                    if(successRounds == 0):
                        schedule.run_all()
                    if(successRounds % 10 == 0):
                        readParameters(xcom, Period.PERIODIC_10)
                    schedule.run_pending()
                    successRounds += 1
                    logProgress(successRounds)
                except Exception as e:
                    log.error(f"Exception thrown: {e}")
                    log.error(traceback.format_exc())
                log.debug(f"Sleeping {SAMPLING_FREQUENCY_SEC} seconds")
                sleep(SAMPLING_FREQUENCY_SEC)
                if time() - last_successful_operation > EXIT_AFTER_FAILED_READS * SAMPLING_FREQUENCY_SEC:
                    log.error(f"No successful operation in the last {EXIT_AFTER_FAILED_READS} rounds, exiting in 30 seconds (to be restarted by supervisor)")
                    sleep(30)
                    sys.exit(1)
            log.info("Reconnect")


main()