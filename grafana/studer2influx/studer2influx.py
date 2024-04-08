import traceback
from datetime import datetime
from enum import Enum

import schedule
from xcom_proto import XcomP as param
from xcom_proto import XcomLANTCP
from xcom_proto import XcomRS232
from influxdb import InfluxDBClient
from time import sleep
import os
import sys
import logging

class Period(Enum):
    HALF_DAY = 1        # 12 hours
    HOURLY = 2          # 1 hour
    QUARTER = 3         # 15 minutes
    PERIODIC = 4        # every STUDER2INFLUX_PERIODIC_FREQUENCY_SEC seconds
    PERIODIC_10 = 5     # every (STUDER2INFLUX_PERIODIC_FREQUENCY_SEC * 10) seconds

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
    [ param.BATT_SOC, Period.PERIODIC_10 ],
    [ param.BATT_TEMP, Period.PERIODIC_10 ],
    [ param.BATT_CYCLE_PHASE, Period.PERIODIC ],
    [ param.BATT_POWER, Period.PERIODIC_10 ],
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

INFLUX_DB_NAME = os.environ['STUDER2INFLUX_DB_NAME']
SAMPLING_FREQUENCY_SEC = int(os.environ.get('STUDER2INFLUX_PERIODIC_FREQUENCY_SEC'))
DEBUG = os.environ['STUDER2INFLUX_DEBUG']
INFLUXDB_HOST = os.environ['STUDER2INFLUX_INFLUXDB_HOST']
INFLUXDB_PORT = os.environ['STUDER2INFLUX_INFLUXDB_PORT']
INFLUXDB_USERNAME = os.environ['STUDER2INFLUX_INFLUXDB_USERNAME']
INFLUXDB_PASSWORD = os.environ['STUDER2INFLUX_INFLUXDB_PASSWORD']
XCOMLAN_LISTEN_PORT = os.environ.get('STUDER2INFLUX_XCOMLAN_LISTEN_PORT', None)
XCOMRS232_SERIAL_PORT = os.environ.get('STUDER2INFLUX_XCOMRS232_SERIAL_PORT', None)
XCOMRS232_BAUD_RATE = os.environ.get('STUDER2INFLUX_XCOMRS232_BAUD_RATE', None)


logging.basicConfig(
    stream=sys.stdout,
    level=logging.DEBUG if DEBUG == "1" else logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger("Studer2Influx")

log.info("Started")

log.info("Connecting to influxdb")
influxClient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD)
influxClient.create_database(INFLUX_DB_NAME)
influxClient.switch_database(INFLUX_DB_NAME)
log.info("Connected to influxdb")

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
    influxJsonBodies = []
    influxJsonBodies.extend(getValues(xcom, BATTERY_PARAMETERS, [100], periodType, "battery", 100))
    influxJsonBodies.extend(getValues(xcom, XT_PARAMETERS, AVAILABLE_XT_ADDRESSES, periodType, "XT", 100))
    influxJsonBodies.extend(getValues(xcom, VT_PARAMETERS, AVAILABLE_VT_ADDRESSES, periodType, "VT", 300))
    influxJsonBodies.extend(getValues(xcom, VS_PARAMETERS, AVAILABLE_VS_ADDRESSES, periodType, "VS", 700))

    influxJsonBodies = [entry for entry in influxJsonBodies if entry]       # remove empty lists
    influxClient.write_points(influxJsonBodies)

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
            log.info("Reconnect")


main()