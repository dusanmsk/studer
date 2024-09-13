import logging
import re
from abc import abstractmethod
import socket
import time
import random

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from paho.mqtt.enums import CallbackAPIVersion
import Util


class AbstractMeasurementProcessor():

    @abstractmethod
    def processMeasurements(self, measurements):
        pass

class InfluxDbMeasurementProcessor(AbstractMeasurementProcessor):

    def __init__(self, INFLUXDB_HOST, INFLUXDB_PORT, INFLUXDB_USERNAME, INFLUXDB_PASSWORD, INFLUX_DB_NAME):
        self.log =  logging.getLogger("InfluxDbProcessor")
        self.log.info("Connecting to influxdb")
        self.influxClient = InfluxDBClient(host=INFLUXDB_HOST, port=INFLUXDB_PORT, username=INFLUXDB_USERNAME, password=INFLUXDB_PASSWORD)
        self.influxClient.create_database(INFLUX_DB_NAME)
        self.influxClient.switch_database(INFLUX_DB_NAME)
        self.log.info("Connected to influxdb")

    def processMeasurements(self, measurements):
        self.influxClient.write_points(measurements)
        self.log.debug(f"Written to influx: {measurements}")


class UdpMeasurementProcessor(AbstractMeasurementProcessor):

    def __init__(self, HOST, PORT, DELIMITER="\n"):
        self.log =  logging.getLogger("UdpProcessor")
        self.host = HOST
        self.port = PORT
        self.delimiter = DELIMITER
    def processMeasurements(self, measurements):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for m in measurements:
            deviceName = m['tags']['deviceName']
            fields = m['fields']
            for f in fields:
                measurementName = f
                measurementValue = fields[f]
                message = f"{deviceName}_{measurementName}={measurementValue}{self.delimiter}".lower().encode('ascii')
                sock.sendto(message, (self.host, self.port))
        sock.close()



class MqttMeasurementProcessor(AbstractMeasurementProcessor):

    def __init__(self, HOST, PORT, TOPIC, CLIENT_ID=""):
        self.cliGetValueTopic = "studercli/value/get"
        self.cliSetValueTopic = "studercli/value/set"
        self.cliResponseValueTopic = "studercli/response"
        self.log = logging.getLogger("MqttProcessor")
        self.host = HOST
        self.port = PORT
        self.topic = TOPIC
        self.client_id = CLIENT_ID + str(random.randint(0, 1000))
        self.client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self.client.connect(self.host, self.port, 60)
        self.client.loop_start()
        self.xcomProvider = None
        self.log.info("MqttMeasurementProcessor initialized")

        @self.client.connect_fail_callback()
        def on_connect_fail():
            self.log.error("Failed to connect to mqtt")

        @self.client.connect_callback()
        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code == 0:
                self.log.info("Connected")
                client.subscribe(self.cliGetValueTopic)
                client.subscribe(self.cliSetValueTopic)
                self.log.info("CLI subscribed")
            else:
                self.log.error(f"Failed to connect. Reason code: {reason_code}")

        @self.client.message_callback()
        def on_message(client, userdata, message):
            topic = message.topic
            payload = message.payload.decode('utf-8')
            if topic == self.cliGetValueTopic and self.xcomProvider is not None:
                try:
                    splt = payload.split(" ")
                    parameterName = splt[0]
                    deviceAddress = splt[1]
                    value = Util.getStuderParameter(self.xcomProvider, parameterName, deviceAddress)
                    self.client.publish(self.cliResponseValueTopic, value)
                except Exception as e:
                    self.log.error(e)
                    self.client.publish(self.cliResponseValueTopic, "ERROR")
            elif topic == self.cliSetValueTopic and self.xcomProvider is not None:
                try:
                    splt = payload.split(" ")
                    parameterName = splt[0]
                    value = splt[1]
                    deviceAddress = splt[2]
                    Util.setStuderParameter(self.xcomProvider, parameterName, value, deviceAddress)
                    self.client.publish(self.cliResponseValueTopic, f"Set {payload} OK")
                except Exception as e:
                    self.log.error(e)
                    self.client.publish(self.cliResponseValueTopic, f"Set {payload} ERROR")

        @self.client.publish_callback()
        def on_publish(client, userdata, mid, reason_code, properties):
            self.log.debug("Message sent")

        @self.client.disconnect_callback()
        def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
            self.log.debug("Connection lost, reconnecting...")
            time.sleep(5)
            client.reconnect()

    def fixTopic(self, x):
        return re.sub(r'[^a-zA-Z0-9]', '_', x)

    def createTopicName(self, topicPrefix, deviceName, measurementName):
        topic= f"{topicPrefix}/{self.fixTopic(deviceName)}/{self.fixTopic(measurementName)}".replace("__", "_").replace("//", "/").lower()
        return topic

    def processMeasurements(self, measurements):
        for m in measurements:
            deviceName = m['tags']['deviceName']
            fields = m['fields']
            for f in fields:
                measurementName = f
                measurementValue = fields[f]
                topic= self.createTopicName(self.topic, deviceName, measurementName)
                logging.debug(f"Publishing to {topic}: {measurementValue}")
                self.client.publish(topic, measurementValue)

    def setXcomProvider(self, xcomProvider):
        self.xcomProvider = xcomProvider


class LoggingMeasurementProcessor(AbstractMeasurementProcessor):

    def __init__(self):
        self.log =  logging.getLogger("LoggingProcessor")

    def processMeasurements(self, measurements):
        self.log.info(f"Logging measurements: {measurements}")
