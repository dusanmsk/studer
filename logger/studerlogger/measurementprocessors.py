import json
import logging
import re
from abc import abstractmethod
import socket
import time
import random

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient
from paho.mqtt.enums import CallbackAPIVersion


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
        self.log = logging.getLogger("MqttProcessor")
        self.host = HOST
        self.port = PORT
        self.topic = TOPIC
        self.client_id = CLIENT_ID + str(random.randint(0, 1000))
        self.client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        self.client.on_disconnect = self.on_disconnect
        self.client.connect(self.host, self.port, 60)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            self.log.info("Connected")
        else:
            self.log.error(f"Failed to connect. Reason code: {reason_code}")

    def on_publish(self, client, userdata, mid, reason_code, properties):
        self.log.debug("Message sent")

    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
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
            # send measurements as individual messages
            for f in fields:
                measurementName = f
                measurementValue = fields[f]
                topic = self.createTopicName(self.topic, deviceName, measurementName)
                logging.debug(f"Publishing to {topic}: {measurementValue}")
                self.client.publish(topic, measurementValue)
        # send measurements in one message
        self.client.publish(f"{self.topic}/measurements", json.dumps(measurements))


class LoggingMeasurementProcessor(AbstractMeasurementProcessor):

    def __init__(self):
        self.log =  logging.getLogger("LoggingProcessor")

    def processMeasurements(self, measurements):
        self.log.info(f"Logging measurements: {measurements}")
