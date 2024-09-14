import logging, time, re, random
from paho.mqtt.enums import CallbackAPIVersion
import paho.mqtt.client as mqtt
from xcom_proto import XcomP

class CliHandler:

    def __init__(self, HOST, PORT, TOPIC, CLIENT_ID="studer_cli", xcomProvider=None):
        self.cliGetValueTopic = "studercli/value/get"
        self.cliSetValueTopic = "studercli/value/set"
        self.cliResponseValueTopic = "studercli/response"
        self.log = logging.getLogger("CliHandler")
        self.host = HOST
        self.port = PORT
        self.topic = TOPIC
        self.client_id = CLIENT_ID + str(random.randint(0, 1000))
        self.client = mqtt.Client(CallbackAPIVersion.VERSION2)
        self.client.connect(self.host, self.port, 60)
        self.client.loop_start()
        self.xcomProvider = xcomProvider
        self.log.info("CliHandler initialized")

        @self.client.connect_fail_callback()
        def on_connect_fail():
            self.log.error("Failed to connect to mqtt")

        @self.client.connect_callback()
        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code == 0:
                self.log.info("Connected")
                client.subscribe(self.cliGetValueTopic)
                client.subscribe(self.cliSetValueTopic)
                self.log.info("CLI handler subscribed")
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
                    value = self.getStuderParameter(parameterName, deviceAddress)
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
                    self.setStuderParameter(parameterName, value, deviceAddress)
                    self.client.publish(self.cliResponseValueTopic, f"Set {payload} OK")
                except Exception as e:
                    self.log.error(e)
                    self.client.publish(self.cliResponseValueTopic, f"Set {payload} ERROR")

        @self.client.publish_callback()
        def on_publish(client, userdata, mid, reason_code, properties):
            pass
            #self.log.debug("Message sent")

        @self.client.disconnect_callback()
        def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
            self.log.debug("Connection lost, reconnecting...")
            time.sleep(5)
            client.reconnect()

    def xcomParamFromName(self, name):
        # try to parse name as a number
        paramId = None
        try:
            paramId = int(name)
        except:
            pass
        # if param name is number
        if paramId is not None:
            return XcomP.getParamByID(paramId)
        # else use param name
        else:
            for datapoint in XcomP._getDatapoints():
                if datapoint.name == name:
                    return datapoint
        return None

    def getStuderParameter(self, parameterName, deviceAddress):
        param = self.xcomParamFromName(parameterName)
        if param:
            with self.xcomProvider:
                xcom = self.xcomProvider.get()
                if xcom:
                    return xcom.getValue(param, int(deviceAddress))


    def setStuderParameter(self, parameterName, value, deviceAddress):
        param = self.xcomParamFromName(parameterName)
        if param:
            with self.xcomProvider:
                xcom = self.xcomProvider.get()
                if xcom:
                    return xcom.setValue(param, value, int(deviceAddress))
