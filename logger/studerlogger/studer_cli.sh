#!/bin/bash

MQTT_HOST="nas"

RESPONSE_TOPIC="studercli/response"
SUB_PID='None'
subscribe_and_print_response() {
    mosquitto_sub -h $MQTT_HOST -t $RESPONSE_TOPIC -C 1 &
    SUB_PID=$!
}

join() {
    wait $SUB_PID
}

usage() {
    echo "Usage:"
    echo "  $0 get {PARAMETER_NAME} {DEVICE_ADDRESS}"
    echo "  $0 set {PARAMETER_NAME} {VALUE} {DEVICE_ADDRESS}"
    exit 1
}

# Check if the number of arguments is correct
if [ "$#" -lt 3 ]; then
    usage
fi

# Extract the command and parameters
COMMAND=$1
PARAMETER_NAME=$2

if [ "$COMMAND" == "get" ]; then
    if [ "$#" -ne 3 ]; then
      usage
    fi
    DEVICE_ADDRESS=$3
    subscribe_and_print_response
    mosquitto_pub -h $MQTT_HOST -t studercli/value/get -m "$PARAMETER_NAME $DEVICE_ADDRESS"
elif [ "$COMMAND" == "set" ]; then
    if [ "$#" -ne 4 ]; then
      usage
    fi
    VALUE=$3
    DEVICE_ADDRESS=$4
    subscribe_and_print_response
    mosquitto_pub -h $MQTT_HOST -t studercli/value/set -m "$PARAMETER_NAME $VALUE $DEVICE_ADDRESS"
else
    echo "Invalid command. Use 'get' or 'set'."
    exit 1
fi

join