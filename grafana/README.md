# Grafana for studer

This docker-compose suite is used to collect data from Studer into Influx/Grafana.

## How to setup

- install docker and docker-compose on a linux computer (rpi (see notes for rpi), virtual, ...) where you want to run this instance
- clone/[download](https://github.com/dusanmsk/studer/archive/refs/heads/main.zip) this repo there
- edit .env file and modify DATADIR, STUDER2INFLUX_PERIODIC_FREQUENCY_SEC etc
- run ./rebuild.sh

### Setup with XCOM-LAN

Open MOXA in web browser (default password should be 'xcomlan'), go to "Operating settings", "Port 1" and set "Destination IP address 2" to computer where docker-compose will run.
![Image](docs/images/moxa_setup.png)

### Setup with XCOM-RS232
TBD, not implemented yet
Connect computer serial port with xcom-rs232, then "TODO binding serial port to container etc ..."

### Run docker-compose

Enable debugging for a while (edit .env then set DEBUG=1). Now run docker in foreground `./run.sh` and watch for logs. You should see something like:

`Written to influx: [{'measurement': 'solar_data', 'tags': {'deviceName': 'battery-0'}, 'fields': {'BATT_SOC': 99.0, 'BATT_TEMP': 26.296875,...`

That means that data are successfully written into influx. Stop (CTRL+C) then disable debug and run again in background `./run.sh -d`

### Setup grafana

Go to http://YOUR_PC:3000, login as admin:admin, configure new password. Configure new influx datasource (masked password on screenshot is 'grafana'):
![Image](docs/images/datasource.png)

Go to Dashboards, click on arrow on "New", select import, then upload json file from this repo (sample_dashboard.json). 

# Done


# Notes

### RPi

It is strongly recommended to use RPi or similar device with ssd disk. SD card will be quickly destroyed by influxdb writes.