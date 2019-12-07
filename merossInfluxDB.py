#!/usr/bin/python3

import sys
import json
import time
from datetime import datetime

from meross_iot.manager import MerossManager
from meross_iot.meross_event import MerossEventType
from meross_iot.cloud.devices.power_plugs import GenericPlug

#from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from influxdb import InfluxDBClient

def event_handler(eventobj):
  if eventobj.event_type == MerossEventType.DEVICE_ONLINE_STATUS:
    print("Device online status changed: %s went %s" % (eventobj.device.name, eventobj.status))
    pass

  elif eventobj.event_type == MerossEventType.DEVICE_SWITCH_STATUS:
    print("Switch state changed: Device %s (channel %d) went %s" % (eventobj.device.name, eventobj.channel_id,
                                                                      eventobj.switch_state))
  elif eventobj.event_type == MerossEventType.CLIENT_CONNECTION:
    print("MQTT connection state changed: client went %s" % eventobj.status)

    # TODO: Give example of reconnection?

  elif eventobj.event_type == MerossEventType.GARAGE_DOOR_STATUS:
    print("Garage door is now %s" % eventobj.door_state)

  else:
    print("Unknown event!")

def main(argv):

  print("Starting")

  configuration = json.load(open('configuration.json'))
  if "prometheuspush-client" not in configuration:
    configuration["prometheuspush-client"] = "Meross-Prometheus"

  if "prometheuspush-server" not in configuration:
    configuration["prometheuspush-server"] = "127.0.0.1"

  if "prometheuspush-port" not in configuration:
    configuration["prometheuspush-port"] = 9091

  if "prometheuspush-prefix" not in configuration:
    configuration["prometheuspush-prefix"] = "weather"

  if "influxdb-client" not in configuration:
    configuration["influxdb-client"] = "Meross-Influxdb"

  if "influxdb-server" not in configuration:
    configuration["influxdb-server"] = "127.0.0.1"

  if "influxdb-username" not in configuration:
    configuration["influxdb-username"] = "influxdb"

  if "influxdb-password" not in configuration:
    configuration["influxdb-password"] = "influxdb"

  if "influxdb-port" not in configuration:
    configuration["influxdb-port"] = 8086

  if "influxdb-database" not in configuration:
    configuration["influxdb-database"] = "measurements"

  if "influxdb-policy" not in configuration:
    configuration["influxdb-policy"] = "sensor"

  if "influxdb-prefix" not in configuration:
    configuration["influxdb-prefix"] = "power"

  merossUsername = ""
  merossPassword = ""

  credentials = json.load(open('credentials.json'))

  if "meross" in credentials:
    meross = credentials["meross"]

    if "username" in meross:
      merossUsername = meross["username"]

    if "password" in meross:
      merossPassword = meross["password"]

  print("Configuration:")
  print("Prometheus Push Client:   ", configuration["prometheuspush-client"])
  print("Prometheus Push Server:   ", configuration["prometheuspush-server"])
  print("Prometheus Push Port:     ", configuration["prometheuspush-port"])
  print("Prometheus Push Prefix    ", configuration["prometheuspush-prefix"])

  print("Influxdb Push Client:     ", configuration["influxdb-client"])
  print("Influxdb Push Username:   ", configuration["influxdb-username"])
  print("Influxdb Push Password:   ", configuration["influxdb-password"])
  print("Influxdb Push Server:     ", configuration["influxdb-server"])
  print("Influxdb Push Port:       ", configuration["influxdb-port"])
  print("Influxdb Push Database    ", configuration["influxdb-database"])
  print("Influxdb Push Policy      ", configuration["influxdb-policy"])
  print("Influxdb Push Prefix      ", configuration["influxdb-prefix"])

  print("Meross Username           ", merossUsername)
  print("Meross Password           ", merossPassword)

  # Initiates the Meross Cloud Manager. This is in charge of handling the communication with the remote endpoint
  manager = MerossManager(meross_email=merossUsername, meross_password=merossPassword)

  # Register event handlers for the manager...
  manager.register_event_handler(event_handler)

  # Starts the manager
  manager.start()

  # You can retrieve the device you are looking for in various ways:
  plugs = manager.get_devices_by_kind(GenericPlug)

  influxDbClient = InfluxDBClient(configuration["influxdb-server"], configuration["influxdb-port"], 
    configuration["influxdb-username"], configuration["influxdb-password"], configuration["influxdb-database"])

  try:
    influxDbClient.create_database(configuration["influxdb-database"])
  except InfluxDBClientError as ex:
    print("InfluxDBClientError", ex)

    # Drop and create
    #client.drop_database(DBNAME)
    #client.create_database(DBNAME)

  influxDbClient.create_retention_policy(configuration["influxdb-policy"], 'INF', 3, default=True)

  for p in plugs:

    if not p.online:
      continue
    #print(p)
    sensorId = p.uuid.lower()

    power = p.get_electricity()

    #power_consumption = p.get_power_consumption()

    #print("power", power)
    #print("power_consumption", power_consumption)

    switch = 1
    if power["power"] == 0:
      switch = 0

    tag = {}
    sensorId = str(sensorId.replace(":", "")[-4:])

    tag["room"] = ("Room", str(p.name))
    tag["switch"] = ("Switch", switch)
    tag["power"] = ("Power", power["power"])

    now = datetime.utcnow()
    lastUtc = ("Updated", now.strftime("%Y-%m-%dT%H:%M:%SZ")) #2017-11-13T17:44:11Z

    influxDbJson = [
    {
      "measurement": configuration["influxdb-prefix"],
      "tags": {
          "sensor": sensorId,
          "name": str(p.name),
      },
      "time": lastUtc[1],
      "fields": {
      }
    }]
    for key in tag.keys():
      influxDbJson[0]["fields"][key] = tag[key][1]

    print("Pushing", influxDbJson)
    influxDbClient.write_points(influxDbJson, retention_policy=configuration["influxdb-policy"])

  manager.stop()


if __name__ == "__main__":
  main(sys.argv)