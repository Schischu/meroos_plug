#!/usr/bin/python3

import sys
import json
import time
import socket
from datetime import datetime

from meross_iot.manager import MerossManager
from meross_iot.meross_event import MerossEventType
from meross_iot.cloud.devices.power_plugs import GenericPlug

import paho.mqtt.client as mqtt
#from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from influxdb import InfluxDBClient

def broadcastMqtt(client, server, port, prefix, postfix, data):
  # Publishing the results to MQTT
  mqttc = mqtt.Client(client)
  mqttc.connect(server, port)

  topic = prefix + "/" + postfix

  #print "MQTT Publish", topic, data
  mqttc.publish(topic, data, qos=1, retain=True)

  mqttc.loop(2)

def broadcastHomie(configuration, topic, value):
  broadcastMqtt(
    configuration["mqtt"]["client"], 
    configuration["mqtt"]["server"], 
    configuration["mqtt"]["port"], 
    "homie", 
    topic,
    value)

def broadcastHomieDevice(configuration, deviceId, friendlyName, state, nodes, extensions, implementation=None):
  broadcastHomie(configuration, deviceId + "/$homie", "3.0") #required
  broadcastHomie(configuration, deviceId + "/$name", friendlyName) #required
  broadcastHomie(configuration, deviceId + "/$state", state) #required
  broadcastHomie(configuration, deviceId + "/$nodes", nodes) #required
  broadcastHomie(configuration, deviceId + "/$extensions", extensions) #required

  if implementation is not None:
    broadcastHomie(configuration, deviceId + "/$implementation", implementation) #optional

def broadcastHomieNode(configuration, deviceId, nodeId, friendlyName, nodeType, properties):
  broadcastHomie(configuration, deviceId + "/" + nodeId + "/$name", friendlyName) #required
  broadcastHomie(configuration, deviceId + "/" + nodeId + "/$type", nodeType) #required
  broadcastHomie(configuration, deviceId + "/" + nodeId + "/$properties", properties) #required

def broadcastHomieProperty(configuration, deviceId, nodeId, propertyId, friendlyName, dataType, unit):
  broadcastHomie(configuration, deviceId + "/" + nodeId +  "/" + propertyId + "/$name", friendlyName) #required
  broadcastHomie(configuration, deviceId + "/" + nodeId +  "/" + propertyId + "/$datatype", dataType) #required

  if unit is not None:
    broadcastHomie(configuration, deviceId + "/" + nodeId +  "/" + propertyId + "/$unit", unit)

def broadcastHomiePropertyValue(configuration, deviceId, nodeId, propertyId, value):
  broadcastHomie(configuration, deviceId + "/" + nodeId +  "/" + propertyId, value)

def event_handler(eventobj):
  if eventobj.event_type == MerossEventType.DEVICE_ONLINE_STATUS:
    print("Device online status changed: %s went %s" % (eventobj.device.name, eventobj.status))
    pass

  elif eventobj.event_type == MerossEventType.DEVICE_SWITCH_STATUS:
    print("Switch state changed: Device %s (channel %d) went %s" % (eventobj.device.name, eventobj.channel_id,
                                                                      eventobj.switch_state))
  elif eventobj.event_type == MerossEventType.CLIENT_CONNECTION:
    print("MQTT connection state changed: client went %s" % eventobj.status)

  elif eventobj.event_type == MerossEventType.GARAGE_DOOR_STATUS:
    print("Garage door is now %s" % eventobj.door_state)

  else:
    print("Unknown event!")

def main(argv):

  print("Starting")

  configuration = json.load(open('configuration.json'))

  if "mqtt" in configuration:
    try:
      if "client" not in configuration["mqtt"]:
        configuration["mqtt"]["client"] = "Ruuvi-Mqtt"

      if "server" not in configuration["mqtt"]:
        configuration["mqtt"]["server"] = "127.0.0.1"

      if "port" not in configuration["mqtt"]:
        configuration["mqtt"]["port"] = 1883

      if "prefix" not in configuration["mqtt"]:
        configuration["mqtt"]["prefix"] = "power"

      if "enabled" not in configuration["mqtt"]:
        configuration["mqtt"]["enabled"] = True

      if "classic" not in configuration["mqtt"]:
        configuration["mqtt"]["classic"] = True

      if "homie" not in configuration["mqtt"]:
        configuration["mqtt"]["homie"] = False

      print ("MQTT Configuration:")
      print ("MQTT Client:    ", configuration["mqtt"]["client"])
      print ("MQTT Server:    ", configuration["mqtt"]["server"])
      print ("MQTT Port:      ", configuration["mqtt"]["port"])
      print ("MQTT Prefix:    ", configuration["mqtt"]["prefix"])
      print ("MQTT Enabled:   ", configuration["mqtt"]["enabled"])
      print ("MQTT Classic:   ", configuration["mqtt"]["classic"])
      print ("MQTT Homie 3.0: ", configuration["mqtt"]["homie"])

    except Exception as ex:
      print ("Error parsing mqtt configuration", ex)
      configuration["mqtt"]["enabled"] = False
  else:
    configuration["mqtt"] = {}
    configuration["mqtt"]["enabled"] = False

  if "influxdb" in configuration:
    try:
      if "client" not in configuration["influxdb"]:
        configuration["influxdb"]["client"] = "Ruuvi-Influxdb"

      if "server" not in configuration["influxdb"]:
        configuration["influxdb"]["server"] = "127.0.0.1"

      if "username" not in configuration["influxdb"]:
        configuration["influxdb"]["username"] = "influxdb"

      if "password" not in configuration["influxdb"]:
        configuration["influxdb"]["password"] = "influxdb"

      if "port" not in configuration["influxdb"]:
        configuration["influxdb"]["port"] = 8086

      if "database" not in configuration["influxdb"]:
        configuration["influxdb"]["database"] = "measurements"

      if "policy" not in configuration["influxdb"]:
        configuration["influxdb"]["policy"] = "sensor"

      if "prefix" not in configuration["influxdb"]:
        configuration["influxdb"]["prefix"] = "power"

      if "enabled" not in configuration["influxdb"]:
        configuration["influxdb"]["enabled"] = True

      print ("Influxdb Configuration:")
      print ("Influxdb Client:     ", configuration["influxdb"]["client"])
      print ("Influxdb Username:   ", configuration["influxdb"]["username"])
      print ("Influxdb Password:   ", configuration["influxdb"]["password"])
      print ("Influxdb Server:     ", configuration["influxdb"]["server"])
      print ("Influxdb Port:       ", configuration["influxdb"]["port"])
      print ("Influxdb Database:   ", configuration["influxdb"]["database"])
      print ("Influxdb Policy:     ", configuration["influxdb"]["policy"])
      print ("Influxdb Prefix:     ", configuration["influxdb"]["prefix"])
      print ("Influxdb Enabled:    ", configuration["influxdb"]["enabled"])

    except Exception as ex:
      print ("Error parsing influxdb configuration", ex)
      configuration["influxdb"]["enabled"] = False
  else:
    configuration["influxdb"] = {}
    configuration["influxdb"]["enabled"] = False

  merossUsername = ""
  merossPassword = ""

  credentials = json.load(open('credentials.json'))

  if "meross" in credentials:
    meross = credentials["meross"]

    if "username" in meross:
      merossUsername = meross["username"]

    if "password" in meross:
      merossPassword = meross["password"]

  print("Meross Configuration:")
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

  if configuration["influxdb"]["enabled"]:
    influxDbClient = InfluxDBClient(configuration["influxdb"]["server"], configuration["influxdb"]["port"], 
      configuration["influxdb"]["username"], configuration["influxdb"]["password"], configuration["influxdb"]["database"])

    try:
      influxDbClient.create_database(configuration["influxdb"]["database"])
    except InfluxDBClientError as ex:
      print("InfluxDBClientError", ex)

    influxDbClient.create_retention_policy(configuration["influxdb"]["policy"], 'INF', 3, default=True)

  for p in plugs:

    if not p.online:
      continue
    print("-"*80)
    print(p)
    print("-"*80)
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

    tag["room"] = ("Room", str(p.name), None)
    tag["switch"] = ("Switch", bool(switch), None)
    tag["power"] = ("Power", power["power"], "W")

    now = datetime.utcnow()
    lastUtc = ("Updated", now.strftime("%Y-%m-%dT%H:%M:%SZ")) #2017-11-13T17:44:11Z

    if configuration["mqtt"]["enabled"]:
      if configuration["mqtt"]["classic"]:
        print ("Pushing Mqtt", sensorId, ":", configuration["mqtt"]["prefix"], tag)
        try:
          broadcastMqtt(
            configuration["mqtt"]["client"], 
            configuration["mqtt"]["server"], 
            configuration["mqtt"]["port"], 
            configuration["mqtt"]["prefix"], 
            sensorId + "/update",
            json.dumps(tag))
        except Exception as ex:
          print ("Error on mqtt broadcast", ex)

      if configuration["mqtt"]["homie"]:
        deviceId = configuration["mqtt"]["prefix"] + "-" + sensorId

        broadcastHomieDevice(configuration, deviceId, tag["room"][1], "init", "0", "")

        properties = ""
        for key in tag.keys():
            properties = properties + key + ","

        # cut last ,
        if len(properties) > 0:
          properties = properties[:-1]

        print ("Pushing Mqtt Homie", sensorId, ":", configuration["mqtt"]["prefix"], tag)
        try:
          # deviceId, nodeId, friendlyName, nodeType, properties
          broadcastHomieNode(configuration, deviceId, "0", "0", configuration["mqtt"]["prefix"], properties)
        except Exception as ex:
          print ("Error on homie broadcast", ex)

        try:
          for key in tag.keys():
            dataType = "string"
            if type(tag[key][1]) is int:
              dataType = "integer"
            elif type(tag[key][1]) is float:
              dataType = "float"
            elif type(tag[key][1]) is bool:
              dataType = "boolean"

            # deviceId, nodeId, propertyId, friendlyName, dataType, unit
            broadcastHomieProperty(configuration, deviceId, "0", key, tag[key][0], dataType, tag[key][2])

            # deviceId, nodeId, propertyId, value
            broadcastHomiePropertyValue(configuration, deviceId, "0", key, tag[key][1])
        except Exception as ex:
          print ("Error on homie broadcast", ex)

    if configuration["influxdb"]["enabled"]:
      influxDbJson = [
      {
        "measurement": configuration["influxdb"]["prefix"],
        "tags": {
            "sensor": sensorId,
            "name": str(p.name),
            "type": "plug",
        },
        "time": lastUtc[1],
        "fields": {
        }
      }]
      for key in tag.keys():
        if key == "switch":
          influxDbJson[0]["fields"][key] = int(tag[key][1]) # its already an int in database :-(
        else:
          influxDbJson[0]["fields"][key] = tag[key][1]

      print("Pushing InfluxDb", influxDbJson)
      try:
        influxDbClient.write_points(influxDbJson, retention_policy=configuration["influxdb"]["policy"])
      except Exceptio as ex:
        print ("Error on influxdb write_points", ex)

  manager.stop()


if __name__ == "__main__":
  main(sys.argv)