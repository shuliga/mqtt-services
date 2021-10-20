import os
import sys
import json
import yaml
import backoff
import paho.mqtt.client as mqtt
import requests
from datetime import datetime, timedelta
from functools import reduce

from requests import HTTPError

DATE_FMT = "%Y-%m-%dT%H:%M"

today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow = today + timedelta(days=1)


def get_params(when):
    return {"findBookingsDto.state": "Active",
            "findBookingsDto.affectsPeriodFrom": when.strftime(DATE_FMT),
            "findBookingsDto.affectsPeriodTo": when.replace(hour=23, minute=59).strftime(DATE_FMT)
            }


mqtt_user = os.environ['MQTT_USER']
mqtt_pass = os.environ['MQTT_PASS']
mqtt_host = os.environ['MQTT_HOST']
mqtt_port = int(os.environ['MQTT_PORT'])


def read_properties(filename):
    with open(filename, "r") as stream:
        try:
            global properties
            properties = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


class HttpReceiver:

    def __init__(self, base_url, api_keys):
        self.base_url = base_url
        self.api_keys = api_keys

    def get_header(self, account):
        return {"X-API-KEY": self.api_keys[account]}

    def api_call(self, account, path, parser, params=None):
        resp = requests.get(url=self.base_url + path, params=params, headers=self.get_header(account))
        if resp.status_code == 200:
            return parser(resp.json())
        else:
            raise HTTPError("HTTP Error: {} \n {}".format(str(resp.status_code), resp.url))

    def get_bookings(self, account, when):
        return self.api_call(account, "bookings", lambda data: data["bookingNumbers"], get_params(when))

    def get_booking_info(self, booking_number, account):
        return self.api_call(account, "bookings/{}".format(booking_number),
                             lambda data: list(map(lambda stays: stays["roomId"], data["roomStays"])))


class MqttSender:

    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = MqttSender.on_connect
        self.client.on_message = MqttSender.on_message
        self.client.on_disconnect = MqttSender.on_disconnect
        self.client.username_pw_set(mqtt_user, mqtt_pass)

    def give_up(self, details):
        raise RuntimeError(
            "Exiting after {tries} tries calling function {target} with args {args} and kwargs {kwargs}".format(
                **details))

    def back_off(details):
        print "Backing off {wait:0.1f} seconds after {tries} tries calling {target}".format(**details)

    def back_off_reconnect(self, details):
        print "Backing off {wait:0.1f} seconds after {tries} tries calling {target}".format(**details)
        self.initial_connect()

    @staticmethod
    def on_connect(mqtt_client, userdata, flags, rc):
        print("Connected with result code " + str(rc))

    @staticmethod
    def on_message(mqtt_client, userdata, msg):
        pass

    @staticmethod
    def on_disconnect(mqtt_client, userdata, rc):
        global mqtt_flag_connected
        mqtt_flag_connected = False
        print("Disconnected with result code " + str(rc))

    @backoff.on_exception(backoff.fibo, Exception, max_tries=15, on_giveup=give_up, on_backoff=back_off, max_value=800)
    def initial_connect(self):
        print "Connecting to {}".format(mqtt_host)
        self.client.connect(mqtt_host, mqtt_port, 60)
        return self.connection_test()

    @backoff.on_predicate(backoff.constant, interval=2, max_tries=20, on_giveup=give_up, on_backoff=back_off)
    def connection_test(self):
        self.client.loop()
        return self.client.is_connected()

    @staticmethod
    def get_topic(account, room):
        return "rent/{}/{}/booking/status".format(account, properties["accounts"][account]["room-mappings"][room])

    def publish(self, account, rooms):
        for room in rooms.keys():
            topic = MqttSender.get_topic(account, room)
            self.client.publish(topic, json.dumps(rooms[room]), retain=True)
            self.client.loop()
            print "Published to {}, payload: {}".format(topic, json.dumps(rooms[room]))


def get_booked_rooms(account, receiver, when):
    return map(lambda bn: reduce(lambda a, b: a + b, receiver.get_booking_info(bn, account)),
        receiver.get_bookings(account, when))


def prepare_rooms(account, receiver):
    all = properties["accounts"][account]["room-mappings"].keys()
    todays = get_booked_rooms(account, receiver, today)
    tomorrows = get_booked_rooms(account, receiver, tomorrow)
    return dict(map(lambda room: (room, {"datetime": str(today), "status": "CHECKED-IN" if room in todays and room in tomorrows else "CHECKED-END" if room in todays else "CHECKING-TOMORROW" if room in tomorrows else "VACANT"}),
               all))


def process_rooms(accounts, receiver, publisher):
    for account in accounts:
        publisher(account, prepare_rooms(account, receiver))


try:
    read_properties("tlintegration.yaml")
    accounts = properties["accounts"].keys()
    receiver = HttpReceiver(properties["base-url"],
                            dict(map(lambda acc: (acc, properties["accounts"][acc]["api-key"]), accounts)))
    sender = MqttSender()
    sender.initial_connect()
    process_rooms(properties["accounts"].keys(), receiver, sender.publish)
    # if initial_connect(client):
    #     run(client)

except Exception as e:
    print e
    sys.exit(1)