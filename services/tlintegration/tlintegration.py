import os
import sys
import json
import yaml
import backoff
import paho.mqtt.client as mqtt
import requests
from datetime import datetime, timedelta
from functools import reduce
from pytz import timezone
from requests import HTTPError

DATE_FMT = "%Y-%m-%dT%H:%M"


def get_params(when, till):
    return {"findBookingsDto.state": "Active",
            "findBookingsDto.affectsPeriodFrom": when.strftime(DATE_FMT),
            "findBookingsDto.affectsPeriodTo": till.strftime(DATE_FMT)
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

    def get_bookings(self, account, when, till):
        return self.api_call(account, "bookings", lambda data: data["bookingNumbers"], get_params(when, till))

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


def get_booked_rooms(account, receiver, when, till):
    return map(lambda bn: reduce(lambda a, b: a + b, receiver.get_booking_info(bn, account)),
        receiver.get_bookings(account, when, till))


def prepare_rooms(account, receiver):
    all_rooms = properties["accounts"][account]["room-mappings"].keys()

    today_start = datetime.now(tz=timezone(properties["timezone"])).replace(hour=0, minute=0, second=0, microsecond=0)
    today_morning = today_start.replace(hour=12)
    today_noon = today_start.replace(hour=14)
    today_night = today_start.replace(hour=23, minute=59)
    tomorrow_start = today_start + timedelta(days=1)
    tomorrow_noon = tomorrow_start.replace(hour=14)
    tomorrow_night = tomorrow_start.replace(hour=23, minute=59)

    today_morning_rooms = get_booked_rooms(account, receiver, today_start, today_morning - timedelta(minutes=1))
    today_noon_rooms = get_booked_rooms(account, receiver, today_morning + timedelta(minutes=30), today_noon - timedelta(minutes=1))
    today_night_rooms = get_booked_rooms(account, receiver, today_noon, today_night)

    tomorrow_night_rooms = get_booked_rooms(account, receiver, tomorrow_noon, tomorrow_night)

    return dict(map(lambda room: (room, {"datetime": str(today_start), "status": get_room_status(room, today_morning_rooms, today_noon_rooms, today_night_rooms, tomorrow_night_rooms)}),
               all_rooms))


def get_room_status(room, today_mornings, today_noons, today_evenings, tomorrow_evenings):
    if room not in today_mornings and room not in today_noons and room not in today_evenings and room in tomorrow_evenings:
        return "CHECKING-TOMORROW"
    if room not in today_mornings and room not in today_noons and room in today_evenings:
        return "CHECKING-TODAY"
    if room in today_noons and room in today_evenings:
        return "CHECKED-IN"
    if room in today_mornings and room not in today_noons and room not in today_evenings:
        return "CHECK-END"
    if room in today_mornings and room not in today_noons and room in today_evenings:
        return "CHECK-IN-OUT"
    if room not in today_mornings and room not in today_noons and room not in today_evenings:
        return "VACANT"


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
