#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2007 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import sys
import paho.mqtt.client as mqtt
from datetime import timedelta
from aggregator import Aggregator

SUB_TOPIC = "rent/+/+/+/status/banner/0"
SUB_TOPIC_KEY_IDX = (min(loc for loc, val in enumerate(SUB_TOPIC.split("/")) if val == "+"), max(loc for loc, val in enumerate(SUB_TOPIC.split("/")) if val == "+"))

mqtt_table = {}
aggregate_table = {}

mqtt_user = os.environ['MQTT_USER']
mqtt_pass = os.environ['MQTT_PASS']
mqtt_host = os.environ['MQTT_HOST']
mqtt_port = int(os.environ['MQTT_PORT'])

interval_minutes = sys.argv[1] if sys.argv[1] else 15
recent_size = sys.argv[2] if sys.argv[2] else 96


def extract_key_parts(path_array):
    ket_parts = []
    for i in range(SUB_TOPIC_KEY_IDX[0], SUB_TOPIC_KEY_IDX[1] + 1):
        ket_parts.append(path_array[i])
    return ket_parts


def on_connect(mqtt_client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    mqtt_client.subscribe(SUB_TOPIC)
    print("Subscribed to: "+SUB_TOPIC)


def on_message(mqtt_client, userdata, msg):
    raw = str(msg.payload).strip()
    mqtt_table[msg.topic] = raw
    key = "/".join(extract_key_parts(str(msg.topic).split('/')))
    values = raw.replace("~C ", "/").replace("%", "").split("/")
    agg.put(key, (float(values[0]), float(values[1])))
#    print "{} -> {}".format(key, raw)


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(mqtt_user, mqtt_pass)

client.connect(mqtt_host, mqtt_port, 60)


def publish_table():
    for key in aggregate_table:
        client.publish(get_pub_topic(key), payload=format_table(aggregate_table[key]), qos=1, retain=True)
        print "Pub topic: {}".format(get_pub_topic(key))
        print_table(key)


def get_pub_topic(key):
    topic_path = SUB_TOPIC.split("/")
    key_path = key.split("/")
    for i in range(SUB_TOPIC_KEY_IDX[0], SUB_TOPIC_KEY_IDX[1] + 1):
        topic_path[i] = key_path[i - SUB_TOPIC_KEY_IDX[0]]
    return "/".join(topic_path + [agg.get_path()])


def print_table(_key):
    for item_key in sorted(aggregate_table[_key]):
        print "{} : {} - {}".format(_key, item_key, output_values(aggregate_table[_key][item_key]))


def format_table(table_dic):
    rows = []
    for item_key in sorted(table_dic):
        rows.append(format_values(table_dic[item_key]))
    return "[{}]".format(", ".join(rows))


def output_values(values):
    return "()" if not values else "({:.1f}°C, {:.0f}%)".format(*values)


def format_values(values):
    return "[]" if not values else "[{:.1f}, {:.0f}]".format(*values)


def table_size():
    return sum(map(len, aggregate_table.values()))


agg = Aggregator(aggregate_table, recent_size, timedelta(minutes=interval_minutes), on_update=publish_table)

print "Started aggregator '{}' every {} min, recent {} items".format(agg.reducer_name, interval_minutes, recent_size)


try:
    al = table_size()
    while True:
        client.loop()
        agg.loop()
except KeyboardInterrupt:
    pass
