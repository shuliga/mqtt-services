#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2020 SHL
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
import time
import argparse
import re
import backoff
import paho.mqtt.client as mqtt
from datetime import timedelta
from aggregator import TimeWindowAggregator


mqtt_table = {}
aggregator_table = {}

mqtt_user = os.environ['MQTT_USER']
mqtt_pass = os.environ['MQTT_PASS']
mqtt_host = os.environ['MQTT_HOST']
mqtt_port = int(os.environ['MQTT_PORT'])


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


parser = argparse.ArgumentParser(description='Lightweight MQTT topic aggregator')
parser.add_argument('-m', type=int, default=15, dest='mins', help='Aggregation interval in minutes.')
parser.add_argument('-r', type=int, default=96, dest='size', help='Maximum size of recently aggregated data.')
parser.add_argument('-p', type=str, default=None, dest='pub_prefix', help='Published MQTT topic prefix. If not set, Subscribed topic will be used as prefix.')
parser.add_argument('-s', type=str, default="rent/+/+/+/status/banner/0", dest='sub_topic', help='MQTT topic to subscribe on and aggregate.')
parser.add_argument('-t', type=str2bool, nargs='?', const=True, default=False, dest='output_timestamp', help='Output timestamp with each item. The resulting table will be a dictionary.')
parser.add_argument('--dry', type=str2bool, nargs='?', const=True, default=False, dest='dry_run', help='Dry run. MQTT subscriptions remain active while responses halted.')

args_space = parser.parse_args()


def sub_topic_key_idx(sub_topic):
    return min(loc for loc, val in enumerate(sub_topic.split("/")) if val == "+"), max(loc for loc, val in enumerate(sub_topic.split("/")) if val == "+")


def matching_sub_topic(sub_topic):
    for key in mqtt_table.keys():
        if re.match(str(key).replace('+', '.+') + '$', sub_topic):
            return key
    return None


def matching_aggregation(sub_topic):
    for key in mqtt_table.keys():
        if re.match(mqtt_table[key]['prefix'] if mqtt_table[key]['prefix'] else str(key).replace('+', '.+') + mqtt_table[key]['aggregator'].get_path() + '$', sub_topic):
            return key
    return None


def extract_key_parts(path, sub_topic):
    ket_parts = []
    for i in range(sub_topic_key_idx(sub_topic)[0], sub_topic_key_idx(sub_topic)[1] + 1):
        ket_parts.append(path.split('/')[i])
    return ket_parts


def on_connect(mqtt_client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    global mqtt_flag_connected
    mqtt_flag_connected = True
    for topic, prefix, aggregator in get_aggregations():
        mqtt_table[topic] = {'prefix': prefix, 'aggregator': aggregator, 'inbound_topics': set()}
        subscribe(mqtt_client, topic, prefix, aggregator)


def on_disconnect(mqtt_client, userdata, rc):
    global mqtt_flag_connected
    mqtt_flag_connected = False
    print("Disconnected with result code "+str(rc))


def on_message(mqtt_client, userdata, msg):
    sub_topic = matching_sub_topic(msg.topic)
    if sub_topic:
        key = get_key(str(msg.topic), sub_topic)
        values = str(msg.payload).strip().replace("~C ", "/").replace("%", "").split("/")
        mqtt_table[sub_topic]['aggregator'].put(key, (float(values[0]), float(values[1])))
        mqtt_table[sub_topic]['inbound_topics'].add(str(msg.topic))
    #    print "{} -> {}".format(key, raw)
        return
    if str(msg.topic).startswith("logger/dashboard"):
        # client.unsubscribe(args_space.sub_topic)
        # key = get_key(str(msg.topic))
        # mqtt_table[sub_topic]['aggregator'].reload_table(json.loads(str(msg.payload)))
        return


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.username_pw_set(mqtt_user, mqtt_pass)


def get_aggregations():
    agg = TimeWindowAggregator(args_space.sub_topic, args_space.size, timedelta(minutes=args_space.mins), on_update=publish_table)
    return [(args_space.sub_topic, args_space.pub_prefix, agg)]


def subscribe(mqtt_client, topic, prefix, aggregator):
    mqtt_client.subscribe(topic)
    print("Subscribed to: " + topic)
    if prefix:
        print "MQTT Publish topic prefix is overridden to '{}'".format(prefix)


def get_key(path, sub_topic):
    return "/".join(extract_key_parts(path, sub_topic))


def publish_table(table, sub_topic, agg_path):
    for key in table:
        pub_topic = get_pub_topic(key, sub_topic, mqtt_table[sub_topic]['prefix'], agg_path)
        if not args_space.dry_run:
            client.publish(pub_topic, payload=format_table(table[key]), qos=1, retain=True)
        else:
            print "Dry run mode:"
        print "Pub topic: {}".format(pub_topic)
        print_table(key, table)


def get_pub_topic(key, sub_topic, pub_prefix, agg_path):
    topic_path = sub_topic.split("/")
    key_path = key.split("/")
    for i in range(sub_topic_key_idx(sub_topic)[0], sub_topic_key_idx(sub_topic)[1] + 1):
        topic_path[i] = key_path[i - sub_topic_key_idx(sub_topic)[0]]
    return "/".join(([pub_prefix] if pub_prefix else topic_path) + [agg_path])


def print_table(_key, table):
    for item_key in sorted(table[_key]):
        print "{} : {}".format(_key, output_values(table[_key][item_key], item_key if args_space.output_timestamp else None))


def format_table(table_dic):
    rows = []
    for item_key in sorted(table_dic):
        rows.append(format_values(table_dic[item_key]))
    return "[{}]".format(", ".join(rows))


def output_values(values, key=None):
    return "()" if not values else "({:.1f}°C, {:.0f}%)".format(*values) if key is None else "'{}' - ({:.1f}°C, {:.0f}%)".format(key, *values)


def format_values(values, key=None):
    return "[]" if not values else "[{:.1f}, {:.0f}]".format(*values) if key is None else "'{}': [{:.1f}, {:.0f}]".format(key, *values)


def table_size(table):
    return sum(map(len, table.values()))


def give_up(details):
    raise RuntimeError("Exiting after {tries} tries calling function {target} with args {args} and kwargs {kwargs}".format(**details))


def back_off(details):
    print "Backing off {wait:0.1f} seconds after {tries} tries calling {target}".format(**details)


def back_off_reconnect(details):
    print "Backing off {wait:0.1f} seconds after {tries} tries calling {target}".format(**details)
    initial_connect(client)


@backoff.on_exception(backoff.fibo, Exception, max_tries=15, on_giveup=give_up, on_backoff=back_off, max_value=800)
def initial_connect(_client):
    print "Connecting to {}".format(mqtt_host)
    _client.connect(mqtt_host, mqtt_port, 60)
    return connection_test(_client)


@backoff.on_predicate(backoff.constant, interval=2, max_tries=20, on_giveup=give_up, on_backoff=back_off)
def connection_test(_client):
    _client.loop()
    return _client.is_connected()


@backoff.on_predicate(backoff.fibo, max_tries=15, on_giveup=give_up, on_backoff=back_off_reconnect, max_value=800)
def run(_client):
    print "Aggregation process is RUNNING {}".format("in DRY RUN mode" if args_space.dry_run else "")
    while mqtt_flag_connected:
        _client.loop()
        for val in mqtt_table.values():
            val['aggregator'].loop()
        time.sleep(0.5)
    else:
        print "Aggregation process is STOPPED"
    return False


try:
    if initial_connect(client):
        run(client)

except RuntimeError:
    pass
