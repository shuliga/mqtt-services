import numpy as np
from datetime import datetime
from datetime import timedelta


class TimeWindowAggregator:

    NAME = "time-window"

    @staticmethod
    def reduce_avg(tuples_list):
        return tuple(np.array(map(sum, zip(*tuples_list))) / len(tuples_list))

    @staticmethod
    def reduce_max(tuples_list):
        return tuple(np.array(map(max, zip(*tuples_list))))

    @staticmethod
    def reduce_min(tuples_list):
        return tuple(np.array(map(min, zip(*tuples_list))))

    @staticmethod
    def reduce_count(tuples_list):
        return tuple(np.array(map(len, zip(*tuples_list))))

    def __init__(self, sub_topic, size=48, t_delta=timedelta(hours=1), on_update=None, reducer=None):
        self.table = {}
        self.sub_topic = sub_topic
        self.timedelta = t_delta
        self.size = size
        self.initiated = datetime.now()
        self.buffer = {}
        self.on_update = on_update
        self.reducer = TimeWindowAggregator.reduce_avg if not reducer else reducer
        self.reducer_name = "avg"
        print "Started aggregator '{}' with '{}' reducer every {}, recent {} items".format(TimeWindowAggregator.NAME, self.reducer_name,
                                                                             self.timedelta, self.size)

    def get_path(self):
        return "/".join([TimeWindowAggregator.NAME, self.reducer_name, self.get_interval_name(), self.get_span_name()])

    def get_interval_name(self):
        timedelta_arr = str(self.timedelta).split(",")
        time_span = timedelta_arr[1] if len(timedelta_arr) == 2 else timedelta_arr[0]
        intervals = map(int, time_span.split(":"))
        return "in" \
            + ("-{}days".format(self.timedelta.days) if self.timedelta.days else "") \
            + ("-{}hrs".format(intervals[0]) if intervals[0] else "") \
            + ("-{}min".format(intervals[1]) if intervals[1] else "") \
            + ("-{}min".format(intervals[2]) if intervals[2] else "")

    def get_span_name(self):
        return "" if self.size <= 1 else "recent-{}".format(self.size)

    def reload_table(self, key, new_data):
        self.buffer[key].clear()
        self.table[key] = new_data

    def put(self, key, values):
        self.buffer.setdefault(key, []).append(values)
        self.table.setdefault(key, {})
        self.loop()

    def loop(self):
        now = datetime.now()
        if now - self.initiated >= self.timedelta:
            self.initiated = now
            for key in self.table.keys():
                self.table.setdefault(key, {})[now] = self.__reduce_buffer__(key)
                if len(self.table[key]) > self.size:
                    self.table[key].pop(min(self.table[key]))
            if self.on_update:
                self.on_update(self.table, self.sub_topic, self.get_path())

    def on_update(self, func):
        self.on_update = func

    def __reduce_buffer__(self, key):
        if len(self.buffer[key]) > 0:
            rec_avg = TimeWindowAggregator.reduce_avg(self.buffer[key])
            del self.buffer[key][:]
            return rec_avg
        else:
            return None
