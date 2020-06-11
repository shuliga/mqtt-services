import numpy as np
from datetime import datetime
from datetime import timedelta


class Aggregator:

    NAME = "agg"

    @staticmethod
    def reduce_avg(tuples_list):
        return tuple(np.array(map(sum, zip(*tuples_list))) / len(tuples_list))

    def __init__(self, table, size=48, t_delta=timedelta(hours=1), on_update=None, reducer=None):
        self.table = table
        self.timedelta = t_delta
        self.size = size
        self.initiated = datetime.now()
        self.buffer = {}
        self.on_update = on_update
        self.reducer = Aggregator.reduce_avg if not reducer else reducer
        self.reducer_name = "avg"

    def get_path(self):
        return "/".join([Aggregator.NAME, self.reducer_name, self.get_interval_name(), self.get_span_name()])

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

    def put(self, key, values):
        self.buffer.setdefault(key, []).append(values)
        self.table.setdefault(key, {})
        self.loop()

    def loop(self):
        if datetime.now() - self.initiated >= self.timedelta:
            self.initiated = datetime.now()
            for key in self.table.keys():
                self.table.setdefault(key, {})[datetime.now()] = self.__reduce_buffer__(key)
                if len(self.table[key]) > self.size:
                    self.table[key].pop(min(self.table[key]))
            if self.on_update:
                self.on_update()

    def on_update(self, func):
        self.on_update = func

    def __reduce_buffer__(self, key):
        if len(self.buffer) > 0:
            rec_avg = Aggregator.reduce_avg(self.buffer[key])
            del self.buffer[key][:]
            return rec_avg
        else:
            return None
