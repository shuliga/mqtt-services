import time
from datetime import timedelta
from aggregator import Aggregator


MAX_TEST = 101
TEST_KEY = "test-key"
TEST_TABLE_SIZE = 2

table = {}

agg = Aggregator(table=table, size=TEST_TABLE_SIZE, t_delta=timedelta(seconds=1))


def print_table(_key):
    for item_key in sorted(table[_key]):
        print "{} : {} - {}".format(_key, item_key, output_values(table[_key][item_key]))


def output_values(values):
    return "()" if not values else "({:d})".format(*values)


def test_aggregation():
    for i in range(0, MAX_TEST):
        agg.put(TEST_KEY, (i,))
        time.sleep(0.04)
    assert len(table[TEST_KEY]) == TEST_TABLE_SIZE, "Table length should be {}".format(TEST_TABLE_SIZE)


test_aggregation()
print_table(TEST_KEY)
