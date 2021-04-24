"""
Test the MQTT reader
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.mqtt import MqttReader
from mabel import Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_mq():

    reader = Reader(
        dataset=os.environ['MQTT_DATASET'],
        host=os.environ['MQTT_HOST'],
        inner_reader=MqttReader
    )

    for i, item in enumerate(reader):
        print(i, item)


if __name__ == "__main__":
    test_mq()

    print('okay')
