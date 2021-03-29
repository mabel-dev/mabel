"""
Test the MQTT reader, not available in most environments
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from mabel.adapters.mqtt import MqttReader
from mabel.data import Reader
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

    for item in reader:
        print(item)


if __name__ == "__main__":
    test_mq()

    print('okay')
