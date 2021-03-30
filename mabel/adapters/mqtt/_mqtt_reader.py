# type:ignore
import glob
import datetime
import paho.mqtt.client as mqtt
from typing import Iterator, Tuple, Optional, List
from ...data.readers.internals.base_inner_reader import BaseInnerReader
from ...utils import paths, common
from ...logging import get_logger
from ...errors import InvalidReaderConfigError


# pip install paho-mqtt

class MqttReader(BaseInnerReader):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.dataset = kwargs.get('dataset')
        self.host = kwargs.get('host')
        if not self.host:
            raise InvalidReaderConfigError('MqttReader requires `host` to be set.')
        self.port = int(kwargs.get('port', 1883))

        self.messages = []

    def on_connect(self, client, userdata, flags, rc):
        get_logger().debug("Connected to mqtt server with result code "+str(rc))
        client.subscribe(self.dataset)

    def on_message(self, client, userdata, msg):
        self.messages.append(msg.payload)

    def list_of_sources(self):
        # just return the topic
        return [self.dataset]

    def read_from_source(self, file_name: str):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.connect(self.host, self.port, 60)

        while True:
            self.client.loop(timeout=1.0)
            if self.messages:
                yield from self.messages
            self.messages.clear()

    def __del__(self):
        self.client.loop_stop()