import zmq.utils.jsonapi
import zmq


class TestDataGenerator(object):
    """
    Client that connects with ZMQ server to write events. 
    """

    def __init__(self, connect_addr='tcp://127.0.0.1:5000'):
        self._context = zmq.Context()
        self._socket = self._context.socket(zmq.PUSH)
        self._socket.connect(connect_addr)

    @staticmethod
    def generate_data():
        """
        Cases:
         1 - simple case
         2 - chopped path
         3 - chopped path with and a full path interleave
         4 - two interleaved paths
        """
        return [(1, 1, {"timestamp": 1, "event_id": 1, "description": "start event A"}),
                (1, 3, {"timestamp": 3, "event_id": 3, "description": "start event B"}),
                (1, 2, {"timestamp": 2, "event_id": 2, "description": "end event A"}),
                (1, 3.1111111, {"timestamp": 3.1111111, "event_id": 4, "description": "middle event B"}),
                (1, 3.22, {"timestamp": 3.22, "event_id": 5, "description": "end event B"}),
                (1, 3.5, {"timestamp": 3.5, "event_id": 6, "description": "start event C"}),
                (1, 3.7, {"timestamp": 3.7, "event_id": 7, "description": "end event C"}),
                (2, 1.5, {"timestamp": 1.5, "event_id": 1, "description": "start event A"}),
                (2, 2, {"timestamp": 2, "event_id": 2, "description": "end event A"}),
                (2, 5.5, {"timestamp": 5.5, "event_id": 3, "description": "start event B"}),
                (3, 1, {"timestamp": 1, "event_id": 1, "description": "start event A"}),
                (3, 2, {"timestamp": 2, "event_id": 2, "description": "end event A"}),
                (3, 2.1, {"timestamp": 2.1, "event_id": 1, "description": "start event A"}),
                (3, 2.2, {"timestamp": 2.2, "event_id": 2, "description": "end event A"}),
                (3, 3, {"timestamp": 3, "event_id": 3, "description": "start event B"}),
                (3, 3.101, {"timestamp": 3.101, "event_id": 3, "description": "start event B"}),
                (3, 3.1111111, {"timestamp": 3.1111111, "event_id": 4, "description": "middle event B"}),
                (3, 3.22, {"timestamp": 3.22, "event_id": 5, "description": "end event B"}),
                (3, 3.5, {"timestamp": 3.5, "event_id": 6, "description": "start event C"}),
                (3, 3.7, {"timestamp": 3.7, "event_id": 7, "description": "end event C"}),
                (4, 1, {"timestamp": 1, "event_id": 1, "description": "start event A"}),
                (4, 2, {"timestamp": 2, "event_id": 2, "description": "end event A"}),
                (4, 2.1, {"timestamp": 2.1, "event_id": 1, "description": "start event A"}),
                (4, 2.2, {"timestamp": 2.2, "event_id": 2, "description": "end event A"}),
                (4, 3, {"timestamp": 3, "event_id": 3, "description": "start event B"}),
                (4, 3.1111111, {"timestamp": 3.1111111, "event_id": 4, "description": "middle event B"}),
                (4, 3.22, {"timestamp": 3.22, "event_id": 5, "description": "end event B"}),
                (4, 3.3, {"timestamp": 3.3, "event_id": 3, "description": "start event B"}),
                (4, 3.5, {"timestamp": 3.5, "event_id": 4, "description": "middle event B"}),
                (4, 3.6, {"timestamp": 3.6, "event_id": 5, "description": "end event B"}),
                (4, 3.5, {"timestamp": 3.5, "event_id": 6, "description": "start event C"}),
                (4, 3.7, {"timestamp": 3.7, "event_id": 7, "description": "end event C"}),
                (4, 3.8, {"timestamp": 3.8, "event_id": 6, "description": "start event C"}),
                (4, 3.9, {"timestamp": 3.9, "event_id": 7, "description": "end event C"})
                ]

    def _send_msg(self, msg):
        self._socket.send_multipart(msg)

    def send_event(self, event_id, event, timestamp):
        return self._send_msg([str(event_id), str(timestamp), zmq.utils.jsonapi.dumps(event)])

    def run(self):
        for sample_id, sample_timestamp, sample in self.generate_data():
            self.send_event(sample_id, sample, sample_timestamp)


def main():
    TestDataGenerator().run()

if __name__ == "__main__":
    main()
