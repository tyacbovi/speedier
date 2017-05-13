import zmq
import zmq.utils.jsonapi
import redis


class RedisZMQ:
    """
    ZMQ server that adds/fetches documents (ie dictionaries) to a Redis.
    
    NOTE: Redis server must be started before using this class
    """

    def __init__(self, bind_addr="tcp://127.0.0.1:5000"):
        """
        :param bind_addr: address to bind zmq socket on
        """
        self._bind_addr = bind_addr
        self._conn = redis.Redis()

    def _add_event_by_id(self, event_id, event, timestamp):
        self._conn.zadd(event_id, zmq.utils.jsonapi.dumps(event), timestamp)

    def start(self):
        context = zmq.Context()
        socket = context.socket(zmq.PULL)
        socket.bind(self._bind_addr)
        print "start running at " + self._bind_addr
        while True:
            msg = socket.recv_multipart()
            print "Received msg: ", msg
            if len(msg) != 3:
                error_msg = 'invalid message received: %s' % msg
                print error_msg
                continue
            event_id = msg[0]
            event_timestamp = msg[1]
            event = zmq.utils.jsonapi.loads(msg[2])
            self._add_event_by_id(event_id, event, event_timestamp)


def main():
    RedisZMQ().start()

if __name__ == '__main__':
    main()
