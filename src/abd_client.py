import random
import threading
import logical_time as time
import server_consts as CONST
from client import Client

def get_handler(server, key, _type, output, barrier, lock):
    client = Client(server[0], server[1])
    data = client.send_data("%s %s\n"%(_type, key))
    lock.acquire()
    data = data.split(":")
    output.append(data)
    lock.release()
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        pass

def put_handler(server, key, value, barrier):
    client = Client(server[0], server[1])
    _type = "WRITE"
    data = client.send_data("%s %s %s\n"%(_type, key, value))
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        pass

class AbdClient:
    def __init__(self, num_servers):
        self.server_ids = [i+1 for i in range(num_servers)]
        self.server_map = CONST.SERVER_ID_MAP
        self.quorum_size = (num_servers//2)+1
        self.read_quorum = random.sample(self.server_ids, self.quorum_size)
        self.write_quorum = random.sample(self.server_ids, self.quorum_size)
        self.lock = threading.Lock()

    def get(self, key):
        value = self._get(key)
        v, ts = value.split(":", 1)
        if v!="-1":
            self._put(key, value)
        return v

    def put(self, key, value):
        timestamp = self._get_timestamp(key)
        timestamp = time.increment(timestamp)
        value = str(value)+":"+str(timestamp)
        self._put(key, value)
        return "ACK"

    def _get_timestamp(self, key):
        barrier = threading.Barrier(self.quorum_size, timeout=1)
        threads = []
        output = []
        for _id in self.server_ids:
            server = self.server_map[_id]
            threads.append(threading.Thread(target=get_handler, \
					    args=(server, key, CONST.GET_TS, output, barrier, self.lock)))
        for thread in threads:
            thread.start()
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass

        max_time = time.get_max_ts(output)
        return max_time
        
    def _get(self, key):
        barrier = threading.Barrier(self.quorum_size, timeout=1)
        threads = []
        output = []
        for _id in self.server_ids:
            server = self.server_map[_id]
            threads.append(threading.Thread(target=get_handler, \
					    args=(server, key, CONST.GET, output, barrier, self.lock)))
        for thread in threads:
            thread.start()
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass

        times = [x[1] for x in output]
        vals = [x[0] for x in output]

        max_time = time.get_max_ts(output)
        val = vals[times.index(max_time)]
        return val+":"+max_time 

    def _put(self, key, value):
        barrier = threading.Barrier(self.quorum_size, timeout=1)
        threads = []
        output = []
        for _id in self.server_ids:
            server = self.server_map[_id]
            threads.append(threading.Thread(target=put_handler, \
					    args=(server, key, value, barrier)))
        for thread in threads:
            thread.start()
        try:
            barrier.wait()
        except threading.BrokenBarrierError:
            pass


if __name__ == "__main__":
    client = AbdClient(1)
    print(client.get(10))
    print(client.get(12))
    print(client.get(10))
    print(client.put(11, "b"))
    print(client.get(13))


