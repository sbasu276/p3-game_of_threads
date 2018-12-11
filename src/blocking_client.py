import sys, random, time
import threading
import logical_time as ltime
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

def acquire_lock_handler(server, client_id, key, barrier):
    client = Client(server[0], server[1])
    data = client.send_data("%s %s %s\n"%(CONST.ACQUIRE_LOCK, client_id, key))
    if(data == 'LOCK_GRANTED'):
        try:
            barrier.wait() #reporting to barrier
        except threading.BrokenBarrierError: #Barrier broken - releasing lock
            client.send_data("%s %s %s\n"%(CONST.RELEASE_LOCK, client_id, key))

    elif(data == 'LOCK_DENIED'):
        print('Lock denied! Exiting..')
    else:
        print('Something wrong @ acquire_lock handler')
        sys.exit(0)

def release_lock_handler(server, client_id, key):
    client = Client(server[0], server[1])
    client.send_data("%s %s %s\n"%(CONST.RELEASE_LOCK, client_id, key))

    
def put_handler(server, key, value, barrier):
    client = Client(server[0], server[1])
    _type = "WRITE"
    data = client.send_data("%s %s %s\n"%(_type, key, value))
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        pass

class BlockingClient:
    def __init__(self, client_id, num_servers):
        self.server_ids = [i+1 for i in range(num_servers)]
        self.server_map = CONST.SERVER_ID_MAP
        self.quorum_size = (num_servers//2)+1
        self.read_quorum = random.sample(self.server_ids, self.quorum_size)
        self.write_quorum = random.sample(self.server_ids, self.quorum_size)
        self.lock = threading.Lock()
        self.client_id = client_id

    def get(self, key):
        while(not self._acquire_lock(key)): #Spin lock acquire
            time.sleep(2)
        value = self._get(key)
        self._release_lock(key)
        v, ts = value.split(":", 1)
        return v

    def put(self, key, value):
        while(not self._acquire_lock(key)): #Spin lock acquire
            sleep(2)
        timestamp = self._get_timestamp(key)
        timestamp = ltime.increment(timestamp)
        value = str(value)+":"+str(timestamp)
        self._put(key, value)
        self._release_lock(key)
        return "ACK"

    def _get_timestamp(self, key):
        barrier = threading.Barrier(self.quorum_size+1, timeout=1)
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

        times = [x[0] for x in output]
        max_time = ltime.get_max_ts(times)
        return max_time
        
    def _get(self, key):
        barrier = threading.Barrier(self.quorum_size+1, timeout=1)
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

        max_time = ltime.get_max_ts(times)
        val = vals[times.index(max_time)]
        return val+":"+max_time 

    def _put(self, key, value):
        barrier = threading.Barrier(self.quorum_size+1, timeout=1)
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

    def _acquire_lock(self, key):
        barrier = threading.Barrier(self.quorum_size+1, timeout=1)
        threads = []
        for _id in self.server_ids:
            server = self.server_map[_id]
            threads.append(threading.Thread(target=acquire_lock_handler, \
					    args=(server, self.client_id, key, barrier)))
        for thread in threads:
            thread.start()

        try:
            barrier.wait(CONST.QUORUM_TIMEOUT) # waiting for quorum
        except threading.BrokenBarrierError: # failed to attain quorum within timeout - aborting barrier
            barrier.abort()
            return False

        #print("Lock quorum attained!")
        return True

    def _release_lock(self, key):
        threads = []
        for _id in self.server_ids:
            server = self.server_map[_id]
            threads.append(threading.Thread(target=release_lock_handler, \
					    args=(server, self.client_id, key)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        return True
        
if __name__ == "__main__":
    client = BlockingClient(1,3)
    print(client.get(10))
    print(client.get(12))
    print(client.get(10))
    print(client.put(11, "b"))
    print(client.get(13))
