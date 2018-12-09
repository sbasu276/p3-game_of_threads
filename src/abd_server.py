import sys
from multi_threaded_server import MultiThreadedServer as ServerBase
# Comment the above line and uncomment the following line for LSM implementation
#from mt_lsm_server import MultiThreadedLsmServer as ServerBase

class AbdServer(ServerBase):
    def __init__(self, host, port, cache_size, db_name):
        super(AbdServer, self).__init__(host, port, cache_size, db_name)


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    cache_size = int(sys.argv[3])
    db_name = sys.argv[4]
    # No sanity check for input
    server = AbdServer(host, port, cache_size, db_name)
    server.run_server()
