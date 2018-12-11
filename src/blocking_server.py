import sys
#from multi_threaded_server_blocking import BlockingMultiThreadedServer as ServerBase
# Comment the above line and uncomment the following line for LSM implementation
from mt_lsm_server_blocking import BlockingMultiThreadedLsmServer as ServerBase

class BlockingServer(ServerBase):
    def __init__(self, host, port, cache_size, db_name):
        super(BlockingServer, self).__init__(host, port, cache_size, db_name)


if __name__ == "__main__":
    host = sys.argv[1]
    port = int(sys.argv[2])
    cache_size = int(sys.argv[3])
    db_name = sys.argv[4]
    # No sanity check for input
    server = BlockingServer(host, port, cache_size, db_name)
    server.run_server()
