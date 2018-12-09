import json
import sys
import multiprocessing.dummy as mp
import os
import time
import math
import random
from time import sleep
import string
import subprocess
import threading
from abd_lient import AbdClient

#Note: This run_client() has been influenced by the code of Soumen's research project
def run_client(config):
    num_server = int(config[0][0])
    fname = config[0][1]
    stream = config[1]
    for msg in stream:
        req = msg.split()
        req_type = req[0]
        c = AbdClient(num_server)
        start = time.time()
        if req_type == "GET":
            c.get(req[1])
        else:
            c.put(req[1], req[2])
        latency = time.time() - start
        with open(fname, 'a') as f:
            f.write(req_type+' '+str(latency)+'\n')
        sleep(0.5)

def get_args(tb):
    stream = []
    with open(tb, "r") as f:
        for line in f.readlines():
            stream.append(line.strip('\n'))
    s = 0
    streams = []
    for i in range(1,300):
        streams.append(stream[s:i*20])
        s = i*20
    return streams

import sys
ratio = [float(sys.argv[1])]
size = sys.argv[2]
n = int(sys.argv[3])
for rr in ratio:
    tb = "../data/tbs/%s"%rr
    res = "../data/res/%s_s"%(size,rr)
    streams = get_args(tb)
    pool_args = [[[n, res], stream] for stream in streams]
    with mp.Pool(300) as pool:
        pool.map(run_client, pool_args)
    print("DONE %s"%rr)
