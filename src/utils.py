from bisect import bisect_left
import logical_time as time

class Request:
    def __init__(self, op=None, key=None, value=None, tag=None, fd=None):
        self.op = op
        self.key = key
        self.value = value
        self.fd = fd
        self.tag = tag

def binary_search(a, x, lo=0, hi=None):
    hi = hi if hi is not None else len(a) 
    pos = bisect_left(a, x, lo, hi)  
    return (pos if pos != hi and a[pos] == x else -1)  

def parse_req(request):
    print("UTIL ", request)
    req = request.strip('\n').replace(':', ' ').split()
    request = None
    if len(req)==3:
        request = Request(req[0], req[1], req[2])
    elif len(req)==4:
        request = Request(req[0], req[1], req[2], req[3])
    else:
        request = Request(req[0], req[1])
    return request

def add_response(mapper, sock, response):
    if sock is not None:
        data = mapper[sock]
        data.resp = response
        mapper[sock] = data

def get(key, cache, persistent, lock):
    lock.acquire()
    val = cache.get(key)
    lock.release()
    if val is None:
        val = persistent.get(key)
        if val:
            lock.acquire()
            retkey, retval, _ = cache.insert(key, val, dirty=False)
            lock.release()
            if retkey and retval:
                persistent.writeback(retkey, retval)
    if val:
        return val
    else:
        return "-1"

def put(key, value, cache, persistent, lock):
    lock.acquire()
    retval = cache.put(key, value)
    lock.release()
    if retval is None:
        if persistent.put(key, value):
            lock.acquire()
            retkey, retval, _ = cache.insert(key, value, dirty=False)
            lock.release()
            if retkey and retval:
                persistent.writeback(retkey, retval)
            return "ACK"
        else:
            return "-1"
    return "ACK"

def insert(key, value, cache, persistent, lock):
    if persistent.insert(key, value):
        lock.acquire()
        retkey, retval, _ = cache.insert(key, value)
        lock.release()
        if retkey and retval:
            persistent.writeback(retkey, retval)
        return "ACK"
    return "-1"

def delete(key, cache, persistent, lock):
    pdel = persistent.delete(key)
    lock.acquire()
    cdel = cache.delete(key)
    lock.release()
    if pdel is False and cdel is False:
        return "-1"
    return "ACK"

def get_timestamp(key, cache, persistent, lock):
    val = get(key, cache, persistent, lock)
    if val != "-1":
        v, t = val.split(":", 1)
        return t
    return None

def write(key, value, tag, cache, persistent, lock):
    ts = get_timestamp(key, cache, persistent, lock)
    value = value+":"+tag
    if ts:
        if time.is_greater(tag, ts):
            lock.acquire()
            cache.put(key, value) #key, value is in cache after get_timestamp()
            lock.release()
    else:
        put(key, value, cache, persistent, lock)
    return "ACK"

def acquire_lock(key, client_id, client_lock):
    if key in client_lock.values():
        #print('lock being held by ', list(client_lock.keys())[list(client_lock.values()).index(key)])
        return "LOCK_DENIED"
    else:
        client_lock[client_id] = key
        print('Granting lock')
        return "LOCK_GRANTED"
 
def release_lock(key, client_id, client_lock):
    client_lock[client_id] = ''
    return "ACK"

OP_FUNC_MAPPER = {
            'GET': get,
            'PUT': put,
            'INSERT': insert,
            'DELETE': delete,
            'GET-TS': get_timestamp,
            'WRITE': write,
            'ACQUIRE_LOCK' : acquire_lock,
            'RELEASE_LOCK' : release_lock
        }

def call_api(req, cache, persistent, lock, fine_grained_lock=None):
    if OP_FUNC_MAPPER.get(req.op):
        if req.op in ['GET', 'DELETE', 'GET-TS']:
            return OP_FUNC_MAPPER[req.op](req.key, cache, persistent, lock)
        elif req.op in ['ACQUIRE_LOCK', 'RELEASE_LOCK']:
            return OP_FUNC_MAPPER[req.op](req.key, req.value, fine_grained_lock)
        elif req.op in ['WRITE']:
            return OP_FUNC_MAPPER[req.op](req.key, req.value, req.tag, cache, persistent, lock)
        else:
            return OP_FUNC_MAPPER[req.op](req.key, req.value, cache, persistent, lock)
    else:
        return "-1"
