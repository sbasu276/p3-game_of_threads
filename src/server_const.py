# Needs to be changed in the client deployment config
SERVER_ID_MAP = {
        1: ["127.0.0.1", 65201],
        2: ["127.0.0.1", 65202], 
        3: ["127.0.0.1", 65203],
        4: ["127.0.0.1", 65204],
        5: ["127.0.0.1", 65205],
    }

# IP and port for central lock server
LOCK_SERVER_IP = ""
LOCK_SERVER_PORT = 

GET_TS = "GET-TS"
GET = "GET"

LOCK_ACQUIRE = "LOCK_ACQUIRE"
LOCK_RELEASE = "LOCK_RELEASE"
QUORUM_TIMEOUT = 1 #in seconds
