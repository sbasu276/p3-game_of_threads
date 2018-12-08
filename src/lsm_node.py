import time

class LsmNode:
    def __init__(self, key, value, tombstone=False):
        self.key = key
        self.value = value # value is a string with format - "<actual_value>:(<integer>, <server-id>)"
        self.timestamp = time.time() #this is just server local time, not used for any comparison
        self.tombstone = tombstone
