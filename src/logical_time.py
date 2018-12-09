def is_greater(time1, time2):
    _int1, _id1 = time1.split("-", 1)
    _int2, _id2 = time2.split("-", 1)
    if int(_int1) > int(_int2):
        return True
    elif int(_int1) == int(_int2):
        return int(_id1) > int(_id2)
    else:
        return False

def increment(time):
    _int, _id = time.split("-", 1)
    return str(int(_int)+1)+"-"+str(_id)

def get_max_ts(times):
    _max = "0-0"
    for time in times:
        if is_greater(time, _max):
            _max = time
    return _max
