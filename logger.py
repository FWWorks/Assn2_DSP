import logging
import time

loggers = {}

def get_logger(log_file):

    # create logger
    logger_name = log_file
    if logger_name in loggers:
        return loggers[logger_name]
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    # create file handler
    log_path = log_file
    fh = logging.FileHandler(log_path, 'w')
    sh = logging.StreamHandler()

    # create formatter
    # fmt = "%(asctime)-15s %(levelname)s %(filename)s %(lineno)d %(process)d %(message)s"
    fmt = "%(asctime)-15s %(levelname)s %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)
    # add handler and formatter to logger
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(sh)

    loggers[logger_name] = logger

    return logger

'''
time stamp: 2019-02-05 00:21:00.254
stamp: 20190205002100254
'''
def get_time_stamp():
    ct = time.time()
    local_time = time.localtime(ct)
    data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
    data_secs = (ct - int(ct)) * 1000
    time_stamp = "%s.%03d" % (data_head, data_secs)
    print(time_stamp)
    stamp = ("".join(time_stamp.split()[0].split("-"))+"".join(time_stamp.split()[1].split(":"))).replace('.', '')
    print(stamp)
    return time_stamp,stamp