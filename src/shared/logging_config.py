# src/shared/logging_config.py

import logging
import logging.handlers
import multiprocessing
import sys
import os

LOG_DIR = "logs"

def log_listener_configurer(log_queue):
    root = logging.getLogger()
    file_handler = setup_file_handler()
    formatter = logging.Formatter('%(asctime)s %(processName)s %(name)s %(levelname)s %(message)s')
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

def log_worker_configurer(log_queue):
    queue_handler = logging.handlers.QueueHandler(log_queue)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(queue_handler)

def setup_file_handler():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    log_file = os.path.join(LOG_DIR, 'app_log.log')
    return logging.handlers.TimedRotatingFileHandler(filename=log_file, when='midnight', backupCount=7, encoding='utf-8')

def start_log_listener(log_queue):
    listener = multiprocessing.Process(target=log_listener, args=(log_queue,))
    listener.start()
    return listener

def log_listener(log_queue):
    listener_configurer = log_listener_configurer
    listener_configurer(log_queue)
    while True:
        try:
            record = log_queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception as e:
            print(f"Error in log listener: {str(e)}", file=sys.stderr)

def stop_log_listener(log_queue, listener):
    log_queue.put(None)
    listener.join()
