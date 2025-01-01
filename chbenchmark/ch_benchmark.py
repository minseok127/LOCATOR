#!/usr/bin/python3

import sys
import random
import atexit
import signal
import subprocess

from threading import Thread
from threading import Lock
from time import sleep
import time

ch_queries = []

sent_query_amount = 0
is_terminated = False
file_suffix="0"
lock = Lock()

RANDOM_SEED = 123
query_nums=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18]

def start_ch_thread(start_index,pgpath):
    global sent_query_amount
    global ch_queries
    global is_terminated

    size = len(ch_queries)

    cur_index = start_index
    while not is_terminated:
        return_code = send_query(ch_queries[cur_index], cur_index, pgpath)
        # if there was an error, we will retry the same query
        if return_code != 0:
            continue

        sent_query_amount += 1

        # Send same query
        cur_index += 1
        cur_index %= size

def send_query(query,cur_index,pgpath):
    global coord_ip, query_nums
    pg = ['%s/pgsql/bin/psql'%(pgpath), '-P', 'pager=off', '-v', 'ON_ERROR_STOP=1', '-h', coord_ip, '-c', query]

    start_time = int(round(time.time() * 1000))
    return_code = subprocess.call(pg)
    end_time = int(round(time.time() * 1000))

    with open("./results/ch_queries_{}.txt".format(file_suffix), "a") as f:
        res = "{} finished in {} milliseconds".format(query_nums[cur_index], end_time - start_time)
        f.write(res+"\n")
        print(res, file=sys.stderr)

    return return_code

def give_stats(sent_query_amount, time_lapsed_in_secs):
    with open("./results/ch_results_{}.txt".format(file_suffix), "w") as f:
        f.write("queries {} in {} seconds\n".format(sent_query_amount, time_lapsed_in_secs))
        f.write("QPH {}\n".format(3600.0 * sent_query_amount / time_lapsed_in_secs))

def get_curtime_in_seconds():
    return int(round(time.time()))


def terminate():
    global is_terminated
    global sent_query_amount
    global start_time_in_secs

    end_time_in_secs = get_curtime_in_seconds()

    give_stats(sent_query_amount, end_time_in_secs - start_time_in_secs)

    is_terminated = True

class GracefulKiller:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        global is_terminated
        self.kill_now = True
        print("got a kill signal")
        terminate()

if __name__ == "__main__":

    thread_count = int(sys.argv[1])
    coord_ip = sys.argv[2]
    initial_sleep_in_mins=int(sys.argv[3])
    file_suffix=sys.argv[4]
    pgpath = sys.argv[5]
    QUERIES = sys.argv[6]

    with open(QUERIES, 'r') as f:
        ch_queries = f.read().split("////")

    random.seed(RANDOM_SEED)

    # start_indexes = list(range(0, len(ch_queries)))
    # random.shuffle(start_indexes)

    interval = len(ch_queries) // thread_count

    jobs = [
        Thread(target = start_ch_thread, args=((i*interval) % len(ch_queries), pgpath))
        for i in range(0, thread_count)]

    sleep(initial_sleep_in_mins * 60)

    start_time_in_secs = get_curtime_in_seconds()
    for j in jobs:
        j.start()

    killer = GracefulKiller()
    while not killer.kill_now:
        time.sleep(10)
