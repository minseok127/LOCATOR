#!/usr/bin/python3

import sys, subprocess, signal
import time, threading
from pytz import timezone
import os, random
import argparse
import pathlib, datetime
import json

RESULT_BASE="./HTAP_results"

CH_BASE=""

OLTP_WORKER=12
OLAP_WORKER=4

RUN_TIME=20
RAMPUP_TIME=5

# Set device name to record iostat
IOSTAT_DEVICE="" # e.g., /dev/md0
TIME_INTERVAL=10

query_nums=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18]
query_dict={1:0, 2:1, 3:2, 4:3, 5:4, 6:5, 7:6, 8:7, 9:8, 10:9, 11:10, 12:11, 14:12, 15:13, 16:14, 18:15}

class DBSizeChecker(threading.Thread):
    def __init__(self, runtime, interval, data_path):
        threading.Thread.__init__(self)
        self.runtime = runtime
        self.interval = interval
        self.data_path = data_path

    def run(self):
        loop_cnt = (self.runtime * 60) // self.interval
        dbsize = open("./dbsize.txt", "w")

        for i in range(loop_cnt+1):
            retry_flag = True

            while retry_flag:
                try:
                    out_raw = subprocess.check_output("du -s %s/base/16384"%(self.data_path), shell=True, text=True)
                    retry_flag = False
                except:
                    retry_flag = True

            output = out_raw.split('\n')[:-1]
            dbsize.write("%-4d  %s\n"%(i * self.interval, output[0].split('\t')[0]))
            dbsize.flush()
            time.sleep(self.interval)

        dbsize.close()

def copy_db(params:str="", warehouse:int=500, mode:str="LOCATOR"):
    print("Copy db start ! (w: %d, m: %s)"%(warehouse, mode))
    params = " -w=%d -m=%s "%(warehouse, mode) + params
    subprocess.run(args=DB_SERVER_SCRIPT+"/load_data.sh" + params,
                   stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                   check=True, shell=True)
    print("Copy db finish !")

def compile_database(params=None):
    print("Compile database start ! (option:%s)"%(params))
    subprocess.run(args=DB_INSTALL_SCRIPT+"/install.sh" + params, 
                   stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                   check=True, shell=True)
    print("Compile database finish !")

def compile_shared_library(params=None):
    print("Compile %s start !"%(params))
    subprocess.run(args=DB_INSTALL_SCRIPT+"/install_shared_library.sh" + params, 
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True, shell=True)
    print("Compile %s finish !"%(params))

def run_server(params=None, cgroup: bool=False):
    print("Run server start")
    run_arg = DB_SERVER_SCRIPT+"/run_server.sh"
    if cgroup == True:
        run_args = "cgexec -g memory:/locator.slice %s"%(run_arg) + params
    else:
        run_args = run_arg + params
    subprocess.run(args=run_args,
                   stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                   check=True, shell=True)
    time.sleep(2)
    print("Run server finish")

def shutdown_server(params=None):
    print("Shutdown server start")
    subprocess.run(args=DB_SERVER_SCRIPT+"/shutdown_server.sh" + params,
                   stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
                   check=True, shell=True)
    print("Shutdown server finish")

def run_benchmark(args):
    global query_nums, query_dict
    global OLAP_WORKER, OLTP_WORKER, RUN_TIME, RAMPUP_TIME, IOSTAT_DEVICE, TIME_INTERVAL
    global CH_BASE, DB_BASE, DB_DATA, DB_CONFIG, DB_SCRIPT, DB_SERVER_SCRIPT, DB_CLIENT_SCRIPT, DB_INSTALL_SCRIPT
    global loop_cnts
    global latency_sum, latency_min, latency_max
    global IO_latency_sum, IO_latency_min, IO_latency_max
    global IO_amount_sum, IO_amount_min, IO_amount_max

    # Pleaf: 1 GiB
    # Ebi: 8 GiB
    compile_options = [" ",
                       " -DDIVA -DPLEAF_NUM_PAGE=262144 -DEBI_NUM_PAGE=1048576 ",
                       " -DDIVA -DPLEAF_NUM_PAGE=262144 -DEBI_NUM_PAGE=1048576 -DLOCATOR ",
                       " -DDIVA -DPLEAF_NUM_PAGE=262144 -DEBI_NUM_PAGE=1048576 -DLOCATOR ",
                       " -DDIVA -DPLEAF_NUM_PAGE=262144 -DEBI_NUM_PAGE=1048576 -DLOCATOR "]
    modes = ["VANILLA", "DIVA", "LOCATOR", "LOCATOR", "LOCATOR"]

    DATE = datetime.datetime.now(timezone('UTC')).strftime("%y-%m-%d_%H:%M:%S")
    RESULT_DIR = RESULT_BASE + "/" + DATE + "/"
    pathlib.Path(RESULT_DIR).mkdir(parents=True, exist_ok=True)

    with open(RESULT_DIR + "arg_list.json", 'w') as f:
        json.dump(args.__dict__, f, indent=4)

    LOCATOR_BASE=args.locator_path

    CH_BASE="%s/chbenchmark"%(LOCATOR_BASE)
    DB_BASE="%s/PostgreSQL"%(LOCATOR_BASE)

    DB_DATA="%s/data_current"%(args.data_path)
    DB_CONFIG="%s/config"%(DB_BASE)

    DB_SCRIPT="%s/script"%(DB_BASE)
    DB_SERVER_SCRIPT="%s/script_server"%(DB_SCRIPT)
    DB_CLIENT_SCRIPT="%s/script_client"%(DB_SCRIPT)
    DB_INSTALL_SCRIPT="%s/script_install"%(DB_SCRIPT)
    
    QUERIES="%s/evaluation/CH_benCHmark/queries_LOCATOR.txt"%(LOCATOR_BASE)

    if args.coredump:
        run_args = "-c"
    else:
        run_args = ""

    IOSTAT_DEVICE = args.iostat_device

    if IOSTAT_DEVICE == "":
        record_iostat = False
    else:
        record_iostat = True

    plot = args.plot

    prev_m = -1

    os.system("rm /tmp/hdbtcount.log")
    os.system("rm /tmp/hdbxtprofile.log")
    os.system("rm ./iostat.json")
    os.system("rm ./dbsize.txt")
    os.system("rm -r %s/results"%(CH_BASE))

    print("PostgreSQL standard benchmark start")

    for m in range(len(modes)):
        if args.systems[m] == '0':
            continue

        if prev_m != -1:
            # Rest the device
            print("sleep 10 minutes...")
            time.sleep(600)

        print("mode: %s"%(modes[m]))
            
        if m > 2:
            print("w/o \"enable_prefetch\"")
            
        if m == 4:
            print("w/o \"enable_uring_partitioning\"")

        os.system("mkdir %s/results"%(CH_BASE))

        # Compile the new system
        if prev_m < 2:
            compile_database(params=(" -DIO_AMOUNT" + compile_options[m] + args.compile_option))

            if m > 1:
                print("sleep 5 seconds...")
                time.sleep(5)
                compile_shared_library(params=" pg_hint_plan")

            print("sleep 1 minutes...")
            time.sleep(60)

        prev_m = m

        copy_db(params=" -d=%s"%(args.data_path), warehouse=args.warehouse, mode=modes[m])
            
        if m > 2:
            os.system("sed -i \"s|#enable_prefetch = on|enable_prefetch = off|g\" %s/data_current/postgresql.conf"%(args.data_path))
            print("GUC disabled: \"enable_prefetch\"")
            
        if m == 4:
            os.system("sed -i \"/enable_prefetch/a\enable_uring_partitioning = off\" %s/data_current/postgresql.conf"%(args.data_path))
            print("GUC disabled: \"enable_uring_partitioning\"")

        # Rest the device
        print("sleep 5 minutes...")
        time.sleep(300)

        os.system("../../drop_cache")

        print("sleep 1 minutes...")
        time.sleep(60)

        run_server(params=" -d=%s "%(args.data_path)+run_args, cgroup=args.cgroup)

        # Check for error
        tail = subprocess.Popen(["tail -f %s/postgresql.log | grep -e 'TRAP' -e 'ERROR' -e 'FATAL' -e 'PANIC' -e 'segfault' -e 'ABORT' -e 'terminated by signal'"%(DB_BASE)], start_new_session=True, shell=True)

        print("sleep 30 seconds...")
        time.sleep(30)

        # Start to record the size of dataset
        db_size_checker = DBSizeChecker(RUN_TIME, TIME_INTERVAL, DB_DATA)
        db_size_checker.start()

        # Start to record iostat
        if record_iostat:
            iostat = subprocess.Popen(["iostat -p %s -x -m %d -t -o JSON >> iostat.json"%(IOSTAT_DEVICE, TIME_INTERVAL)], start_new_session=True, shell=True)
        
        # Run CH-benCHmark
        os.system("%s/run.sh 4.3 %s %d %d %d %d %d %s %s"%(CH_BASE, modes[m], OLTP_WORKER, OLAP_WORKER, RUN_TIME, RAMPUP_TIME, args.warehouse, LOCATOR_BASE, QUERIES))
        
        # Finish recording iostat
        if record_iostat:
            os.killpg(iostat.pid, signal.SIGINT)

        print("sleep 2 minutes...")
        time.sleep(120)

        shutdown_server(params=" -d=%s"%(args.data_path))

        os.killpg(tail.pid, signal.SIGINT)
        db_size_checker.join()

        time.sleep(10)

        directory = "%s/results_%s"%(RESULT_DIR, modes[m])

        if m == 3:
            directory = "%s_wo_prefetch"%(directory)
        elif m == 4:
            directory = "%s_wo_uring"%(directory)

        os.system("mv %s/results %s"%(CH_BASE, directory))
        os.system("mv ./dbsize.txt %s"%(directory))
        os.system("cp %s/postgresql.log %s/"%(DB_BASE, directory))

        # Statistics the number of requests from the device
        if record_iostat:
            with open("./iostat.json", "r") as f:
                iostat = json.load(f)

            iostat_requests = open("%s/iostat.txt"%(directory), "w")
            statistics = iostat["sysstat"]["hosts"][0]["statistics"]

            iostat_requests.write("timestamp, r/s, w/s, sum, rMB/s, wMB/s, util\n")
            timestamp = 0
            for stat in statistics:
                disk = stat["disk"][0]
                read_req = float(disk["r/s"])
                write_req = float(disk["w/s"])
                read_amt = float(disk["rMB/s"])
                write_amt = float(disk["wMB/s"])
                util = float(disk["util"])
                iostat_requests.write("%-4d  %12.2f  %12.2f  %12.2f  %12.2f  %12.2f  %12.2f\n"%(timestamp * TIME_INTERVAL, read_req, write_req, read_req + write_req, read_amt, write_amt, util))
                timestamp += 1

            iostat_requests.close()

            os.system("mv ./iostat.json %s"%(directory))

        # Statistics the tpm
        with open("%s/hdbtcount.log"%(directory), "r") as f:
            lines = f.readlines()
        tpm = open("%s/tpm.txt"%(directory), "w")
        
        for i in range(2, len(lines)):
            chunks = lines[i].split()
            tpm.write("%-4d  %d\n"%((i-2)*TIME_INTERVAL, int(chunks[0])))

        tpm.close()

        # Statistics the latency of each query
        with open("%s/ch_queries_%s.txt"%(directory, modes[m]), "r") as f:
            lines = f.readlines()
        latency_file = open("%s/query_latency.txt"%(directory), "w")

        query_cnt = [0 for _ in range(len(query_nums))]
        query_latency_sum = [0.0 for _ in range(len(query_nums))]
        query_latency_min = [1000000.0 for _ in range(len(query_nums))]
        query_latency_max = [0.0 for _ in range(len(query_nums))]
        for i in range(len(lines)):
            chunks = lines[i].split()
            query_idx = query_dict[int(chunks[0])]
            latency = float(chunks[3])
            query_cnt[query_idx] += 1
            query_latency_sum[query_idx] += latency

            if query_latency_min[query_idx] > latency:
                query_latency_min[query_idx] = latency
            if query_latency_max[query_idx] < latency:
                query_latency_max[query_idx] = latency

        latency_file.write("query no, avg, min, max, sum\n")
        for i in range(len(query_nums)):
            if query_cnt[i] != 0:
                latency_file.write("%-4d  %12.3f  %12.3f  %12.3f  %12.3f\n"%(query_nums[i], query_latency_sum[i]/query_cnt[i], query_latency_min[i], query_latency_max[i], query_latency_sum[i]))
            else:
                latency_file.write("%-4d  %12.3f  %12.3f  %12.3f  %12.3f\n"%(query_nums[i], 0.0, 0.0, 0.0, 0.0))

        latency_file.close()

        # Statistics the amount of read IO
        out_raw = subprocess.check_output("cat %s/postgresql.log | grep -e 'IO_AMOUNT'"%(directory), shell=True, text=True)
        lines = out_raw.split('\n')[:-1]
        readIO_file = open("%s/readIO.txt"%(directory), "w")

        query_cnt = [0 for _ in range(len(query_nums))]
        query_read_amount_sum = [0 for _ in range(len(query_nums))]
        query_read_amount_min = [9999999999999999 for _ in range(len(query_nums))]
        query_read_amount_max = [0 for _ in range(len(query_nums))]
        for i in range(len(lines)):
            chunks = lines[i].split(', ')
            query_idx = query_dict[int(chunks[0].split()[1])]
            read_amount = int(chunks[1].split(': ')[1]) * 8192
            query_cnt[query_idx] += 1
            query_read_amount_sum[query_idx] += read_amount

            if query_read_amount_min[query_idx] > read_amount:
                query_read_amount_min[query_idx] = read_amount
            if query_read_amount_max[query_idx] < read_amount:
                query_read_amount_max[query_idx] = read_amount

        readIO_file.write("query no, nbytes (avg), nbytes (min), nbytes (max), nbytes (sum)\n")
        for i in range(len(query_nums)):
            if query_cnt[i] != 0:
                readIO_file.write("%-4d  %12d  %12d  %12d  %12d\n"%(query_nums[i], query_read_amount_sum[i]/query_cnt[i], query_read_amount_min[i], query_read_amount_max[i], query_read_amount_sum[i]))
            else:
                readIO_file.write("%-4d  %12d  %12d  %12d  %12d\n"%(query_nums[i], 0, 0, 0, 0))

        readIO_file.close()

    print("run standard benchmark done")

    # Link the result
    os.system("rm -rf %s/latest"%(RESULT_BASE))
    os.symlink(DATE, "%s/latest"%(RESULT_BASE), target_is_directory=True)

    if plot:
        time.sleep(1)

        # Plot the graphs
        os.system("gnuplot HTAP_11_a.gp")
        os.system("gnuplot HTAP_11_b.gp")
        os.system("gnuplot HTAP_11_c.gp")

if __name__ == "__main__":
    # Parser
    parser = argparse.ArgumentParser(description="CH-benCHmark Arguments...")

    pgsql_parser = parser.add_argument_group('pgsql', 'postgresql options')
    options_parser = parser.add_argument_group('options', 'other options')

    options_parser.add_argument("--compile-option", default="", help="compile options")
    options_parser.add_argument("--systems", default="11100", help="Vanilla, DIVA, LOCATOR, LOCATOR w/o enable_prefetch, LOCATOR w/o enable_prefetch and enable_uring_partitioning")
    options_parser.add_argument("--cgroup", action='store_true', default=False, help="is cgroup applied")
    options_parser.add_argument("--warehouse", type=int, default="500", help="warehouse")
    options_parser.add_argument("--coredump", action='store_true', default=False, help="coredump")
    options_parser.add_argument("--locator-path", default="~/LOCATOR/", help="locator path")
    options_parser.add_argument("--data-path", default="~/LOCATOR/PostgreSQL/data/", help="locator path")
    options_parser.add_argument("--plot", action='store_true', default=False, help="plotting")
    options_parser.add_argument("--iostat-device", default="", help="device for iostat")
    options_parser.add_argument("--comment", default="", help="options comment")

    pgsql_parser.add_argument("--pgsql-host", default="localhost", help="pgsql host")
    pgsql_parser.add_argument("--pgsql-db", default="locator", help="pgsql database")

    args=parser.parse_args()

    run_benchmark(args)
