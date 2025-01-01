#!/usr/bin/python3

import sys, subprocess, signal
import time, threading
from pytz import timezone
import os, random
import argparse
import pathlib, datetime
import json
import psycopg2

RESULT_BASE="./OLAP_results"

query_nums=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 18]
query_dict={1:0, 2:1, 3:2, 4:3, 5:4, 6:5, 7:6, 8:7, 9:8, 10:9, 11:10, 12:11, 14:12, 15:13, 16:14, 18:15}

ch_queries = []

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
    global query_nums, query_dict, ch_queries
    global DB_BASE, DB_DATA, DB_CONFIG, DB_SCRIPT, DB_SERVER_SCRIPT, DB_CLIENT_SCRIPT, DB_INSTALL_SCRIPT
    global loop_cnts
    global latency_sum, latency_min, latency_max
    global IO_latency_sum, IO_latency_min, IO_latency_max
    global IO_amount_sum, IO_amount_min, IO_amount_max

    # Pleaf: 1 GiB
    # Ebi: 8 GiB
    compile_options = [" ",
                       " ",
                       " -DDIVA -DPLEAF_NUM_PAGE=262144 -DEBI_NUM_PAGE=1048576 ",
                       " -DDIVA -DPLEAF_NUM_PAGE=262144 -DEBI_NUM_PAGE=1048576 -DLOCATOR ",
                       " -DDIVA -DPLEAF_NUM_PAGE=262144 -DEBI_NUM_PAGE=1048576 -DLOCATOR "]
    modes = ["VANILLA", "CITUS", "DIVA", "LOCATOR", "LOCATOR"]

    DATE = datetime.datetime.now(timezone('UTC')).strftime("%y-%m-%d_%H:%M:%S")
    RESULT_DIR = RESULT_BASE + "/" + DATE
    pathlib.Path(RESULT_DIR).mkdir(parents=True, exist_ok=True)

    with open(RESULT_DIR + "/arg_list.json", 'w') as f:
        json.dump(args.__dict__, f, indent=4)

    LOCATOR_BASE=args.locator_path

    DB_BASE="%s/PostgreSQL"%(LOCATOR_BASE)

    DB_DATA="%s/data_current"%(args.data_path)
    DB_CONFIG="%s/config"%(DB_BASE)

    DB_SCRIPT="%s/script"%(DB_BASE)
    DB_SERVER_SCRIPT="%s/script_server"%(DB_SCRIPT)
    DB_CLIENT_SCRIPT="%s/script_client"%(DB_SCRIPT)
    DB_INSTALL_SCRIPT="%s/script_install"%(DB_SCRIPT)
    
    QUERIES="%s/evaluation/CH_benCHmark/queries_LOCATOR.txt"%(LOCATOR_BASE)
    with open(QUERIES, 'r') as f:
        ch_queries = f.read().split("////")

    if args.coredump:
        run_args = "-c"
    else:
        run_args = ""

    plot = args.plot

    prev_m = -1

    print("PostgreSQL standard benchmark start")

    for m in range(len(modes)):
        if args.systems[m] == '0':
            continue

        print("mode: %s"%(modes[m]))
            
        if m == 4:
            print("w/o \"enable_prefetch\"")

        # Compile the new system
        if not (prev_m == 0 and m == 1 or prev_m > 2):
            compile_database(params=(" -DIO_AMOUNT" + compile_options[m] + args.compile_option))
            print("sleep 5 seconds...")
            time.sleep(5)

            if m < 2:
                compile_shared_library(params=" citus")
            elif m > 2:
                compile_shared_library(params=" pg_hint_plan")

            print("sleep 1 minutes...")
            time.sleep(60)

        prev_m = m

        copy_db(params=" -d=%s"%(args.data_path), warehouse=args.warehouse, mode=modes[m])
            
        if m == 4:
            os.system("sed -i \"s|#enable_prefetch = on|enable_prefetch = off|g\" %s/data_current/postgresql.conf"%(args.data_path))
            print("GUC disabled: \"enable_prefetch\"")

        # Rest the device
        print("sleep 5 minutes...")
        time.sleep(300)

        directory = "%s/results_%s"%(RESULT_DIR, modes[m])
        
        if m == 4:
            directory = "%s_wo_prefetch"%(directory)

        os.system("mkdir %s"%(directory))

        latency_file = open("%s/query_latency.txt"%(directory), "w")
        latency_file.write("query no, latency\n")

        readIO_file = open("%s/readIO.txt"%(directory), "w")
        readIO_file.write("query no, total, reduced by HP, reduced by VP\n")

        for i in range(len(ch_queries)):
            query = ch_queries[i]
            os.system("../../drop_cache")

            print("sleep 10 seconds...")
            time.sleep(10)

            run_server(params=" -d=%s "%(args.data_path)+run_args, cgroup=args.cgroup)

            print("sleep 10 seconds...")
            time.sleep(10)
        
            db = psycopg2.connect(
                host=args.pgsql_host,
                dbname=args.pgsql_db,
                port=5555
            )

            db.autocommit = True
            cursor = db.cursor()
            
            start_time = int(round(time.time() * 1000))
            cursor.execute(query)
            cursor.fetchall()
            end_time = int(round(time.time() * 1000))
            
            db.close()

            print("sleep 10 seconds...")
            time.sleep(10)

            shutdown_server(params=" -d=%s"%(args.data_path))

            print("Query %-2d took %d milliseconds"%(query_nums[i], (end_time - start_time)))
            latency_file.write("%-2d  %8d\n"%(query_nums[i], (end_time - start_time)))

            out_raw = subprocess.check_output("cat %s/postgresql.log | grep -e 'IO_AMOUNT'"%(DB_BASE), shell=True, text=True)
            lines = out_raw.split('\n')[:-1]
            chunks = lines[0].split(', ')
            read_amount = int(chunks[1].split(': ')[1]) * 8192
            reduced_by_HP = int(chunks[2].split(': ')[1]) * 8192
            reduced_by_VP = int(chunks[3].split(': ')[1]) * 8192
            
            print("read amount: %12d, reduced by HP: %12d, reduced by VP: %12d"%(read_amount, reduced_by_HP, reduced_by_VP))
            readIO_file.write("%-2d  %12d  %12d  %12d\n"%(query_nums[i], read_amount, reduced_by_HP, reduced_by_VP))

        latency_file.close()
        readIO_file.close()

        time.sleep(10)

    print("run standard benchmark done")

    # Link the result
    os.system("rm -rf %s/latest"%(RESULT_BASE))
    os.symlink(DATE, "%s/latest"%(RESULT_BASE), target_is_directory=True)
        
    if plot:
        time.sleep(1)

        # Plot the graphs
        os.system("gnuplot OLAP_7_a.gp")
        os.system("gnuplot OLAP_7_b.gp")

if __name__ == "__main__":
    # Parser
    parser = argparse.ArgumentParser(description="CH-benCHmark Arguments...")

    pgsql_parser = parser.add_argument_group('pgsql', 'postgresql options')
    options_parser = parser.add_argument_group('options', 'other options')

    options_parser.add_argument("--compile-option", default="", help="compile options")
    options_parser.add_argument("--systems", default="11110", help="Vanilla, Citus, DIVA, LOCATOR, LOCATOR w/o enable_prefetch")
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
