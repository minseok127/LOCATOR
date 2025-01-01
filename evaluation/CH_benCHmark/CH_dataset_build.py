#!/usr/bin/python3

import sys, subprocess, signal
import time, threading
from pytz import timezone
import os, random
import argparse
import pathlib, datetime
import psycopg2

CH_BASE=""

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

def init_server(params=None):
    print("Init server start !")
    subprocess.run(args=DB_SERVER_SCRIPT+"/init_server.sh" + params,
        check=True, shell=True)
    print("Init server finish !")

def run_server(params=None):
    print("Run server start")
    run_arg = DB_SERVER_SCRIPT+"/run_server.sh"
    subprocess.run(args=run_arg + params, 
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

def build_dataset(args):
    global CH_BASE, DB_BASE, DB_DATA, DB_CONFIG, DB_SCRIPT, DB_SERVER_SCRIPT, DB_CLIENT_SCRIPT, DB_INSTALL_SCRIPT

    compile_options = [" ", " ", " -DDIVA ", " -DDIVA -DLOCATOR "]
    modes = ["VANILLA", "CITUS", "DIVA", "LOCATOR"]

    LOCATOR_BASE=args.locator_path

    CH_BASE="%s/chbenchmark"%(LOCATOR_BASE)
    DB_BASE="%s/PostgreSQL"%(LOCATOR_BASE)

    DB_DATA="%s/data_current"%(args.data_path)
    DB_CONFIG=DB_BASE + "/config"

    DB_SCRIPT=DB_BASE + "/script"
    DB_SERVER_SCRIPT=DB_SCRIPT + "/script_server"
    DB_CLIENT_SCRIPT=DB_SCRIPT + "/script_client"
    DB_INSTALL_SCRIPT=DB_SCRIPT + "/script_install"

    if args.coredump:
        run_args = "-c"
    else:
        run_args = ""

    prev_m = -1

    print("PostgreSQL building dataset start")

    for m in range(len(modes)):
        if args.systems[m] == '0':
            continue

        print("mode: %s"%(modes[m]))

        # Compile the new system
        if not (prev_m == 0 and m == 1):
            compile_database(params=(compile_options[m] + args.compile_option))
            print("sleep 5 seconds...")
            time.sleep(5)

            if m < 2:
                compile_shared_library(params=" citus")
            elif m == 3:
                compile_shared_library(params=" pg_hint_plan")

            print("sleep 5 seconds...")
            time.sleep(5)

        prev_m = m

        init_server(params=" -d=%s"%(args.data_path))

        print("sleep 5 seconds...")
        time.sleep(5)

        # Set the config for this system
        os.system("rm %s/postgresql.conf"%(DB_DATA))
        os.system("cp %s/postgresql_%s.conf %s/postgresql.conf"%(DB_CONFIG, modes[m], DB_DATA))

        run_server(params=" -d=%s "%(args.data_path)+run_args)

        # Check for error
        tail = subprocess.Popen(["tail -f %s/postgresql.log | grep -e 'TRAP' -e 'ERROR' -e 'FATAL' -e 'PANIC' -e 'segfault' -e 'ABORT' -e 'terminated by signal'"%(DB_BASE)], start_new_session=True, shell=True)
        time.sleep(1)

        # Set the environment
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", "postgres", "-c", "CREATE DATABASE locator;"])
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", "locator", "-c", "CREATE EXTENSION intarray;"])

        if m == 3:
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", "locator", "-c", "CREATE ACCESS METHOD hap TYPE TABLE HANDLER locator_hap_handler;"])
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", "locator", "-c", "CREATE ACCESS METHOD locator TYPE TABLE HANDLER locatoram_handler;"])

        print("sleep 5 seconds...")
        time.sleep(5)

        if m == 3:
            is_locator = "true"
        else:
            is_locator = "false"

        if m == 1 or m == 3:
            is_columnar = "true"
        else:
            is_columnar = "false"

        # Build the dataset
        os.system("%s/build.sh 4.3 %s true true %s %d %d %s"%(CH_BASE, is_locator, is_columnar, args.warehouse, args.worker, LOCATOR_BASE))

        if m == 3:
            # Wait for the partitioning worker to finish
            print("sleep 5 minutes...")
            time.sleep(300)
        else:
            print("sleep 10 seconds...")
            time.sleep(10)
            
        # Citus
        if m == 1:
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "CREATE EXTENSION citus;"])
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "SELECT alter_table_set_access_method('orders', 'columnar');"])
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "SELECT alter_table_set_access_method('order_line', 'columnar');"])
            print("orders, order_line were converted to columnar")

        # VACUUM
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE region;"])
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE nation;"])
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE supplier;"])
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE district;"])
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE history;"])
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE warehouse;"])
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE new_order;"])
        subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE item;"])

        if m < 3:
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE customer;"])
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE stock;"])
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE orders;"])
            subprocess.call(["%s/pgsql/bin/psql"%(DB_BASE), "-h", args.pgsql_host, "-p", "5555", "-d", args.pgsql_db, "-c", "VACUUM ANALYZE order_line;"])
        else:
            db = psycopg2.connect(
                host=args.pgsql_host,
                dbname=args.pgsql_db,
                port=5555
            )
            cursor = db.cursor()

            # Disable prefetching(io_uring) for setting hint bits
            cursor.execute("SET enable_prefetch=off;")

            # LOCATOR does not support VACUUM, so simply set hint bits
            cursor.execute("SELECT COUNT(*) FROM customer;")
            cursor.fetchall()
            print("customer was scanned")
            cursor.execute("SELECT COUNT(*) FROM stock;")
            cursor.fetchall()
            print("stock was scanned")
            cursor.execute("SELECT COUNT(*) FROM orders;")
            cursor.fetchall()
            print("orders was scanned")
            cursor.execute("SELECT COUNT(*) FROM order_line;")
            cursor.fetchall()
            print("order_line was scanned")

            db.close()

        print("sleep 10 seconds...")
        time.sleep(10)

        shutdown_server(params=" -d=%s"%(args.data_path))
        os.killpg(tail.pid, signal.SIGINT)

        # Restart the server to transform remained partitions
        if m == 3:
            print("sleep 5 seconds...")
            time.sleep(5)

            run_server(params=" -d=%s "%(args.data_path)+run_args)

            print("sleep 10 seconds...")
            time.sleep(10)

            shutdown_server(params=" -d=%s"%(args.data_path))

        os.system("mv %s/data_current %s/data_%d_%s"%(args.data_path, args.data_path, args.warehouse, modes[m]))


    print("building dataset done")


if __name__ == "__main__":
    # Parser
    parser = argparse.ArgumentParser(description="CH-benCHmark Arguments...")

    pgsql_parser = parser.add_argument_group('pgsql', 'postgresql options')
    options_parser = parser.add_argument_group('options', 'other options')

    options_parser.add_argument("--compile-option", default="", help="compile options")
    options_parser.add_argument("--systems", default="1111", help="Vanilla, Citus, DIVA, LOCATOR")
    options_parser.add_argument("--coredump", action='store_true', default=False, help="coredump")
    options_parser.add_argument("--warehouse", type=int, default="500", help="warehouse")
    options_parser.add_argument("--worker", type=int, default="125", help="warehouse")
    options_parser.add_argument("--locator-path", default="~/LOCATOR", help="locator path")
    options_parser.add_argument("--data-path", default="~/LOCATOR/PostgreSQL/data", help="locator path")

    pgsql_parser.add_argument("--pgsql-host", default="localhost", help="pgsql host")
    pgsql_parser.add_argument("--pgsql-db", default="locator", help="pgsql database")

    args=parser.parse_args()

    build_dataset(args)
