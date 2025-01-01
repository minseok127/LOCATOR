import sys, subprocess, signal
import psycopg2
import os, random
import argparse
import time
import re
from concurrent.futures import ThreadPoolExecutor
import threading
import clickhouse_connect
from pathlib import Path

# Cgroup memory setting directory
CGROUP_DIR = "/sys/fs/cgroup/memory"

def drop_os_cache(args):
    # Drop OS cache
    command = f"sh -c 'echo 3 > /proc/sys/vm/drop_caches'"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

def mount_cgroup(args):
    global CGROUP_DIR

    if not os.path.exists(CGROUP_DIR):
        # Make cgroup directory
        command = f"mkdir {CGROUP_DIR}"
        subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

    print("Mount cgroup")
    command = "mount -t cgroup -o memory postgres /sys/fs/cgroup/memory"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

def umount_cgroup(args):
    global CGROUP_DIR

    print("Umount cgroup")
    command = "umount /sys/fs/cgroup/memory"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

    # Remove cgroup directory
    command = f"rmdir {CGROUP_DIR}"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

def adjust_vm_swappiness(args):
    # Set swappiness to 0
    command = f"sysctl vm.swappiness=0"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

def is_process_running(pid):
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    else:
        return True

def activate_cgroup(args):
    global CGROUP_DIR

    background_pids = []
    limit_bytes = 0

    if not os.path.exists(f"{CGROUP_DIR}/postgres"):
        # Make cgroup directory
        command = f"mkdir {CGROUP_DIR}/postgres"
        subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

    # Set cgroup's swappiness
    command = f"sh -c 'echo 0 > {CGROUP_DIR}/memory.swap.max'"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

    # Get pids of background processes
    result = subprocess.run( \
                args=["ps -ef | grep clickhouse | grep -v python | awk '{print $2;}'"], \
                capture_output=True, text=True, check=True, shell=True)
    background_pids = result.stdout.splitlines()

    for pid in background_pids:
        if not is_process_running(pid):
            continue
        # Add processes to cgroup
        command = f"sh -c 'echo '{pid}' >> {CGROUP_DIR}/postgres/cgroup.procs'"
        subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    # Set memory limit
    limit_bytes = int(args.memory_limit * 1024 * 1024 * 1024)
    command = f"sh -c 'echo '{limit_bytes}' > {CGROUP_DIR}/memory.max'"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

def deactivate_cgroup(args):
    result = subprocess.run( \
                    args=["ps -ef | grep clickhouse | grep -v python | awk '{print $2;}'"], \
                    capture_output=True, text=True, check=True, shell=True)
    background_pids = result.stdout.splitlines()

    for pid in background_pids:
        # Kill processes
        subprocess.run(args=[f"kill -9 {pid}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, check=False)

    time.sleep(3)


def execute_target_queries(args, query_string):
    cursor = clickhouse_connect.get_client(
                host='localhost', 
                database=args.db_name,
                send_receive_timeout=3000,
                connect_timeout=3000)

    # OLAP optimization settings
    query_settings = {
                        "max_threads": 1,                      # Number of threads for parallel query execution
                        "max_memory_usage": 32 * (1024**3),      # Set memory usage limit (10GB)
                        "allow_experimental_join_condition": 1, # Allow experimental join condition
                    }

    print(f"Query: {query_string}")

    # Run query and measure the latency
    start_time = time.time()
    result = cursor.query(query_string, settings=query_settings)
    end_time = time.time()

    # Calculate latency
    latency = end_time - start_time
    cursor.close()

    return latency

def evaluate(args):
    global CLICKHOUSE_BASE
    target_olap_queries = get_olap_queries()

    # Make directory for result.
    file_name = 'clickhouse'
    eval_folder_name = 'OLAP_ONLY'
    if not os.path.exists(f'./result/{eval_folder_name}'):
        os.makedirs(f'./result/{eval_folder_name}')

    with open(f'./result/{eval_folder_name}/{file_name}.dat', 'w') as f:
        f.write(f"#query_number \t\t latency(ms)\n")

    with open(f'./result/{eval_folder_name}/{file_name}_io.dat', 'w') as f:
        f.write(f"#query no, total, reduced by HP, reduced by VP\n")

    for key, value in target_olap_queries.items(): 
        try:
            # Remove existing logfile in "$CLICKHOUSE_BASE/clickhouse/build/programs/logfile"
            os.system(f"rm -f {CLICKHOUSE_BASE}/clickhouse/build/programs/logfile")

            # Run server
            run_server()

            # Drop OS cache
            drop_os_cache(args)
            # Activate cgroup
            activate_cgroup(args)

            print(f"Start running query {key}")
            latency = execute_target_queries(args, value)
        
            with open(f'./result/{eval_folder_name}/{file_name}.dat', 'a') as f:
                f.write(f"{key} \t\t {latency * 1000.0}\n")

            # Get IO amount from logfile.
            read_io_amount = 0
            with open(f"{CLICKHOUSE_BASE}/clickhouse/build/programs/logfile", 'r') as f:
                for line in f:
                    if "EVAL_IO_AMOUNT" in line and "endReadIOAmountProfile" in line:
                        words = line.split()
                        read_io_amount += int(words[-1])

                        print(f"IO amount: {read_io_amount}")

            with open(f'./result/{eval_folder_name}/{file_name}_io.dat', 'a') as f:
                f.write(f"{key}\t{read_io_amount}\t0\t0\n")


            time.sleep(3)
        except Exception as e:
            print(e)
        finally:
            # Deactivate cgroup and shutdown server
            deactivate_cgroup(args)
            shutdown_server()

def run_server(params=None):
    subprocess.run(args=str(Path(CLICKHOUSE_SCRIPT, "run_server.sh")),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        check=True, shell=True)
        
    time.sleep(5)
    print("ClickHouse server started")

def shutdown_server(params=None):
    subprocess.run(args=str(Path(CLICKHOUSE_SCRIPT, "shutdown_server.sh")),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        check=False)
    time.sleep(5)
    print("ClickHouse server stopped")

def get_olap_queries():
    global CLICKHOUSE_SCRIPT

    olap_file_path = Path(CLICKHOUSE_SCRIPT, "..", "..", "..", "CH_benCHmark", "queries_LOCATOR.txt" )
    olap_queries = {}

    with open(olap_file_path, 'r') as f:
        raw_text = f.read()
        raw_olap_queries = raw_text.split('////')

        for olap_query in raw_olap_queries:
            # Find "--  Q"
            query_num = re.search(r'--\s+Q\d+', olap_query)
            query_num = query_num.group(0)
            query_num = query_num.split(' ')[1]

            olap_queries[query_num] = olap_query[:-1]

    return olap_queries


def main():
    parser = argparse.ArgumentParser(description="Create dimension and fact tables in PostgreSQL and populate them with data.")
    
    # XXX: Number of attributes in the fact table should be at least 8
    parser.add_argument('--repository-path', type=str, required=True, help="The repository path for this project")
    parser.add_argument('--compile-options', type=str, required=False, default=" ", help="Compile options for PostgreSQL server")
    parser.add_argument('--db-name', type=str, required=False, default="postgres", help="Name of database")
 
    parser.add_argument('--memory-limit', type=int, required=True, help="Memory limit for cgroup")
    parser.add_argument('--user-password', type=str, required=True, help="Password for sudo")

    args = parser.parse_args()

    global CLICKHOUSE_BASE, CLICKHOUSE_SCRIPT

    CLICKHOUSE_BASE = f"{args.repository_path}/evaluation/databases/ClickHouse"
    CLICKHOUSE_SCRIPT = Path(CLICKHOUSE_BASE, "scripts")

    mount_cgroup(args)
    adjust_vm_swappiness(args)

    # ClickHouse
    evaluate(args)

    umount_cgroup(args)

if __name__ == "__main__":
    main()

