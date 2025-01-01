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

# DB
POSTGRES=0
LASER=1
LOCATOR=2
CLICKHOUSE=3

# Running DB
RUN_DB = None

global EVAL_RUN_FLAG, NUM_READY_THREADS, THREAD_LOCK, CNT_ARRAY
global THRESHOLD_PER_SECOND

THRESHOLD_PER_SECOND = 1000
THREAD_LOCK = threading.Lock()
CNT_ARRAY = [0] * 256
LATENCY_AVG = 0

# Cgroup memory setting directory
CGROUP_DIR = "/sys/fs/cgroup/memory"

# Target workload
INSERT_WORKLOAD = 0
UPDATE_WORKLOAD = 1

# Measuring query
POINT_LOOKUP = 0
RANGE_SCAN = 1
JOIN = 2

def drop_os_cache(args):
    # Drop OS cache
    command = f"sh -c 'echo 3 > /proc/sys/vm/drop_caches'"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

def mount_cgroup(args):
    if not os.path.exists(CGROUP_DIR):
        # Make cgroup directory
        command = f"mkdir {CGROUP_DIR}"
        subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

    print("Mount cgroup")
    command = "mount -t cgroup -o memory postgres /sys/fs/cgroup/memory"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

def umount_cgroup(args):
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
    global CGROUP_DIR, RUN_DB, POSTGRES, LASER, LOCATOR, CLICKHOUSE

    background_pids = []
    limit_bytes = 0

    if not os.path.exists(f"{CGROUP_DIR}/postgres"):
        # Make cgroup directory
        command = f"mkdir {CGROUP_DIR}/postgres"
        subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

    # Set cgroup's swappiness
    command = f"sh -c 'echo 0 > {CGROUP_DIR}/memory.swap.max'"
    subprocess.call('echo {} | sudo -S {}'.format(args.user_password, command), shell=True)

    if RUN_DB == CLICKHOUSE:
        # Get pids of background processes
        result = subprocess.run( \
                args=["ps -ef | grep clickhouse | grep -v python | awk '{print $2;}'"], \
                capture_output=True, text=True, check=True, shell=True)
    else:
        # Get pids of background processes
        result = subprocess.run( \
                args=["ps -ef | grep postgres | grep -v python | awk '{print $2;}'"], \
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
    global RUN_DB, POSTGRES, LASER, LOCATOR, CLICKHOUSE

    if RUN_DB == CLICKHOUSE:
        result = subprocess.run( \
                    args=["ps -ef | grep clickhouse | grep -v python | awk '{print $2;}'"], \
                    capture_output=True, text=True, check=True, shell=True)
    else:
        # Get pids of background processes
        result = subprocess.run( \
                    args=["ps -ef | grep postgres | grep -v python | awk '{print $2;}'"], \
                    capture_output=True, text=True, check=True, shell=True)
    background_pids = result.stdout.splitlines()

    for pid in background_pids:
        # Kill processes
        subprocess.run(args=[f"kill -9 {pid}"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, shell=True, check=False)

    time.sleep(3)


def execute_target_queries(args, connection_string, num_workers,
                           target_workload, measuring_query):
    global EVAL_RUN_FLAG, NUM_READY_THREADS, THREAD_LOCK, LATENCY_AVG, CNT_ARRAY
    global RUN_DB, POSTGRES, LASER, LOCATOR, CLICKHOUSE
    
    if RUN_DB == CLICKHOUSE:
        cursor = clickhouse_connect.get_client(host='localhost', database=args.db_name)

        query_settings = {
            "max_threads": 1,
            "max_memory_usage": 32 * (1024 ** 3),
            "allow_experimental_join_condition": 1,
            "use_uncompressed_cache": 1,
        }
    else:
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

    THREAD_LOCK.acquire()
    NUM_READY_THREADS += 1
    THREAD_LOCK.release()

    while not EVAL_RUN_FLAG:
        pass

    # Measure after two minute
    time.sleep(60 * 2)

    cnt = 0
    latency_sum = 0
    while EVAL_RUN_FLAG:
        # Make target query for measuring
        if measuring_query == POINT_LOOKUP:
            if target_workload == INSERT_WORKLOAD:
                if num_workers == 0:
                    primary_key = args.fact_num_records + 1
                else:
                    primary_key = args.fact_num_records + 1 + \
                                    int(min(CNT_ARRAY[0:num_workers]) * num_workers * (30 / 100.0))

            elif target_workload == UPDATE_WORKLOAD:
                primary_key = random.randint(1, args.fact_num_records)

            # Select a point lookup query
            query = f"SELECT a19, a20, a21, a22, a23, a24, a25, a26, a27, a28 FROM fact WHERE primary_key = {primary_key};"
        elif measuring_query == RANGE_SCAN:
            if target_workload == INSERT_WORKLOAD:
                if num_workers == 0:
                    primary_key = args.fact_num_records + 1 - 100
                else:
                    primary_key = args.fact_num_records + 1 - 100 + \
                                    int(min(CNT_ARRAY[0:num_workers]) * num_workers * (90 / 100.0))

            elif target_workload == UPDATE_WORKLOAD:
                primary_key = random.randint(1, args.fact_num_records) - 100

            # Select a range scan query
            query = f"SELECT a24, a25, a26, a27, a28 FROM fact WHERE primary_key BETWEEN {primary_key} AND {primary_key + 100};"

        elif measuring_query == JOIN:
            if target_workload == INSERT_WORKLOAD:
                dimension_data = random.randint(1, args.dimension_num_records + 1)

            elif target_workload == UPDATE_WORKLOAD:
                dimension_data = random.randint(1, args.dimension_num_records + 1)

            # Select a join query
            query = f"SELECT a1, a2, a3 FROM fact INNER JOIN dimension ON fact.foreign_key = dimension.primary_key WHERE dimension.data LIKE 'data {dimension_data}%';"

        # Run query and measure the latency
        start_time = time.time()
        if RUN_DB == CLICKHOUSE:
            result = cursor.query(query, settings=query_settings)
        else:
            cursor.execute(query)
            result = cursor.fetchall()
        end_time = time.time()

        if RUN_DB != CLICKHOUSE:
            conn.commit()

        # Calculate latency
        latency = end_time - start_time
        
        if cnt >= 1:
            latency_sum += latency
        cnt += 1
        time.sleep(0.3)

    if RUN_DB == CLICKHOUSE:
        cursor.close()
    else:
        cursor.close()
        conn.close()

    LATENCY_AVG = latency_sum / (cnt - 1)

def execute_background_queries(args, connection_string, num_workers, worker_id,
                               target_workload, measuring_query):
    global EVAL_RUN_FLAG, NUM_READY_THREADS, THREAD_LOCK, CNT_ARRAY, THRESHOLD_PER_SECOND
    global RUN_DB, POSTGRES, LASER, LOCATOR, CLICKHOUSE
    
    if RUN_DB == CLICKHOUSE:
        cursor = clickhouse_connect.get_client(host='localhost', port=8123,
                                               database=args.db_name)
    else:
        # Connect to the database
        conn = psycopg2.connect(connection_string)
        cursor = conn.cursor()

    if target_workload == INSERT_WORKLOAD:
        columns = ["primary_key", "foreign_key"] + [f"a{j}" for j in range(1, args.fact_num_attributes + 1)]
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        query = f"INSERT INTO fact ({columns_str}) VALUES ({placeholders})"

        rand_values = []
        for _ in range(args.fact_num_attributes):
            rand_values.append(random.randint(1, 100))

    elif target_workload == UPDATE_WORKLOAD:
        query = f"UPDATE fact SET a%s = %s WHERE primary_key = %s;"

    CNT_ARRAY[worker_id] = 0

    THREAD_LOCK.acquire()
    NUM_READY_THREADS += 1
    THREAD_LOCK.release()

    # Wait until all threads are ready
    while not EVAL_RUN_FLAG:
        pass

    cnt = 0
    while EVAL_RUN_FLAG:
        if target_workload == INSERT_WORKLOAD:
            # Set random values for the primary key and foreign key
            primary_key = args.fact_num_records + max(num_workers, 1) * cnt + worker_id + 1
            foreign_key = random.randint(1, args.dimension_num_records)
            
            random.shuffle(rand_values)
        
            values = [primary_key, foreign_key] + rand_values

        elif target_workload == UPDATE_WORKLOAD:
            target_attribute = random.randint(24, 28)  # The update target is a24 ~ a28
            #primary_key = (cnt % args.fact_num_records) + 1
            primary_key = random.randint(1, args.fact_num_records)

            values = [target_attribute, random.randint(1, 100), primary_key]

        try:
            if RUN_DB == CLICKHOUSE:
                cursor.command("START TRANSACTION;")
                cursor.command(query, values)
                cursor.command("COMMIT;")
            else:

                # Run query
                cursor.execute("BEGIN;")
                cursor.execute(query, values)
                cursor.execute("END;")

                # Commit the transaction
                conn.commit()
        except Exception as e:
            #print(e, f"Primary key: {primary_key}, Values: {values}")
            pass


        # Per 100 queries, we regulate the number of queries executed per second
        if cnt % 100 == 0:
            start_time = time.time()

            # We store the number of queries executed by this thread
            CNT_ARRAY[worker_id] = cnt
        elif cnt % 100 == 99:
            end_time = time.time()

            # Regulate the number of queries executed per second
            elapsed_time = end_time - start_time
            if target_workload == INSERT_WORKLOAD and elapsed_time < 1:
                time.sleep(1 - elapsed_time)
            elif target_workload == UPDATE_WORKLOAD and elapsed_time < 0.1:
                time.sleep(0.1 - elapsed_time)

        cnt += 1

    # Print the number of queries executed by this thread
    print(f"Worker {worker_id} executed {cnt} queries")

    if RUN_DB == CLICKHOUSE:
        cursor.close()
    else:
        cursor.close()
        conn.close()

def measure_latency(args, connection_string, num_workers, target_workload,
                    measuring_query):
    global EVAL_RUN_FLAG, NUM_READY_THREADS, THREAD_LOCK, LATENCY_AVG

    if target_workload == INSERT_WORKLOAD:
        target_workload_name = "INSERT"
    elif target_workload == UPDATE_WORKLOAD:
        target_workload_name = "UPDATE"

    if measuring_query == POINT_LOOKUP:
        target_workload_name += ", POINT_LOOKUP"
    elif measuring_query == RANGE_SCAN:
        target_workload_name += ", RANGE_SCAN"
    elif measuring_query == JOIN:
        target_workload_name += ", JOIN"

    print(f'[STATUS] Measuring latency with {num_workers} workers... ({target_workload_name})')

    EVAL_RUN_FLAG = False
    NUM_READY_THREADS = 0
    LATENCY_AVG = 0

    # Fork a thread to execute point lookup queries
    target_thread = \
        [threading.Thread(target=execute_target_queries, args=(args,
                                                               connection_string,
                                                               num_workers,
                                                               target_workload,
                                                               measuring_query
                                                               ))]

    # Fork num_workers threads to execute background queries
    background_threads = \
        [threading.Thread(target=execute_background_queries,
         args=(args, connection_string, num_workers, worker_id,
               target_workload, measuring_query)) for worker_id in range(num_workers)]

    # Start all threads
    for t in target_thread:
        t.start()
    for t in background_threads:
        t.start()            
    
    # Wait for all threads to be ready
    while NUM_READY_THREADS < num_workers:
        pass

    # Start the evaluation
    EVAL_RUN_FLAG = True

    # Five minutes for evaluation
    time.sleep(60 * 5)

    # Stop the evaluation
    EVAL_RUN_FLAG = False

    # Wait for the evaluation to finish
    for t in target_thread:
        t.join()
    for t in background_threads:
        t.join()

    return LATENCY_AVG

def warmup(args, connection_string):
    conn = psycopg2.connect(connection_string)
    cursor = conn.cursor()

    print("Warmup Start.")

    cnt = 0
    for i in range(1, args.fact_num_records + 1):
        # Select a random query
        query = f"SELECT * FROM fact WHERE primary_key = {i};"

        # Run query
        cursor.execute(query)
        result = cursor.fetchone()

        # Print progress
        cnt += 1
        if cnt % 1000 == 0:
            print(f"Progress: {cnt}/{args.fact_num_records}", end='\r')

    cursor.close()
    conn.close()

def evaluate(args, target_db, target_workload, measuring_query):
    global RUN_DB, POSTGRES, LASER, LOCATOR, CLICKHOUSE
    global CLICKHOUSE_BASE

    RUN_DB = target_db

    print("Target database has started evaluating the dataset")

    connection_string = f"dbname={args.db_name} host='localhost' port={args.port}"

    worker_counts = [0, 4, 16, 64]
    results = {}

    if target_db == POSTGRES:
        db_path = f'{DB_DATA}/data_microbenchmark_vanilla'
        RUN_DB = POSTGRES
    elif target_db == LASER:
        db_path = f'{DB_DATA}/data_microbenchmark_laser'
        RUN_DB = LASER
    elif target_db == LOCATOR:
        db_path = f'{DB_DATA}/data_microbenchmark_locator'
        RUN_DB = LOCATOR
    elif target_db == CLICKHOUSE:
        db_path = f'{DB_DATA}/data_microbenchmark_clickhouse'
        RUN_DB = CLICKHOUSE
    
    for num_workers in worker_counts: 
        if target_db == CLICKHOUSE:
            # Remove existing tables
            os.system(f"rm -rf {CLICKHOUSE_BASE}/data")

            # Copy dataset
            os.system(f"cp -r {CLICKHOUSE_BASE}/microbenchmark_data {CLICKHOUSE_BASE}/data")
        else:
            # Remove existing data directory
            os.system(f'rm -rf {DB_DATA_CURRENT}')

            # Copy dataset
            os.system(f'cp -r {db_path} {DB_DATA_CURRENT}')

        run_server(params=f' -d={DB_DATA}')


        # Drop OS cache
        drop_os_cache(args)

        # Activate cgroup
        activate_cgroup(args)

        # Use select queries to warm up the database buffer
        #warmup(args, connection_string)

        # Measure latency
        average_latency = measure_latency(args, 
                                          connection_string,
                                          num_workers,
                                          target_workload,
                                          measuring_query)
        results[num_workers] = average_latency
        print(f"Workers: {num_workers}, Average Latency: {average_latency:.6f} seconds")


        if target_db != CLICKHOUSE:
            os.system(f'pkill -9 postgres > /dev/null 2>&1')
        time.sleep(5)

        # Deactivate cgroup
        deactivate_cgroup(args)

        shutdown_server(params=f' -d={DB_DATA}')

    if target_db == POSTGRES:
        file_name = 'postgres'
    elif target_db == LASER:
        file_name = 'laser'
    elif target_db == LOCATOR:
        file_name = 'locator'
    elif target_db == CLICKHOUSE:
        file_name = 'clickhouse'

    eval_folder_name = 'insert' if target_workload == INSERT_WORKLOAD else 'update'
    if measuring_query == POINT_LOOKUP:
        eval_folder_name += '_for_measuring_point_lookup'
    elif measuring_query == RANGE_SCAN:
        eval_folder_name += '_for_measuring_range_scan'
    elif measuring_query == JOIN:
        eval_folder_name += '_for_measuring_join'

    if not os.path.exists(f'./result/{eval_folder_name}'):
        os.makedirs(f'./result/{eval_folder_name}')

    with open(f'./result/{eval_folder_name}/{file_name}.dat', 'w') as f:
        f.write(f"# num_workers \t\t latency(s)\n")
        for num_workers, average_latency in results.items():
            # Write results to a file
            f.write(f"{num_workers} \t\t {average_latency}\n")

def compile_database(params=None):
    print("Compile... flag: %s" % (params))
    subprocess.run(args=DB_INSTALL_SCRIPT + "/install.sh" + params,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        check=True, shell=True)

def init_server(params=None):
    subprocess.run(args=DB_SERVER_SCRIPT + "/init_server.sh" + params,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        check=True, shell=True)

def run_server(params=None):
    global RUN_DB, CLICKHOUSE

    if RUN_DB == CLICKHOUSE:
        subprocess.run(args=str(Path(CLICKHOUSE_SCRIPT, "run_server.sh")),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            check=True, shell=True)
        
        time.sleep(5)
        print("ClickHouse server started")
    else:
        subprocess.run(args=DB_SERVER_SCRIPT + "/run_server.sh" + params,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            check=True, shell=True)
        print("PostgreSQL server started")

def shutdown_server(params=None):
    global RUN_DB, CLICKHOUSE

    if RUN_DB == CLICKHOUSE:
        subprocess.run(args=str(Path(CLICKHOUSE_SCRIPT, "shutdown_server.sh")),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            check=False)
        time.sleep(15)
        print("ClickHouse server stopped")
    else:
        subprocess.run(args=DB_SERVER_SCRIPT + "/shutdown_server.sh" + params,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            check=True, shell=True)
        print("PostgreSQL server stopped")

def change_postgresql_conf_port(file_path, port):
    # Read the contents of the file and then modify and save it
    with open(file_path, 'r') as file:
        lines = file.readlines()

    with open(file_path, 'w') as file:
        for line in lines:
            # Find and modify the line that starts with "port = number"
            if line.startswith("port ="):
                line = re.sub(r'port = \d+', f'port = {port}', line)
            file.write(line)

def create_vanilla_tables(cursor, num_attributes):
    global RUN_DB, LASER, LOCATOR, CLICKHOUSE

    # Create dimension table
    cursor.execute("""
    CREATE TABLE dimension (
        primary_key BIGINT PRIMARY KEY,
        data TEXT
    );
    """)

    # Create fact table with dynamic attributes
    columns = ["primary_key BIGINT PRIMARY KEY", 
               "foreign_key BIGINT REFERENCES dimension(primary_key)"]
    
    for i in range(1, num_attributes + 1):
        columns.append(f"a{i} BIGINT")
    
    columns_str = ", ".join(columns)
    
    cursor.execute(f"""
    CREATE TABLE fact (
        {columns_str}
    );
    """)


def create_clickhouse_tables(cursor, num_attributes):
    global RUN_DB, LASER, LOCATOR, CLICKHOUSE

    # Drop existing tables
    cursor.command("DROP TABLE IF EXISTS dimension;")
    cursor.command(f"""DROP TABLE IF EXISTS fact;""")

    # Create dimension table
    cursor.command("""
    CREATE TABLE dimension (
        primary_key BIGINT,
        data TEXT
    ) ENGINE = MergeTree()
    ORDER BY (primary_key)
    PRIMARY KEY primary_key;
    """)

    # Create fact table with dynamic attributes
    columns = ["primary_key BIGINT", 
               "foreign_key BIGINT"]
    
    for i in range(1, num_attributes + 1):
        columns.append(f"a{i} BIGINT")
    
    columns_str = ", ".join(columns)
    

    cursor.command(f"""
    CREATE TABLE fact (
        {columns_str}
    ) ENGINE = MergeTree()
    ORDER BY (primary_key)
    PRIMARY KEY primary_key;
    """)

def create_laser_tables(cursor, num_attributes):
    # Create dimension table
    cursor.execute("""
    CREATE TABLE dimension (
        primary_key BIGINT PRIMARY KEY,
        data TEXT
    );
    """)

    # Create fact table with dynamic attributes
    columns = ["primary_key BIGINT", 
               "foreign_key BIGINT"]
    
    for i in range(1, num_attributes + 1):
        columns.append(f"a{i} BIGINT")
    
    columns_str = ", ".join(columns)
    
    cursor.execute(f"""
    CREATE FOREIGN TABLE fact (
        {columns_str}
    ) SERVER lsm_server;
    """)

def create_locator_tables(cursor, num_attributes):
    # Create dimension table
    cursor.execute("""
    CREATE TABLE dimension (
        primary_key BIGINT PRIMARY KEY,
        data TEXT
    ) USING HAP;
    """)

    # Create fact table with dynamic attributes
    columns = ["primary_key BIGINT PRIMARY KEY", 
               "foreign_key BIGINT REFERENCES dimension(primary_key)"]
    
    for i in range(1, num_attributes + 1):
        columns.append(f"a{i} BIGINT")
    
    columns_str = ", ".join(columns)
    
    cursor.execute(f"""
    CREATE TABLE fact (
        {columns_str}
    ) USING HAP;
    """)

def insert_dimension_data(cursor, num_records):
    global RUN_DB, CLICKHOUSE

    if RUN_DB == CLICKHOUSE:
        # Insert data into dimension table
        for i in range(1, num_records + 1):
            data = f"data {i}"
            cursor.command(f"INSERT INTO dimension VALUES ({i}, '{data}');")
    else:
        # Insert data into dimension table
        for i in range(1, num_records + 1):
            data = f"data {i}"
            cursor.execute("INSERT INTO dimension (primary_key, data) VALUES (%s, %s);", (i, data))
    print("Dimension table is created and data is inserted")

def insert_fact_data_batch(connection_string, start_id, end_id, num_attributes,
                           dimension_size, db_name="postgres"):
    global RUN_DB, CLICKHOUSE

    if RUN_DB == CLICKHOUSE:

        cursor = None
        try:
            cursor = clickhouse_connect.get_client(host='localhost', 
                                                   port=8123,
                                                   database=db_name,
                                                   connect_timeout=6000,
                                                   send_receive_timeout=6000)

            insert_values = []
            for i in range(start_id, end_id + 1):
                foreign_key = random.randint(1, dimension_size)
                values = [i, foreign_key]
            
                for _ in range(num_attributes):
                    values.append(random.randint(1, 100))  # Random value for each a1, a2, ...

                if i % 100000 == 0:
                    print(f"Inserting data in range {start_id}-{end_id}: {i}")

                values_str = "(" + ", ".join([str(value) for value in values]) + ")"
                insert_values.append(values_str)
                                                                                                       
            columns = ["primary_key", "foreign_key"] + [f"a{j}" for j in range(1, num_attributes + 1)]
            columns_str = ", ".join(columns)
            
            insert_values_str = ", ".join(insert_values)
            cursor.command(f"INSERT INTO fact ({columns_str}) VALUES {insert_values_str}")
        except Exception as error:
            print(f"Error while inserting data in range {start_id}-{end_id}: {error}")
        finally:
            if cursor is not None:
                cursor.close()
    else:
        try:
            connection = psycopg2.connect(connection_string)
            connection.autocommit = False
            cursor = connection.cursor()

            insert_values = []
            for i in range(start_id, end_id + 1):
                foreign_key = random.randint(1, dimension_size)
                values = [i, foreign_key]

                if i % 100000 == 0:
                    print(f"Inserting data in range {start_id}-{end_id}: {i}")
            
                for _ in range(num_attributes):
                    values.append(random.randint(1, 100))  # Random value for each a1, a2, ...

                values_str = "(" + ", ".join([str(value) for value in values]) + ")"
                #insert_values.append(values_str)

                columns = ["primary_key", "foreign_key"] + [f"a{j}" for j in range(1, num_attributes + 1)]
                columns_str = ", ".join(columns)
                #placeholders = ", ".join(["%s"] * len(values))

                cursor.execute("BEGIN;")
                cursor.execute(f"INSERT INTO fact ({columns_str}) VALUES {values_str}")
                cursor.execute("END;")
            
            #insert_values_str = ", ".join(insert_values)
            #cursor.execute(f"INSERT INTO fact ({columns_str}) VALUES {insert_values_str}")

            connection.commit()

        except (Exception, psycopg2.Error) as error:
            print(f"Error while inserting data in range {start_id}-{end_id}: {error}")
    
        finally:
            if connection:
                cursor.close()
                connection.close()

def populate_fact_table_dataset(args, connection_string):
    global RUN_DB

    # Calculate the batch size for each thread
    batch_size = args.fact_num_records // args.num_threads
    ranges = [(i * batch_size + 1, (i + 1) * batch_size) for i in range(args.num_threads)]

    # Handle any remainder records
    if args.fact_num_records % args.num_threads != 0:
       ranges[-1] = (ranges[-1][0], args.fact_num_records)

    # Use ThreadPoolExecutor to insert fact data in parallel
    with ThreadPoolExecutor(max_workers=args.num_threads) as executor:
        futures = [
            executor.submit(insert_fact_data_batch, connection_string, start_id,
                            end_id, args.fact_num_attributes,
                            args.dimension_num_records,
                            args.db_name)
            for start_id, end_id in ranges
        ]
        for future in futures:
            future.result()  # Ensure all threads have completed

    if RUN_DB == CLICKHOUSE:
        cursor = clickhouse_connect.get_client(host='localhost', 
                                           port=8123,
                                           database=args.db_name)     
        cursor.command("OPTIMIZE TABLE fact FINAL;")

    print("Tables created and data is inserted successfully.")


def build_vanilla_dataset(args):
    print("Vanilla PostgreSQL has started building the dataset")

    connection_string = f"dbname={args.db_name} host='localhost' port={args.port}"

    compile_database(params=args.compile_options)

    # Remove exsiting data directory
    os.system("rm -rf %s" % (DB_DATA_CURRENT))

    init_server(params=" -d=%s" % (DB_DATA))

    # Set config file
    os.system("cp %s/postgresql_%s.conf %s/postgresql.conf"
        % (DB_CONFIG, "microbenchmark_vanilla", DB_DATA_CURRENT))

    change_postgresql_conf_port("%s/postgresql.conf" % (DB_DATA_CURRENT),
        args.port)

    run_server(params=" -d=%s " % (DB_DATA))

    if args.db_name != 'postgres':
        subprocess.call(["%s/pgsql/bin/psql" % (DB_BASE), "-h", "localhost",
        "-p", args.port, "-d", "postgres",
        "-c", f"CREATE DATABASE {args.db_name};"])

    try:
        # Connect to the PostgreSQL database to create tables
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()

        # Create tables with dynamic columns
        create_vanilla_tables(cursor, args.fact_num_attributes)
        
        # Insert data into dimension table
        insert_dimension_data(cursor, args.dimension_num_records)

        connection.commit()
        cursor.close()
        connection.close()

        # Because this function operates in a multithreaded environment, the
        # existing connection is closed and a new connection is assigned to each
        # thread
        populate_fact_table_dataset(args, connection_string)

    except (Exception, psycopg2.Error) as error:
        print("Error while working with Vanilla PostgreSQL", error)
        shutdown_server(params=" -d=%s" % (DB_DATA))
    
    finally:
        if connection:
            cursor.close()
            connection.close()

    shutdown_server(params=" -d=%s" % (DB_DATA))

    os.system("rm -rf %s/data_%s"
        % (DB_DATA, "microbenchmark_vanilla"))
    os.system("mv %s %s/data_%s"
        % (DB_DATA_CURRENT, DB_DATA, "microbenchmark_vanilla"))

def build_laser_dataset(args):
    print("Laser has started building the dataset")

    connection_string = f"dbname={args.db_name} host='localhost' port={args.port}"

    compile_database(params=(args.compile_options + " -DLASER -DLSM_TXN -DEVAL_LASER "))

    # Remove exsiting data directory
    os.system("rm -rf %s" % (DB_DATA_CURRENT))

    # Compile and install rocksDB
    os.system('cd %s/laser-lsm-htap/laser-lsm' % (DB_BASE))
    os.system('sudo make DEBUG_LEVEL=0 shared_lib install-shared DISABLE_WARNING_AS_ERROR=1 -j24 > /dev/null 2>&1')
    os.system('sudo sh -c "echo /usr/local/lib >> /etc/ld.so.conf" > /dev/null')
    os.system('sudo ldconfig && cd -')

    init_server(params=" -d=%s" % (DB_DATA))

    # Set config file
    os.system("cp %s/postgresql_%s.conf %s/postgresql.conf"
        % (DB_CONFIG, "microbenchmark_laser", DB_DATA_CURRENT))

    change_postgresql_conf_port("%s/postgresql.conf" % (DB_DATA_CURRENT),
        args.port)

    run_server(params=" -d=%s " % (DB_DATA))

    if args.db_name != 'postgres':
        subprocess.call(["%s/pgsql/bin/psql" % (DB_BASE), "-h", "localhost",
        "-p", args.port, "-d", "postgres",
        "-c", f"CREATE DATABASE {args.db_name};"])

    try:
        # Connect to the PostgreSQL database to create tables
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()

        # Copy cg_matrix file to the data/lsm directory
        os.system(f"cp ./cg_matrix {DB_DATA_CURRENT}/lsm")

        # Create LSM extension
        cursor.execute("""
        CREATE EXTENSION lsm;
        """)

        # Create LSM server to use FDW
        cursor.execute("""
        CREATE SERVER lsm_server FOREIGN DATA WRAPPER lsm_fdw;
        """)

        # Create tables with dynamic columns
        create_laser_tables(cursor, args.fact_num_attributes)
        
        # Insert data into dimension table
        insert_dimension_data(cursor, args.dimension_num_records)

        connection.commit()
        cursor.close()
        connection.close()

        # Because this function operates in a multithreaded environment, the
        # existing connection is closed and a new connection is assigned to each
        # thread
        populate_fact_table_dataset(args, connection_string)

    except (Exception, psycopg2.Error) as error:
        print("Error while working with Laser PostgreSQL", error)
        shutdown_server(params=" -d=%s" % (DB_DATA))
    
    finally:
        if connection:
            cursor.close()
            connection.close()

    shutdown_server(params=" -d=%s" % (DB_DATA))

    os.system("rm -rf %s/data_%s"
        % (DB_DATA, "microbenchmark_laser"))
    os.system("mv %s %s/data_%s"
        % (DB_DATA_CURRENT, DB_DATA, "microbenchmark_laser"))

def build_clickhouse_dataset(args):
    global RUN_DB, CLICKHOUSE, CLICKHOUSE_BASE, CLICKHOUSE_SCRIPT

    print("ClickHouse has started building the dataset")
    RUN_DB = CLICKHOUSE

    # If the folder "/clickhouse/build/programs" does not exist
    if not os.path.exists(CLICKHOUSE_BASE + "/clickhouse/build/programs/clickhouse"):
        # Submodule initialization
        os.system("git submodule init")
        os.system("git submodule update")

        # Compile ClickHouse
        os.system(f"{CLICKHOUSE_SCRIPT}/build.sh")

    # Remove existing data directory
    os.system(f"rm -rf {CLICKHOUSE_BASE}/microbenchmark_data")

    cursor = None
    try:
        # Run ClickHouse server
        run_server(params="")

        # Create tables
        cursor = clickhouse_connect.get_client(host='localhost', port=8123)
        cursor.command(f"CREATE DATABASE IF NOT EXISTS {args.db_name};")
        cursor.close()

        # Create a client instance
        cursor = clickhouse_connect.get_client(host='localhost', port=8123, database=args.db_name)

        # Create tables with dynamic columns
        create_clickhouse_tables(cursor, args.fact_num_attributes)
        
        # Insert data into dimension table
        insert_dimension_data(cursor, args.dimension_num_records)

        # Because this function operates in a multithreaded environment, the
        # existing connection is closed and a new connection is assigned to each
        # thread
        connection_string = ""
        populate_fact_table_dataset(args, connection_string)
    except Exception as error:
        print("Error while working with ClickHouse", error)
    finally:
        if cursor is not None:
            cursor.close()
        shutdown_server(params="")

    os.system(f"mv {CLICKHOUSE_BASE}/data {CLICKHOUSE_BASE}/microbenchmark_data")

def build_locator_dataset(args):
    print("Locator has started building the dataset")

    connection_string = f"dbname={args.db_name} host='localhost' port={args.port}"

    compile_database(params=(args.compile_options + " -DDIVA -DPLEAF_NUM_PAGE=262144 -DEBI_NUM_PAGE=131072 -DLOCATOR "))

    # Remove exsiting data directory
    os.system("rm -rf %s" % (DB_DATA_CURRENT))

    # Compile and install pg_hint_plan extension
    subprocess.run(['make', 'PG_CONFIG=' + DB_BASE + '/pgsql/bin/pg_config'],
            cwd=(DB_BASE + "/pg_hint_plan"),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    subprocess.run(['make', 'PG_CONFIG=' + DB_BASE + '/pgsql/bin/pg_config', 'install'],
            cwd=(DB_BASE + "/pg_hint_plan"),
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

    init_server(params=" -d=%s" % (DB_DATA))

    # Set config file
    os.system("cp %s/postgresql_%s.conf %s/postgresql.conf"
        % (DB_CONFIG, "microbenchmark_locator", DB_DATA_CURRENT))

    change_postgresql_conf_port("%s/postgresql.conf" % (DB_DATA_CURRENT),
        args.port)

    run_server(params=" -d=%s " % (DB_DATA))

    if args.db_name != 'postgres':
        subprocess.call(["%s/pgsql/bin/psql" % (DB_BASE), "-h", "localhost",
        "-p", args.port, "-d", "postgres",
        "-c", f"CREATE DATABASE {args.db_name};"])

    connection = None
    try:
        # Connect to the PostgreSQL database to create tables
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()

        cursor.execute("""
        CREATE EXTENSION intarray;
        """)

        cursor.execute("""
        CREATE ACCESS METHOD hap TYPE TABLE HANDLER locator_hap_handler;
        """)

        cursor.execute("""
        CREATE ACCESS METHOD locator TYPE TABLE HANDLER locatoram_handler;
        """)

        # Create tables with dynamic columns
        create_locator_tables(cursor, args.fact_num_attributes)

        # Insert data into dimension table
        insert_dimension_data(cursor, args.dimension_num_records)

        connection.commit()

        # Make hidden attribute
        print("Hidden attribute encoding...")
        cursor.execute("""
        SELECT locator_hap_encode('public.dimension.data');
        """)

        connection.commit()
        
        # Set the propagation path
        spread_factor = round(args.dimension_num_records ** (1 / args.locator_max_level))
        print(f"Dimension size: {args.dimension_num_records}, Max level: {args.locator_max_level}, Spread factor: {spread_factor}")
        cursor.execute("""
        ALTER TABLE fact SET (hidden_partition_key ='{
        public.dimension.data (dimension->fact)}', locator_spread_factor=%d);
        """ % (spread_factor))

        connection.commit()
        
        # Create columnar layout for fact table
        # 1. a1 ~ a4 are used for join.
        # 2. a19 ~ a28 are used for point lookup.
        # 3. a24 ~ a28 are used for update and range scan.
        # We also consider hidden attributes in the layout.
        tmp_query = f"SELECT locator_set_columnar_layout(to_regclass('fact')::oid, '1|2|5|5|25|26|27|28|29|1|0|1|1|10|2|3|4|5|20|21|22|23|24|30|14|6|7|8|9|10|11|12|13|14|15|16|17|18|19');"
        cursor.execute(tmp_query)
        
        connection.commit()

        cursor.close()
        connection.close()

        # Because this function operates in a multithreaded environment, the
        # existing connection is closed and a new connection is assigned to each
        # thread
        populate_fact_table_dataset(args, connection_string)

        # Wait..
        print("Sleep 5 minutes...")
        time.sleep(60 * 5)

    except (Exception, psycopg2.Error) as error:
        print("Error while working with Locator PostgreSQL", error)
        shutdown_server(params=" -d=%s" % (DB_DATA))
    
    finally:
        if connection:
            cursor.close()
            connection.close()

    shutdown_server(params=" -d=%s" % (DB_DATA))

    # Restart the server to transform remained partitions
    print("Sleep 5 seconds...")
    time.sleep(5)
    run_server(params=" -d=%s " % (DB_DATA))

    print("Sleep 10 seconds...")
    time.sleep(10)
    shutdown_server(params=" -d=%s" % (DB_DATA))

    os.system("rm -rf %s/data_%s"
        % (DB_DATA, "microbenchmark_locator"))
    os.system("mv %s %s/data_%s"
        % (DB_DATA_CURRENT, DB_DATA, "microbenchmark_locator"))

def main():
    parser = argparse.ArgumentParser(description="Create dimension and fact tables in PostgreSQL and populate them with data.")
    
    parser.add_argument('--dimension-num-records', type=int, required=True, help="Number of records in the dimension table")
    parser.add_argument('--fact-num-records', type=int, required=True, help="Number of records in the fact table")

    # XXX: Number of attributes in the fact table should be at least 8
    parser.add_argument('--fact-num-attributes', type=int, required=True, help="Number of additional attributes in the fact table")
    parser.add_argument('--port', type=str, required=True, help="Port number of the PostgreSQL server")
    parser.add_argument('--num-threads', type=int, required=True, help="Number of threads to use for inserting fact data")
    parser.add_argument('--repository-path', type=str, required=True, help="The repository path for this project")
    parser.add_argument('--compile-options', type=str, required=False, default=" ", help="Compile options for PostgreSQL server")
    parser.add_argument('--db-name', type=str, required=False, default="postgres", help="Name of database")
    parser.add_argument('--locator-max-level', type=int, required=True, help="Max level of locator")
 
    parser.add_argument('--memory-limit', type=int, required=True, help="Memory limit for cgroup")
    parser.add_argument('--user-password', type=str, required=True, help="Password for sudo")

    args = parser.parse_args()

    global DB_BASE, DB_DATA, DB_DATA_CURRENT, DB_CONFIG, DB_SCRIPT, DB_SERVER_SCRIPT, DB_CLIENT_SCRIPT, DB_INSTALL_SCRIPT
    global POSTGRES, LASER, LOCATOR, CLICKHOUSE
    global CLICKHOUSE_BASE, CLICKHOUSE_SCRIPT, CLICKHOUSE_DATA

    DB_BASE = "%s/PostgreSQL" % (args.repository_path)
    DB_DATA = DB_BASE + "/data"
    DB_DATA_CURRENT = DB_DATA + "/data_current"
    DB_CONFIG = DB_BASE + "/config"
    DB_SCRIPT = DB_BASE + "/script"
    DB_SERVER_SCRIPT = DB_SCRIPT + "/script_server"
    DB_CLIENT_SCRIPT = DB_SCRIPT + "/script_client"
    DB_INSTALL_SCRIPT = DB_SCRIPT + "/script_install"

    CLICKHOUSE_BASE = "%s/evaluation/databases/ClickHouse" % (args.repository_path)
    CLICKHOUSE_DATA = Path(CLICKHOUSE_BASE, "data")
    CLICKHOUSE_SCRIPT = Path(CLICKHOUSE_BASE, "scripts")

    mount_cgroup(args)
    adjust_vm_swappiness(args)

    # Locator
    build_locator_dataset(args)
    evaluate(args, LOCATOR, target_workload=INSERT_WORKLOAD, measuring_query=POINT_LOOKUP)
    evaluate(args, LOCATOR, target_workload=UPDATE_WORKLOAD, measuring_query=POINT_LOOKUP)
                                                                                          
    evaluate(args, LOCATOR, target_workload=INSERT_WORKLOAD, measuring_query=RANGE_SCAN)
    evaluate(args, LOCATOR, target_workload=UPDATE_WORKLOAD, measuring_query=RANGE_SCAN)
                                                                                          
    evaluate(args, LOCATOR, target_workload=INSERT_WORKLOAD, measuring_query=JOIN)
    evaluate(args, LOCATOR, target_workload=UPDATE_WORKLOAD, measuring_query=JOIN)


    # Vanilla PostgreSQL
    build_vanilla_dataset(args)
    evaluate(args, POSTGRES, target_workload=INSERT_WORKLOAD, measuring_query=POINT_LOOKUP)
    evaluate(args, POSTGRES, target_workload=UPDATE_WORKLOAD, measuring_query=POINT_LOOKUP)

    evaluate(args, POSTGRES, target_workload=INSERT_WORKLOAD, measuring_query=RANGE_SCAN)
    evaluate(args, POSTGRES, target_workload=UPDATE_WORKLOAD, measuring_query=RANGE_SCAN)

    evaluate(args, POSTGRES, target_workload=INSERT_WORKLOAD, measuring_query=JOIN)
    evaluate(args, POSTGRES, target_workload=UPDATE_WORKLOAD, measuring_query=JOIN)

    # Laser
    build_laser_dataset(args)
    evaluate(args, LASER, target_workload=INSERT_WORKLOAD, measuring_query=POINT_LOOKUP)
    evaluate(args, LASER, target_workload=UPDATE_WORKLOAD, measuring_query=POINT_LOOKUP)

    evaluate(args, LASER, target_workload=INSERT_WORKLOAD, measuring_query=RANGE_SCAN)
    evaluate(args, LASER, target_workload=UPDATE_WORKLOAD, measuring_query=RANGE_SCAN)

    evaluate(args, LASER, target_workload=INSERT_WORKLOAD, measuring_query=JOIN)
    evaluate(args, LASER, target_workload=UPDATE_WORKLOAD, measuring_query=JOIN)

    # ClickHouse
    build_clickhouse_dataset(args)

    evaluate(args, CLICKHOUSE, target_workload=INSERT_WORKLOAD, measuring_query=POINT_LOOKUP)
    evaluate(args, CLICKHOUSE, target_workload=UPDATE_WORKLOAD, measuring_query=POINT_LOOKUP)

    evaluate(args, CLICKHOUSE, target_workload=INSERT_WORKLOAD, measuring_query=RANGE_SCAN)
    evaluate(args, CLICKHOUSE, target_workload=UPDATE_WORKLOAD, measuring_query=RANGE_SCAN)

    evaluate(args, CLICKHOUSE, target_workload=INSERT_WORKLOAD, measuring_query=JOIN)
    evaluate(args, CLICKHOUSE, target_workload=UPDATE_WORKLOAD, measuring_query=JOIN)

    umount_cgroup(args)

if __name__ == "__main__":
    main()

