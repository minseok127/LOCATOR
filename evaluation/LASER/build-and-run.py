import sys, subprocess, signal
import threading
import psycopg2
import os, random
import argparse
import time
import re
import shutil
from concurrent.futures import ThreadPoolExecutor

# Dictionary to track the current_id for each insert thread
current_ids = {}

# Global flag to control the running state of threads
stop_threads = threading.Event()

# Global dictionaries to track latency and transaction counts
latency_data = {
    "insert": {},
    "update": {},
    "olap_point_lookup": {},
    "olap_range_scan": {},
    "olap_join": {}
}

transaction_counts = {
    "insert": {},
    "update": {},
    "olap_point_lookup": {},
    "olap_range_scan": {},
    "olap_join": {}
}

performance_results = []

def compile_database(params=None):
    print("Compile... flag: %s" % (params))
    subprocess.run(args=DB_INSTALL_SCRIPT + "/install.sh" + params,
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True, shell=True)

def run_server(params=None):
    subprocess.run(args=DB_SERVER_SCRIPT + "/run_server.sh" + params,
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True, shell=True)

def shutdown_server(params=None):
    print("Shutdown...")
    subprocess.run(args=DB_SERVER_SCRIPT + "/shutdown_server.sh" + params,
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True, shell=True)

# Function to generate the data directory name (without worker details)
def generate_data_dir_name(args):
    if args.mode == 'vanilla' or args.mode == 'locator':
        # For vanilla and locator modes
        data_dir_name = f"{args.dimension_num_records}d_{args.fact_num_records}f_{args.fact_num_attributes}a"
    elif args.mode == 'laser':
        # For laser mode, include OLAP query type (point, range, join)
        data_dir_name = f"{args.dimension_num_records}d_{args.fact_num_records}f_{args.fact_num_attributes}a_{args.point_lookup_num_projection}p_{args.range_scan_num_projection}r_{args.join_num_projection}j"
    else:
        raise ValueError("Unknown mode: must be vanilla, laser, or locator.")
    
    return data_dir_name

# Function to generate the experiment name
def generate_experiment_name(args):
    experiment_name = f"experiment_{args.num_insert_workers}ins_{args.num_update_workers}upd_{args.num_olap_workers}olap_{args.duration_sec}sec_{args.point_lookup_num_projection}p_{args.range_scan_num_projection}r_{args.join_num_projection}j"
    
    return experiment_name

def check_and_prepare_data_dir(mode, data_dir_name):
    data_dir_path = os.path.join(os.getcwd(), f"data_{mode}_{data_dir_name}")

    if os.path.exists(data_dir_path):
        # If the directory exists, remove the current data directory and copy the existing one
        if os.path.exists(DB_DATA_CURRENT):
            shutil.rmtree(DB_DATA_CURRENT)
        shutil.copytree(data_dir_path, DB_DATA_CURRENT)
        print(f"Copied {data_dir_path} to {DB_DATA_CURRENT}")
        return None  # Explicitly return None when the directory exists
    else:
        # If the directory doesn't exist, proceed with the normal setup and then copy the current data directory
        print(f"{data_dir_path} does not exist. Proceeding with normal setup.")
        return data_dir_path

def prepare_vanilla_dataset(args, data_dir_name):
    data_dir_path = check_and_prepare_data_dir("vanilla", data_dir_name)

    if data_dir_path:
        command = [
            'python3', 'build.py',
            '--dimension-num-records', str(args.dimension_num_records),
            '--fact-num-records', str(args.fact_num_records),
            '--fact-num-attributes', str(args.fact_num_attributes),
            '--port', args.port,
            '--num-threads', str(args.num_insert_workers),
            '--repository-path', args.repository_path,
            '--compile-options', str(args.compile_options),
            '--db-name', args.db_name,
            '--mode', 'vanilla']
        
        try:
            subprocess.run(command)
        except subprocess.CalledProcessError as e:
            print(f"Error during vanilla dataset preparation: {e}")
            sys.exit(1)

        # After running the build, copy the DB_DATA_CURRENT to the named directory
        shutil.copytree(DB_DATA_CURRENT, data_dir_path)
        print(f"Copied {DB_DATA_CURRENT} to {data_dir_path}")

def prepare_laser_dataset(args, data_dir_name):
    data_dir_path = check_and_prepare_data_dir("laser", data_dir_name)

    if data_dir_path:
        num_inserts = 10000
        num_updates_per_level_str = make_num_update_arg(args, num_inserts)
        num_point_lookups_per_level_str = make_num_point_lookup_arg(args, num_inserts)
        num_range_scans_per_level_str = make_num_range_scan_arg(args, num_inserts)

        command = [
            'python3', 'find_optimal_cg.py',
            '--num-levels', str(args.laser_num_levels),
            '--level-size-ratio', str(2),
            '--num-attributes', str(args.fact_num_attributes),
            '--point-lookup-num-projection', str(args.point_lookup_num_projection),
            '--range-scan-num-selected-keys', str(args.range_scan_num_selected_keys),
            '--range-scan-num-projection', str(args.range_scan_num_projection),
            '--update-num-projection', str(args.update_num_projection),
            '--join-num-projection', str(args.join_num_projection),
            '--num-point-lookups-per-level', num_point_lookups_per_level_str,
            '--num-range-scans-per-level', num_range_scans_per_level_str,
            '--num-updates-per-level', num_updates_per_level_str,
            '--num-inserts', str(num_inserts)]

        try:
            subprocess.run(command)
        except subprocess.CalledProcessError as e:
            print(f"Error during laser cg_matrix preparation: {e}")
            sys.exit(1)

        command = [
            'python3', 'build.py',
            '--dimension-num-records', str(args.dimension_num_records),
            '--fact-num-records', str(args.fact_num_records),
            '--fact-num-attributes', str(args.fact_num_attributes),
            '--port', args.port,
            '--num-threads', str(args.num_insert_workers),
            '--repository-path', args.repository_path,
            '--compile-options', str(args.compile_options),
            '--db-name', args.db_name,
            '--mode', 'laser']

        try:
            subprocess.run(command)
        except subprocess.CalledProcessError as e:
            print(f"Error during laser dataset preparation: {e}")
            sys.exit(1)

        # After running the build, copy the DB_DATA_CURRENT to the named directory
        shutil.copytree(DB_DATA_CURRENT, data_dir_path)
        print(f"Copied {DB_DATA_CURRENT} to {data_dir_path}")

def prepare_locator_dataset(args, data_dir_name):
    data_dir_path = check_and_prepare_data_dir("locator", data_dir_name)

    if data_dir_path:
        command = [
            'python3', 'build.py',
            '--dimension-num-records', str(args.dimension_num_records),
            '--fact-num-records', str(args.fact_num_records),
            '--fact-num-attributes', str(args.fact_num_attributes),
            '--port', args.port,
            '--num-threads', str(args.num_insert_workers),
            '--repository-path', args.repository_path,
            '--compile-options', str(args.compile_options),
            '--db-name', args.db_name,
            '--locator-max-level', str(args.locator_max_level),
            '--mode', 'locator']

        try:
            subprocess.run(command)
        except subprocess.CalledProcessError as e:
            print(f"Error during locator dataset preparation: {e}")
            sys.exit(1)

        # After running the build, copy the DB_DATA_CURRENT to the named directory
        shutil.copytree(DB_DATA_CURRENT, data_dir_path)
        print(f"Copied {DB_DATA_CURRENT} to {data_dir_path}")

def measure_performance(args, interval=1):
    previous_transaction_counts = {
        "insert": {thread_id: 0 for thread_id in transaction_counts["insert"]},
        "update": {thread_id: 0 for thread_id in transaction_counts["update"]},
        "olap_point_lookup": {thread_id: 0 for thread_id in transaction_counts["olap_point_lookup"]},
        "olap_range_scan": {thread_id: 0 for thread_id in transaction_counts["olap_range_scan"]},
        "olap_join": {thread_id: 0 for thread_id in transaction_counts["olap_join"]},
    }

    previous_latency_data = {
        "insert": {thread_id: 0 for thread_id in latency_data["insert"]},
        "update": {thread_id: 0 for thread_id in latency_data["update"]},
        "olap_point_lookup": {thread_id: 0 for thread_id in transaction_counts["olap_point_lookup"]},
        "olap_range_scan": {thread_id: 0 for thread_id in transaction_counts["olap_range_scan"]},
        "olap_join": {thread_id: 0 for thread_id in transaction_counts["olap_join"]},
    }

    current_sec = 0

    while not stop_threads.is_set():
        time.sleep(interval)

        per_second_result = []

        per_second_result.append(f"{current_sec} / {args.duration_sec}")
        print(f"{current_sec} / {args.duration_sec}")
        
        # Aggregate metrics for each workload type
        for workload in ["insert", "update", "olap_point_lookup", "olap_range_scan", "olap_join"]:
            # Calculate TPS and average latency for the last interval
            tps = sum(transaction_counts[workload][thread_id] - previous_transaction_counts[workload][thread_id] for thread_id in transaction_counts[workload])
            total_latency = sum(latency_data[workload][thread_id] - previous_latency_data[workload][thread_id] for thread_id in latency_data[workload])

            avg_latency = total_latency / max(tps, 1)
            result_str = f"{workload.capitalize()} Throughput: {tps} TPS, Latency: {avg_latency} ms"

            per_second_result.append(result_str)
            print(result_str)

            # Update previous transaction counts and latency for the next interval
            previous_transaction_counts[workload] = transaction_counts[workload].copy()
            previous_latency_data[workload] = latency_data[workload].copy()

        performance_results.append("\n".join(per_second_result))
        print()

        current_sec += 1

def run_insert_query(args, thread_id):
    conn = None
    transaction_counts["insert"][thread_id] = 0
    latency_data["insert"][thread_id] = 0

    try:
        conn = psycopg2.connect(
            host='localhost',
            port=args.port,
            database=args.db_name
        )
        cursor = conn.cursor()

        # Start inserting records from initial_fact_num_records + 1
        current_id = args.fact_num_records + 1 + thread_id

        # To accumulate records in batches
        batch_values = []
        
        while not stop_threads.is_set():
            foreign_key = random.randint(1, args.dimension_num_records)
            values = [current_id, foreign_key]

            # Populate the rest of the attributes (a2, a3, ..., an)
            for _ in range(2, args.fact_num_attributes + 1):
                values.append(random.randint(1, 100))

            # Add the values to the batch
            batch_values.append(values)

            # If the batch reaches the size of 10, perform a batch insert
            if len(batch_values) == 10:
                columns = ["primary_key", "a1"] + [f"a{j}" for j in range(2, args.fact_num_attributes + 1)]
                columns_str = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(batch_values[0]))

                # Measure latency for the batch execution
                execute_start_time = time.time()
                
                # Create the SQL query for batch insert
                args_str = ",".join(cursor.mogrify(f"({placeholders})", value_set).decode("utf-8") for value_set in batch_values)
                cursor.execute(f"INSERT INTO fact ({columns_str}) VALUES {args_str}")
                conn.commit()
                
                latency = (time.time() - execute_start_time) * 1000  # Convert to milliseconds

                # Update the metrics for batch insertion
                transaction_counts["insert"][thread_id] += len(batch_values)
                latency_data["insert"][thread_id] += latency

                # Clear the batch after insertion
                batch_values.clear()

                # Update the current_id in the shared dictionary
                current_ids[thread_id] = current_id

            # Move to the next id for this thread 
            current_id += args.num_insert_workers

    except (Exception, psycopg2.Error) as error:
        print(f"Insert Query Error in thread {thread_id}: {error}")

    finally:
        if conn:
            cursor.close()
            conn.close()


def run_update_query(args, thread_id, assigned_insert_ids):
    conn = None
    transaction_counts["update"][thread_id] = 0
    latency_data["update"][thread_id] = 0

    # Initialize update id based on the first primary key each insert thread will use
    begin_update_id = {insert_id: args.fact_num_records + 1 + insert_id
                       for insert_id in assigned_insert_ids}

    # Determine the attributes to update based on update_num_projection
    update_attributes = [f"a{j}" for j in range(args.fact_num_attributes - args.update_num_projection + 1, args.fact_num_attributes + 1)]
    set_clause = ", ".join([f"{attr} = {attr} + 1" for attr in update_attributes])

    try:
        conn = psycopg2.connect(
            host='localhost',
            port=args.port,
            database=args.db_name
        )
        cursor = conn.cursor()

        # Batch values to update multiple records at once
        batch_size = 10  # Batch size (adjust as needed)
        batch_update_ids = []

        while not stop_threads.is_set():
            for insert_id in assigned_insert_ids:
                if insert_id in current_ids:
                    current_id = current_ids[insert_id]

                    # Gather IDs for batch update
                    for update_id in range(begin_update_id[insert_id], current_id + 1, args.num_insert_workers):
                        batch_update_ids.append(update_id)

                        # If batch is full, execute the update for all gathered IDs
                        if len(batch_update_ids) == batch_size:
                            execute_start_time = time.time()
                            update_placeholders = ",".join(["%s"] * len(batch_update_ids))
                            query = f"UPDATE fact SET {set_clause} WHERE primary_key IN ({update_placeholders})"
                            cursor.execute(query, batch_update_ids)
                            conn.commit()
                            latency = (time.time() - execute_start_time) * 1000  # Convert to milliseconds

                            # Update metrics
                            transaction_counts["update"][thread_id] += len(batch_update_ids)
                            latency_data["update"][thread_id] += latency

                            # Clear the batch
                            batch_update_ids.clear()

                        if stop_threads.is_set():
                            break

                    begin_update_id[insert_id] = current_id + args.num_insert_workers

                    if stop_threads.is_set():
                            break

    except (Exception, psycopg2.Error) as error:
        print(f"Update Query Error in thread {thread_id}: {error}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def run_olap_query(args, query_type):
    conn = None
    thread_id = threading.current_thread().ident
    transaction_counts[query_type][thread_id] = 0
    latency_data[query_type][thread_id] = 0

    time.sleep(args.ramp_up_duration_sec)

    try:
        conn = psycopg2.connect(
            host='localhost',
            port=args.port,
            database=args.db_name
        )
        cursor = conn.cursor()

        while not stop_threads.is_set():
            if query_type == 'olap_point_lookup':
                start_attribute = args.fact_num_attributes
                attributes = [f"a{start_attribute - i}" for i in range(args.point_lookup_num_projection)]
                attributes_str = ", ".join(attributes)

                max_inserted_id = max(current_ids.values(), default=0)
                if max_inserted_id == 0:
                    continue

                random_primary_key = random.randint(1, max_inserted_id)

                # Measure latency for the execution
                execute_start_time = time.time()
                query = f"SELECT {attributes_str} FROM fact WHERE primary_key = %s"
                cursor.execute(query, (random_primary_key,))
                conn.commit()
                latency = (time.time() - execute_start_time) * 1000  # Convert to milliseconds

            elif query_type == 'olap_range_scan':
                start_attribute = args.fact_num_attributes
                attributes = [f"a{start_attribute - i}" for i in range(args.range_scan_num_projection)]
                attributes_str = ", ".join(attributes)

                max_inserted_id = max(current_ids.values(), default=0)
                if max_inserted_id == 0:
                    continue

                random_start = random.randint(1, max_inserted_id - args.range_scan_num_selected_keys + 1)
                random_end = random_start + args.range_scan_num_selected_keys - 1

                # Measure latency for the execution
                execute_start_time = time.time()
                query = f"SELECT {attributes_str} FROM fact WHERE primary_key BETWEEN %s AND %s"
                cursor.execute(query, (random_start, random_end))
                conn.commit()
                latency = (time.time() - execute_start_time) * 1000  # Convert to milliseconds

            elif query_type == 'olap_join':
                attributes = [f"a{i}" for i in range(1, args.join_num_projection + 1)]
                attributes_str = ", ".join(attributes)

                like_value = random.randint(1, args.dimension_num_records)

                # Measure latency for the execution
                execute_start_time = time.time()
                query = f"""
                    SELECT {attributes_str} 
                    FROM fact 
                    JOIN dimension ON fact.a1 = dimension.primary_key 
                    WHERE dimension.data LIKE %s
                """
                like_pattern = f"data {like_value}"
                cursor.execute(query, (like_pattern,))
                conn.commit()
                latency = (time.time() - execute_start_time) * 1000  # Convert to milliseconds

            transaction_counts[query_type][thread_id] += 1
            latency_data[query_type][thread_id] += latency

    except (Exception, psycopg2.Error) as error:
        print(f"OLAP Query Error: {error}")

    finally:
        if conn:
            cursor.close()
            conn.close()

def run_threads(args):
    threads = []

    # Distribute insert thread IDs among update threads as evenly as possible
    insert_ids_per_update_thread = [[] for _ in range(args.num_update_workers)]

    for i in range(args.num_insert_workers):
        insert_ids_per_update_thread[i % args.num_update_workers].append(i)

    # Create and add Update threads
    for thread_id in range(args.num_update_workers):
        assigned_insert_ids = insert_ids_per_update_thread[thread_id]
        t = threading.Thread(target=run_update_query, args=(args, thread_id, assigned_insert_ids))
        threads.append(t)

    # Create and add Insert threads
    for thread_id in range(args.num_insert_workers):
        t = threading.Thread(target=run_insert_query, args=(args, thread_id))
        threads.append(t)

    olap_group_size = args.num_olap_workers // 3

    # Create and add point lookup threads
    for _ in range(olap_group_size):
        t = threading.Thread(target=run_olap_query, args=(args, 'olap_point_lookup'))
        threads.append(t)

    # Create and add range scan threads
    for _ in range(olap_group_size):
        t = threading.Thread(target=run_olap_query, args=(args, 'olap_range_scan'))
        threads.append(t)

    # Create and add join threads
    for _ in range(olap_group_size):
        t = threading.Thread(target=run_olap_query, args=(args, 'olap_join'))
        threads.append(t)

    # Start all threads
    for t in threads:
        t.start()

    # Start performance measurement thread
    performance_thread = threading.Thread(target=measure_performance, args=(args,))
    performance_thread.start()

    time.sleep(args.duration_sec)

    # Stop all threads
    stop_threads.set()

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Wait for the performance measurement thread to complete
    performance_thread.join()

def save_results(args):
    experiment_name = generate_experiment_name(args)
    result_filename = f"result_{args.mode}_{experiment_name}"
    performance_log_filename = f"performance_log_{args.mode}_{experiment_name}"

    print("Saving results...")

    with open(result_filename, 'w') as result_file:
        result_file.write(f"Experiment: {experiment_name}\n")
        
        # Aggregate final metrics
        for workload in ["insert", "update", "olap_point_lookup", "olap_range_scan", "olap_join"]:
            total_transactions = sum(transaction_counts[workload].values())
            total_latency = sum(latency_data[workload].values())
            
            avg_latency = total_latency / max(total_transactions, 1)
            tps = total_transactions / args.duration_sec
            
            result_file.write(f"{workload.capitalize()} Throughput: {tps} TPS, Latency: {avg_latency} ms\n")

    with open(performance_log_filename, 'w') as log_file:
        log_file.write("\n\n".join(performance_results))

def make_num_update_arg(args, num_inserts):
    num_update_workers = args.num_update_workers
    num_insert_workers = args.num_insert_workers
    num_levels = args.laser_num_levels

    # Calculate total updates based on the ratio of insert to update workers
    total_updates = int(num_inserts * (num_update_workers / num_insert_workers))

    # Generate the arithmetic sequence for updates per level
    # The first term (a1) and common difference (d) are calculated to ensure the sum is total_updates
    first_term = int(2 * total_updates / (num_levels * (num_levels + 1)))
    updates_per_level = [first_term + i * first_term for i in range(num_levels)]

    # Reverse the list to have the largest number at the front
    updates_per_level.reverse()

    # Create the updates per level string
    updates_per_level_str = ",".join(map(str, updates_per_level))

    return updates_per_level_str

def make_num_point_lookup_arg(args, num_inserts):
    num_olap_workers = args.num_olap_workers
    num_insert_workers = args.num_insert_workers
    num_levels = args.laser_num_levels

    # Calculate total point lookups based on the ratio of insert workers to OLAP workers
    if args.point_lookup_num_projection == 0:
        # If point_lookup_num_projection is 0, all levels have 0 point lookups
        point_lookups_per_level = [0] * num_levels
    else:
        total_point_lookups = int(num_inserts * (num_olap_workers / num_insert_workers))

        # Distribute point lookups evenly across all levels
        point_lookups_per_level = [total_point_lookups // num_levels] * num_levels

    # Create the point lookups per level string
    point_lookups_per_level_str = ",".join(map(str, point_lookups_per_level))

    return point_lookups_per_level_str

def make_num_range_scan_arg(args, num_inserts):
    num_olap_workers = args.num_olap_workers
    num_insert_workers = args.num_insert_workers
    num_levels = args.laser_num_levels

    # Calculate total range scans based on the ratio of insert workers to OLAP workers
    if args.range_scan_num_projection == 0:
        # If range_scan_num_projection is 0, all levels have 0 range scans
        range_scans_per_level = [0] * num_levels
    else:
        total_range_scans = int(num_inserts * (num_olap_workers / num_insert_workers))

        # Distribute range scans evenly across all levels
        range_scans_per_level = [total_range_scans // num_levels] * num_levels

    # Create the range scans per level string
    range_scans_per_level_str = ",".join(map(str, range_scans_per_level))

    return range_scans_per_level_str

def main():
    parser = argparse.ArgumentParser(description="Database Benchmark Program")

    parser.add_argument('--dimension-num-records', type=int, required=True, help="Number of records in the dimension table")
    parser.add_argument('--fact-num-records', type=int, required=True, help="Number of records in the fact table")
    parser.add_argument('--fact-num-attributes', type=int, required=True, help="Number of additional attributes in the fact table")
    parser.add_argument('--port', type=str, required=True, help="Port number of the PostgreSQL server")
    parser.add_argument('--repository-path', type=str, required=True, help="The repository path for this project")
    parser.add_argument('--compile-options', type=str, required=False, default=" ", help="Compile options for PostgreSQL server")
    parser.add_argument('--db-name', type=str, required=False, default="postgres", help="Name of database")
    parser.add_argument('--locator-max-level', type=int, required=False, default=0, help="Max level of locator")
    parser.add_argument('--laser-num-levels', type=int, required=True, help="Total number of levels of laser")
    parser.add_argument('--level-size-ratio', type=int, required=False, default=2, help="Size ration between adjacent levels")
    parser.add_argument('--point-lookup-num-projection', type=int, required=True, help="Number of attributes projected in point lookup")
    parser.add_argument('--range-scan-num-selected-keys', type=int, required=True, help="Range scan selectivity (number of entries selected)")
    parser.add_argument('--range-scan-num-projection', type=int, required=True, help="Number of attributes projected in range scan")
    parser.add_argument('--update-num-projection', type=int, required=True, help="Number of attributes projected in update")
    parser.add_argument('--join-num-projection', type=int, required=True, help="Number of attributes projected in join")
    parser.add_argument('--num-insert-workers', type=int, required=True, help="Number of insert workers")
    parser.add_argument('--num-update-workers', type=int, required=True, help="Number of update workers")
    parser.add_argument('--num-olap-workers', type=int, required=True, help="Number of olap workers")
    parser.add_argument('--duration-sec', type=int, required=True, help="Duration for how long the benchmark should run (in seconds)")
    parser.add_argument('--ramp-up-duration-sec', type=int, required=True, help="Duration for ramp-up period before OLAP threads start (in seconds)")
    parser.add_argument('--do-compile', type=str, required=False, default="False", choices=['True', 'False'],  help="Whther do compile or not")
    parser.add_argument('--mode', type=str, required=True, choices=['vanilla', 'laser', 'locator'], help="Mode for the benchmark")

    args = parser.parse_args()

    # Ensure only one of point_lookup_num_projection, range_scan_num_projection, or join_num_projection is non-zero
    non_zero_projections = sum([args.point_lookup_num_projection != 0,
                                args.range_scan_num_projection != 0,
                                args.join_num_projection != 0])

    if non_zero_projections != 3:
        print("Error: Provide projection num for every type of OLAP queries")
        sys.exit(1)

    experiment_name = generate_experiment_name(args)
    print(f"Starting experiment: {experiment_name}")

    global DB_BASE, DB_DATA, DB_DATA_CURRENT, DB_CONFIG, DB_SCRIPT, DB_SERVER_SCRIPT, DB_CLIENT_SCRIPT, DB_INSTALL_SCRIPT

    DB_BASE = "%s/PostgreSQL" % (args.repository_path)
    DB_DATA = DB_BASE + "/data"
    DB_DATA_CURRENT = DB_DATA + "/data_current"
    DB_CONFIG = DB_BASE + "/config"
    DB_SCRIPT = DB_BASE + "/script"
    DB_SERVER_SCRIPT = DB_SCRIPT + "/script_server"
    DB_CLIENT_SCRIPT = DB_SCRIPT + "/script_client"
    DB_INSTALL_SCRIPT = DB_SCRIPT + "/script_install"

    data_dir_name = generate_data_dir_name(args)
    print(f"Data dir name: {data_dir_name}")

    experiment_name = generate_experiment_name(args)
    print(f"Starting experiment: {experiment_name}")

    if args.mode == 'vanilla':
        prepare_vanilla_dataset(args, data_dir_name)
        if args.do_compile == 'True':
            compile_database(params=args.compile_options)
    elif args.mode == 'laser':
        prepare_laser_dataset(args, data_dir_name)
        if args.do_compile == 'True':
            compile_database(params=(args.compile_options + " -DLASER -DLSM_TXN "))
            subprocess.run(['make'], cwd=(DB_BASE + "/postgres/contrib/lsm"))
            subprocess.run(['make', 'install'], cwd=(DB_BASE + "/postgres/contrib/lsm"))
    elif args.mode == 'locator':
        prepare_locator_dataset(args, data_dir_name)
        if args.do_compile == 'True':
            compile_database(params=(args.compile_options + " -DDIVA -DLOCATOR "))
            subprocess.run(['make'], cwd=(DB_BASE + "/postgres/contrib/intarray"))
            subprocess.run(['make', 'install'], cwd=(DB_BASE + "/postgres/contrib/intarray"))
            subprocess.run(['make', 'PG_CONFIG=' + DB_BASE + '/pgsql/bin/pg_config'],
                cwd=(DB_BASE + "/pg_hint_plan"))
            subprocess.run(['make', 'PG_CONFIG=' + DB_BASE + '/pgsql/bin/pg_config', 'install'],
                cwd=(DB_BASE + "/pg_hint_plan"))

    os.system(f"sudo {args.repository_path}/evaluation/drop_cache")

    run_server(params=" -d=%s " % (DB_DATA))

    try:
        run_threads(args)

        # Save the results to a file
        save_results(args)
    except:
        print(f"Error while running {experiment_name} benchmark", error)
        shutdown_server(params=" -d=%s " % (DB_DATA))

    shutdown_server(params=" -d=%s " % (DB_DATA))

if __name__ == "__main__":
    main()

