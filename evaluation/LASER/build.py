import sys, subprocess, signal
import psycopg2
import os, random
import argparse
import time
import re
from concurrent.futures import ThreadPoolExecutor

def compile_database(params=None):
    print("Compile... flag: %s" % (params))
    subprocess.run(args=DB_INSTALL_SCRIPT + "/install.sh" + params,
        stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT,
        check=True, shell=True)

def init_server(params=None):
    subprocess.run(args=DB_SERVER_SCRIPT + "/init_server.sh" + params,
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
    # Create dimension table
    cursor.execute("""
    CREATE TABLE dimension (
        primary_key INT PRIMARY KEY,
        data TEXT
    );
    """)

    # Create fact table with dynamic attributes
    columns = ["primary_key BIGINT PRIMARY KEY", 
               "a1 INT REFERENCES dimension(primary_key)"]
    
    for i in range(2, num_attributes + 1):
        columns.append(f"a{i} INT")
    
    columns_str = ", ".join(columns)
    
    cursor.execute(f"""
    CREATE TABLE fact (
        {columns_str}
    );
    """)

def create_laser_tables(cursor, num_attributes):
    # Create dimension table
    cursor.execute("""
    CREATE TABLE dimension (
        primary_key INT PRIMARY KEY,
        data TEXT
    );
    """)

    # Create fact table with dynamic attributes
    columns = ["primary_key BIGINT", 
               "a1 INT"]
    
    for i in range(2, num_attributes + 1):
        columns.append(f"a{i} INT")
    
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
        primary_key INT PRIMARY KEY,
        data TEXT
    ) USING HAP;
    """)

    # Create fact table with dynamic attributes
    columns = ["primary_key BIGINT PRIMARY KEY", 
               "a1 INT REFERENCES dimension(primary_key)"]
    
    for i in range(2, num_attributes + 1):
        columns.append(f"a{i} INT")
    
    columns_str = ", ".join(columns)
    
    cursor.execute(f"""
    CREATE TABLE fact (
        {columns_str}
    ) USING HAP;
    """)

def insert_dimension_data(cursor, num_records):
    # Insert data into dimension table
    for i in range(1, num_records + 1):
        data = f"data {i}"
        cursor.execute("INSERT INTO dimension (primary_key, data) VALUES (%s, %s);", (i, data))
    print("Dimension table is created and data is inserted")

def insert_fact_data_batch(connection_string, start_id, end_id, num_attributes, dimension_size, batch_size=10000):
    try:
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()

        # To accumulate records in batches
        batch_values = []
        
        for i in range(start_id, end_id + 1):
            foreign_key = random.randint(1, dimension_size)  # a1
            values = [i, foreign_key]
            
            for _ in range(2, num_attributes + 1):
                values.append(random.randint(1, 100))  # Random value for each a2, ...
            
            batch_values.append(values)
            
            # If the batch size reaches the limit, insert the batch
            if len(batch_values) == batch_size:
                # Prepare column names and placeholders
                columns = ["primary_key", "a1"] + [f"a{j}" for j in range(2, num_attributes + 1)]
                columns_str = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(values))
                
                # Batch insert
                args_str = ",".join(cursor.mogrify(f"({placeholders})", value_set).decode("utf-8") for value_set in batch_values)
                cursor.execute(f"INSERT INTO fact ({columns_str}) VALUES {args_str}")
                connection.commit()
                
                # Clear the batch_values after insert
                batch_values.clear()

        # Insert any remaining records that are less than the batch size
        if batch_values:
            columns = ["primary_key", "a1"] + [f"a{j}" for j in range(2, num_attributes + 1)]
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(values))
            
            args_str = ",".join(cursor.mogrify(f"({placeholders})", value_set).decode("utf-8") for value_set in batch_values)
            cursor.execute(f"INSERT INTO fact ({columns_str}) VALUES {args_str}")
            connection.commit()

    except (Exception, psycopg2.Error) as error:
        print(f"Error while inserting data in range {start_id}-{end_id}: {error}")
    
    finally:
        if connection:
            cursor.close()
            connection.close()

def populate_fact_table_dataset(args, connection_string):
    # Calculate the batch size for each thread
    batch_size = args.fact_num_records // args.num_threads
    ranges = [(i * batch_size + 1, (i + 1) * batch_size) for i in range(args.num_threads)]

    # Handle any remainder records
    if args.fact_num_records % args.num_threads != 0:
       ranges[-1] = (ranges[-1][0], args.fact_num_records)

    # Use ThreadPoolExecutor to insert fact data in parallel
    with ThreadPoolExecutor(max_workers=args.num_threads) as executor:
        futures = [
            executor.submit(insert_fact_data_batch, connection_string, start_id, end_id, args.fact_num_attributes, args.dimension_num_records)
            for start_id, end_id in ranges
        ]
        for future in futures:
            future.result()  # Ensure all threads have completed

    print("Tables created and data is inserted successfully.")

def build_vanilla_dataset(args):
    print("Vanilla PostgreSQL has started building the dataset")

    connection_string = f"dbname={args.db_name} host='localhost' port={args.port}"

    compile_database(params=args.compile_options)

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

def build_laser_dataset(args):
    print("Laser has started building the dataset")

    connection_string = f"dbname={args.db_name} host='localhost' port={args.port}"

    compile_database(params=(args.compile_options + " -DLASER -DLSM_TXN "))

    # Compile and install LSM extension
    subprocess.run(['make'], cwd=(DB_BASE + "/postgres/contrib/lsm"))
    subprocess.run(['make', 'install'], cwd=(DB_BASE + "/postgres/contrib/lsm"))

    os.system("rm -rf %s" % (DB_DATA_CURRENT))

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

    os.system("cp cg_matrix %s/lsm/"
        % (DB_DATA_CURRENT))

    try:
        # Connect to the PostgreSQL database to create tables
        connection = psycopg2.connect(connection_string)
        cursor = connection.cursor()

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

def build_locator_dataset(args):
    print("Locator has started building the dataset")

    connection_string = f"dbname={args.db_name} host='localhost' port={args.port}"

    compile_database(params=(args.compile_options + " -DDIVA -DLOCATOR "))

    # Compile and install intarray extension
    subprocess.run(['make'], cwd=(DB_BASE + "/postgres/contrib/intarray"))
    subprocess.run(['make', 'install'], cwd=(DB_BASE + "/postgres/contrib/intarray"))

    # Compile and install pg_hint_plan extension
    subprocess.run(['make', 'PG_CONFIG=' + DB_BASE + '/pgsql/bin/pg_config'],
        cwd=(DB_BASE + "/pg_hint_plan"))
    subprocess.run(['make', 'PG_CONFIG=' + DB_BASE + '/pgsql/bin/pg_config', 'install'],
        cwd=(DB_BASE + "/pg_hint_plan"))

    os.system("rm -rf %s" % (DB_DATA_CURRENT))

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
        cursor.close()
        connection.close()

        # Because this function operates in a multithreaded environment, the
        # existing connection is closed and a new connection is assigned to each
        # thread
        populate_fact_table_dataset(args, connection_string)

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

def main():
    parser = argparse.ArgumentParser(description="Create dimension and fact tables in PostgreSQL and populate them with data.")
    
    parser.add_argument('--dimension-num-records', type=int, required=True, help="Number of records in the dimension table")
    parser.add_argument('--fact-num-records', type=int, required=True, help="Number of records in the fact table")
    parser.add_argument('--fact-num-attributes', type=int, required=True, help="Number of additional attributes in the fact table")
    parser.add_argument('--port', type=str, required=True, help="Port number of the PostgreSQL server")
    parser.add_argument('--num-threads', type=int, required=True, help="Number of threads to use for inserting fact data")
    parser.add_argument('--repository-path', type=str, required=True, help="The repository path for this project")
    parser.add_argument('--compile-options', type=str, required=False, default=" ", help="Compile options for PostgreSQL server")
    parser.add_argument('--db-name', type=str, required=False, default="postgres", help="Name of database")
    parser.add_argument('--locator-max-level', type=int, required=False, default=0, help="Max level of locator")
    parser.add_argument('--mode', type=str, required=True, choices=['vanilla', 'laser', 'locator'], help="Mode for the benchmark")
    
    args = parser.parse_args()

    global DB_BASE, DB_DATA, DB_DATA_CURRENT, DB_CONFIG, DB_SCRIPT, DB_SERVER_SCRIPT, DB_CLIENT_SCRIPT, DB_INSTALL_SCRIPT

    DB_BASE = "%s/PostgreSQL" % (args.repository_path)
    DB_DATA = DB_BASE + "/data"
    DB_DATA_CURRENT = DB_DATA + "/data_current"
    DB_CONFIG = DB_BASE + "/config"
    DB_SCRIPT = DB_BASE + "/script"
    DB_SERVER_SCRIPT = DB_SCRIPT + "/script_server"
    DB_CLIENT_SCRIPT = DB_SCRIPT + "/script_client"
    DB_INSTALL_SCRIPT = DB_SCRIPT + "/script_install"

    if args.mode == 'vanilla':
        build_vanilla_dataset(args)
    elif args.mode == 'laser':
        build_laser_dataset(args)
    elif args.mode == 'locator':
        build_locator_dataset(args)

if __name__ == "__main__":
    main()

