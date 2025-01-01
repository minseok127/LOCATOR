CREATE DATABASE IF NOT EXISTS {pg_database};

USE {pg_database};

DROP TABLE IF EXISTS nation;
DROP TABLE IF EXISTS region;
DROP TABLE IF EXISTS history;
DROP TABLE IF EXISTS warehouse;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS new_order;
DROP TABLE IF EXISTS supplier;
DROP TABLE IF EXISTS customer;
DROP TABLE IF EXISTS district;
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS order_line;


CREATE TABLE nation ENGINE = MergeTree() ORDER BY (n_nationkey) 
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'nation', '{pg_user}', '{pg_password}');

CREATE TABLE region ENGINE = MergeTree() ORDER BY (r_regionkey) 
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'region', '{pg_user}', '{pg_password}');

CREATE TABLE history ENGINE = MergeTree() ORDER BY tuple()
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'history', '{pg_user}', '{pg_password}');

CREATE TABLE warehouse ENGINE = MergeTree() ORDER BY (w_id)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'warehouse', '{pg_user}', '{pg_password}');

CREATE TABLE item ENGINE = MergeTree() ORDER BY (i_id)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'item', '{pg_user}', '{pg_password}');

CREATE TABLE new_order ENGINE = MergeTree() ORDER BY (no_w_id, no_d_id, no_o_id)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'new_order', '{pg_user}', '{pg_password}');

CREATE TABLE supplier ENGINE = MergeTree() ORDER BY (su_suppkey)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'supplier', '{pg_user}', '{pg_password}');

CREATE TABLE customer ENGINE = MergeTree() ORDER BY (c_w_id, c_d_id, c_id)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'customer', '{pg_user}', '{pg_password}');

CREATE TABLE district ENGINE = MergeTree() ORDER BY (d_w_id, d_id)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'district', '{pg_user}', '{pg_password}');

CREATE TABLE stock ENGINE = MergeTree() ORDER BY (s_i_id, s_w_id)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'stock', '{pg_user}', '{pg_password}');

CREATE TABLE orders ENGINE = MergeTree() ORDER BY (o_w_id, o_d_id, o_id)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'orders', '{pg_user}', '{pg_password}');

CREATE TABLE order_line ENGINE = MergeTree() ORDER BY (ol_w_id, ol_d_id, ol_o_id, ol_number)
AS SELECT * FROM postgresql('{pg_host}:{pg_port}', '{pg_database}', 'order_line', '{pg_user}', '{pg_password}');


OPTIMIZE TABLE nation FINAL;
OPTIMIZE TABLE region FINAL;
OPTIMIZE TABLE history FINAL;
OPTIMIZE TABLE warehouse FINAL;
OPTIMIZE TABLE item FINAL;
OPTIMIZE TABLE new_order FINAL;
OPTIMIZE TABLE supplier FINAL;
OPTIMIZE TABLE customer FINAL;
OPTIMIZE TABLE district FINAL;
OPTIMIZE TABLE stock FINAL;
OPTIMIZE TABLE orders FINAL;
OPTIMIZE TABLE order_line FINAL;
