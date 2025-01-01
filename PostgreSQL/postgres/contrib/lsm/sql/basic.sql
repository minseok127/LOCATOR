--
-- Test basic select, insert, delete, update operations
--

\c lsmtest

CREATE FOREIGN TABLE test(key TEXT, value TEXT) SERVER lsm_server;  

INSERT INTO test VALUES('YC', 'VidarDB');  
SELECT * FROM test;  

INSERT INTO test VALUES('California', 'Waterloo');  
SELECT * FROM test;  

DELETE FROM test WHERE key='California';  
SELECT * FROM test;  

UPDATE test SET value='VidarSQL';  
SELECT * FROM test;  

DROP FOREIGN TABLE test;  
