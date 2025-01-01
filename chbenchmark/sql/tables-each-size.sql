SELECT relname, oid, pg_size_pretty(pg_relation_size(oid))
FROM (SELECT relname, oid FROM pg_class WHERE oid >= 16384) AS t
ORDER BY relname;
