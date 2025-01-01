./pgsql/bin/psql -p 5555 -d postgres -c "CREATE DATABASE locator;"
./pgsql/bin/psql -p 5555 -d locator -c "CREATE ACCESS METHOD hap TYPE TABLE HANDLER locator_hap_handler;"
./pgsql/bin/psql -p 5555 -d locator -c "CREATE ACCESS METHOD locator TYPE TABLE HANDLER locatoram_handler;"
./pgsql/bin/psql -p 5555 -d locator -c "CREATE EXTENSION intarray;"
