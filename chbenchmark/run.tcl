#!/bin/tclsh
puts "SETTING CONFIGURATION"
global complete
proc wait_to_complete {} {
global complete
set complete [vucomplete]
if {!$complete} { after 15000 wait_to_complete } else { exit }
}
dbset db pg
loadscript
diset connection pg_host $env(PGHOST)
diset connection pg_port $env(PGPORT)
diset tpcc pg_dbase $env(PGDATABASE)
diset tpcc pg_user $env(PGUSER)
diset tpcc pg_superuser $env(PGUSER)
diset tpcc pg_defaultdbase $env(PGDATABASE)
diset tpcc pg_pass $env(PGPASSWORD)
diset tpcc pg_superuserpass $env(PGPASSWORD)
diset tpcc pg_storedprocs true
diset tpcc pg_count_ware 500
diset tpcc pg_driver timed
diset tpcc pg_rampup 0
diset tpcc pg_duration 20
diset tpcc pg_total_iterations 100000000
diset tpcc pg_raiseerror true
diset tpcc pg_timeprofile true
loadscript

tcset logtotemp 1
tcset timestamps 1
tcstart
tcstatus

print dict
vuset vu 12
vuset timestamps 1
vuset logtotemp 1
vuset showoutput 1
vuset unique 1
vuset delay 100
vuset repeat 1
vurun
wait_to_complete
vwait forever
