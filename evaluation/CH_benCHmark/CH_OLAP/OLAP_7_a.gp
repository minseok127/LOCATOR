set term epscairo enhanced size 8in, 3.5in background rgb 'white'
set output "figure_7_a.eps"

$tmp << EOD
1 1
EOD

BOX_WIDTH = 0.145

CLICKHOUSE = "#ffb900"
DUCKDB = "#ffde00"
CITUS = "#ffff80"
VANILLA = "#37517f"
DIVA = "#af7ac5"
LOCATOR = "#58d68d"

dir_exists(dir) = system("[ -d '".dir."' ] && echo '1' || echo '0'") + 0

# CLICKHOUSE_exists = dir_exists("./OLAP_results/latest/results_CLICKHOUSE")
# DUCKDB_exists = dir_exists("./OLAP_results/latest/results_DUCKDB")
CITUS_exists = dir_exists("./OLAP_results/latest/results_CITUS")
VANILLA_exists = dir_exists("./OLAP_results/latest/results_VANILLA")
DIVA_exists = dir_exists("./OLAP_results/latest/results_DIVA")
LOCATOR_exists = dir_exists("./OLAP_results/latest/results_LOCATOR")

set logscale y 10

set xrange [-0.6:7.6]
set yrange [0.5:800]

set ylabel "Query Latency (sec)" font "Arial-Bold, 22" offset -2.0, -5

set multiplot

unset xtics
set ytics nomirror ("0.1"0.1, "1"1, "10"10, "100"100) font "Arial, 22" scale 0.0

set lmargin at screen 0.08
set rmargin at screen 0.99
set tmargin at screen 1. - 0.13
set bmargin at screen 1. - 0.5

set style data histograms
set style line 1 dt 3 lw 5 lc rgb 'black'

set style arrow 1 heads size screen 0.008,90 ls 2 dt 1 lw 3 lc rgb "#000000"
set style arrow 2 heads filled size 0.04, 50 ls 2 dt 1 lw 3 lc rgb "#000000"
set style arrow 3 nohead ls 7 lt 3 dt 2 lw 4 lc rgb "#222222"
set style textbox opaque noborder margins 0, 1

unset xlabel

set style fill solid 1.00 border -1
set boxwidth BOX_WIDTH

set grid y lw 5
plot NaN notitle
unset grid
unset ytics
unset ylabel

# if ( CLICKHOUSE_exists ) plot "<(sed -n '2,9p' ./OLAP_results/latest/results_ClickHouse/query_latency.txt)" using ($0 - (BOX_WIDTH*5/2)):($2/1000) with boxes fs solid lt rgb CLICKHOUSE notitle
# if ( DUCKDB_exists ) plot "<(sed -n '2,9p' ./OLAP_results/latest/results_DuckDB/query_latency.txt)" using ($0 - (BOX_WIDTH*3/2)):($2/1000) with boxes fs solid lt rgb DUCKDB notitle
if ( CITUS_exists ) plot "<(sed -n '2,9p' ./OLAP_results/latest/results_CITUS/query_latency.txt)" using ($0 - (BOX_WIDTH/2)):($2/1000) with boxes fs solid lt rgb CITUS notitle
if ( VANILLA_exists ) plot "<(sed -n '2,9p' ./OLAP_results/latest/results_VANILLA/query_latency.txt)" using ($0 + (BOX_WIDTH/2)):($2/1000) with boxes fs solid lt rgb VANILLA notitle
if ( DIVA_exists ) plot "<(sed -n '2,9p' ./OLAP_results/latest/results_DIVA/query_latency.txt)" using ($0 + (BOX_WIDTH*3/2)):($2/1000) with boxes fs solid lt rgb DIVA notitle
if ( LOCATOR_exists ) plot "<(sed -n '2,9p' ./OLAP_results/latest/results_LOCATOR/query_latency.txt)" using ($0 + (BOX_WIDTH*5/2)):($2/1000) with boxes fs solid lt rgb LOCATOR notitle

set xtics font "Arial, 22" scale 0.0
plot "" using ($0):(0):xtic(sprintf("%d", $1)) with boxes notitle
unset xtics

if (DIVA_exists && LOCATOR_exists) {
	plot "<(sed -n '2,9p' ./OLAP_results/latest/results_DIVA/query_latency.txt)" using ($0 + (BOX_WIDTH*3/2)):($2/1000):(BOX_WIDTH):(0) with vectors arrowstyle 3 notitle, \
		 "<(join ./OLAP_results/latest/results_DIVA/query_latency.txt ./OLAP_results/latest/results_LOCATOR/query_latency.txt | sed -n '2,9p')" using ($0 + (BOX_WIDTH*5/2)):($3/1000):(0):(($2 - $3)/1000) with vectors arrowstyle 2 notitle, \
		 "<(join ./OLAP_results/latest/results_DIVA/query_latency.txt ./OLAP_results/latest/results_LOCATOR/query_latency.txt | sed -n '2,9p')" using ($0 + (BOX_WIDTH*5/2) + 0.04):(10 ** ((log10($2/1000) + log10($3/1000)) / 2)):(sprintf("%.0fx", $2 / $3)) with labels boxed center rotate by 90 font "Calibri, 20" notitle
}

set tmargin at screen 1. - 0.5
set bmargin at screen 1. - 0.87

set xrange [-0.6:7.6]
set yrange [0.5:1500]
set ytics nomirror ("0.1"0.1, "1"1, "10"10, "100"100) font "Arial, 22" scale 0.0

set style fill solid 1.00 border -1

set grid y lw 5
plot NaN notitle
unset grid
unset ytics
unset xlabel
unset ylabel

# if (CLICKHOUSE_exists) plot "<(sed -n '10,17p' ./OLAP_results/latest/results_ClickHouse/query_latency.txt)" using ($0 - (BOX_WIDTH*5/2)):($2/1000) with boxes fs solid lt rgb CLICKHOUSE notitle
# if (DUCKDB_exists) plot "<(sed -n '10,17p' ./OLAP_results/latest/results_DuckDB/query_latency.txt)" using ($0 - (BOX_WIDTH*3/2)):($2/1000) with boxes fs solid lt rgb DUCKDB notitle
if (CITUS_exists) plot "<(sed -n '10,17p' ./OLAP_results/latest/results_CITUS/query_latency.txt)" using ($0 - (BOX_WIDTH/2)):($2/1000) with boxes fs solid lt rgb CITUS notitle
if (VANILLA_exists) plot "<(sed -n '10,17p' ./OLAP_results/latest/results_VANILLA/query_latency.txt)" using ($0 + (BOX_WIDTH/2)):($2/1000) with boxes fs solid lt rgb VANILLA notitle
if (DIVA_exists) plot "<(sed -n '10,17p' ./OLAP_results/latest/results_DIVA/query_latency.txt)" using ($0 + (BOX_WIDTH*3/2)):($2/1000) with boxes fs solid lt rgb DIVA notitle
if (LOCATOR_exists) plot "<(sed -n '10,17p' ./OLAP_results/latest/results_LOCATOR/query_latency.txt)" using ($0 + (BOX_WIDTH*5/2)):($2/1000) with boxes fs solid lt rgb LOCATOR notitle

set xlabel "TPC-CH Query #" font "Arial, 22" offset 0, -0.3
set xtics font "Arial, 22" scale 0.0
plot "" using ($0):(0):xtic(sprintf("%d", $1)) with boxes notitle
unset xlabel
unset xtics

if (DIVA_exists && LOCATOR_exists) {
	plot "<(sed -n '10,17p' ./OLAP_results/latest/results_DIVA/query_latency.txt)" using ($0 + (BOX_WIDTH*3/2)):($2/1000):(BOX_WIDTH):(0) with vectors arrowstyle 3 notitle, \
		 "<(join ./OLAP_results/latest/results_DIVA/query_latency.txt ./OLAP_results/latest/results_LOCATOR/query_latency.txt | sed -n '10,17p')" using ($0 + (BOX_WIDTH*5/2)):($3/1000):(0):(($2 - $3)/1000) with vectors arrowstyle 2 notitle, \
		 "<(join ./OLAP_results/latest/results_DIVA/query_latency.txt ./OLAP_results/latest/results_LOCATOR/query_latency.txt | sed -n '10,17p')" using ($0 + (BOX_WIDTH*5/2) + 0.04):(10 ** ((log10($2/1000) + log10($3/1000)) / 2)):(sprintf("%.0fx", $2 / $3)) with labels boxed center rotate by 90 font "Calibri, 20" notitle
}

set lmargin at screen 0
set rmargin at screen 1
set tmargin at screen 1
set bmargin at screen 0

set xrange [0:8]
set yrange [0.01:4.5]
unset border

unset logscale y

unset xtics
unset xlabel
unset grid

set key reverse Left outside horizontal font "Arial, 20" samplen 1 width 5
pos = 0.0

pos = pos + 2.5
set key at pos, 4.5
plot "" using (0):(0) with boxes fs solid lt rgb CLICKHOUSE title "ClickHouse"

pos = pos + 3
set key at pos, 4.5
plot "" using (0):(0) with boxes fs solid lt rgb DUCKDB title "DuckDB (Parquet)"

pos = pos + 3.1
set key at pos, 4.5
plot "" using (0):(0) with boxes fs solid lt rgb CITUS title "PostgreSQL(PG) w/ Citus"

pos = 0.0

pos = pos + 2.5
set key at pos, 4.2
plot "" using (0):(0) with boxes fs solid lt rgb VANILLA title "Vanilla PG"

pos = pos + 2.38
set key at pos, 4.2
plot "" using (0):(0) with boxes fs solid lt rgb DIVA title "PG w/ DIVA"

pos = pos + 2.69
set key at pos, 4.2
plot "" using (0):(0) with boxes fs solid lt rgb LOCATOR title "PG w/ LOCATOR"

unset key