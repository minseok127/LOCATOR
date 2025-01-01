set term epscairo enhanced size 8in, 3.5in background rgb 'white'
set output "figure_11_c.eps"

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

# CLICKHOUSE_exists = dir_exists("../CH_OLAP/OLAP_results/latest/results_CLICKHOUSE")
# DUCKDB_exists = dir_exists("../CH_OLAP/OLAP_results/latest/results_DUCKDB")
CITUS_exists = dir_exists("../CH_OLAP/OLAP_results/latest/results_CITUS")
VANILLA_exists = dir_exists("./HTAP_results/latest/results_VANILLA")
DIVA_exists = dir_exists("./HTAP_results/latest/results_DIVA")
LOCATOR_exists = dir_exists("./HTAP_results/latest/results_LOCATOR")

array off[18] = [0, 0, 0.05, 0.05, 0.1, 0, 0, 0, 0, 0.05, 0, 0.05, 0, 0, 0, 0, 0, 0.1]
set logscale y 10

set xrange [-0.6:7.6]
set yrange [0.03:500]

set ylabel "I/O Amount (GiB)" font "Arial-Bold, 22" offset -2.0, -5

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
set style arrow 3 nohead ls 7 lt 3 dt 2 lw 2 lc rgb "#222222"
set style textbox opaque noborder margins 0, 1

unset xlabel

set style fill solid 1.00 border -1
set boxwidth BOX_WIDTH

set grid y lw 5
plot NaN notitle
unset grid
unset ytics
unset ylabel

# if ( CLICKHOUSE_exists ) plot "<(sed -n '2,9p' ../CH_OLAP/OLAP_results/latest/results_ClickHouse/readIO.txt)" using ($0 - (BOX_WIDTH*5/2)):($2/1073741824) with boxes fs solid lt rgb CLICKHOUSE notitle, \
	"<(sed -n '2,9p' ../CH_OLAP/OLAP_results/latest/results_ClickHouse/readIO.txt)" using ($0 - (BOX_WIDTH*5/2)):($2/1073741824) with boxes fs pattern 7 transparent lt rgb 'black' notitle
# if ( DUCKDB_exists ) plot "<(sed -n '2,9p' ../CH_OLAP/OLAP_results/latest/results_DuckDB/readIO.txt)" using ($0 - (BOX_WIDTH*3/2)):($2/1073741824) with boxes fs solid lt rgb DUCKDB notitle, \
	"<(sed -n '2,9p' ../CH_OLAP/OLAP_results/latest/results_DuckDB/readIO.txt)" using ($0 - (BOX_WIDTH*3/2)):($2/1073741824) with boxes fs pattern 7 transparent lt rgb 'black' notitle
if ( CITUS_exists ) plot "<(sed -n '2,9p' ../CH_OLAP/OLAP_results/latest/results_CITUS/readIO.txt)" using ($0 - (BOX_WIDTH/2)):($2/1073741824) with boxes fs solid lt rgb CITUS notitle, \
	"<(sed -n '2,9p' ../CH_OLAP/OLAP_results/latest/results_CITUS/readIO.txt)" using ($0 - (BOX_WIDTH/2)):($2/1073741824) with boxes fs pattern 7 transparent lt rgb 'black' notitle
if ( VANILLA_exists ) plot "<(sed -n '2,9p' ./HTAP_results/latest/results_VANILLA/readIO.txt)" using ($0 + (BOX_WIDTH/2)):($2/1073741824) with boxes fs solid lt rgb VANILLA notitle
if ( DIVA_exists ) plot "<(sed -n '2,9p' ./HTAP_results/latest/results_DIVA/readIO.txt)" using ($0 + (BOX_WIDTH*3/2)):($2/1073741824) with boxes fs solid lt rgb DIVA notitle
if ( LOCATOR_exists ) plot "<(sed -n '2,9p' ./HTAP_results/latest/results_LOCATOR/readIO.txt)" using ($0 + (BOX_WIDTH*5/2)):($2/1073741824) with boxes fs solid lt rgb LOCATOR notitle

set xtics font "Arial, 22" scale 0.0
plot "" using ($0):(0):xtic(sprintf("%d", $1)) with boxes notitle
unset xtics

if ( CITUS_exists ) {
	set label sprintf("OLAP\nonly") at 2 - (BOX_WIDTH/2) - 0.15, 150 center font "Arial, 20"
	plot "<(sed -n '3p' ../CH_OLAP/OLAP_results/latest/results_CITUS/readIO.txt)" using (2 - BOX_WIDTH*2):($2/1073741824 - 25):(sprintf('\\}')) with labels left font "Arial, 42" rotate by 90 notitle
	unset label
}

if (DIVA_exists && LOCATOR_exists ) {
	plot "<(sed -n '2,9p' ./HTAP_results/latest/results_DIVA/readIO.txt)" using ($0 + (BOX_WIDTH*3/2)):($2/1073741824):(BOX_WIDTH):(0) with vectors arrowstyle 3 notitle, \
		 "<(join ./HTAP_results/latest/results_DIVA/readIO.txt ./HTAP_results/latest/results_LOCATOR/readIO.txt | sed -n '2,9p')" using ($0 + (BOX_WIDTH*5/2)):($6/1073741824):(0):(($2 - $6)/1073741824) with vectors arrowstyle 2 notitle, \
		 "<(join ./HTAP_results/latest/results_DIVA/readIO.txt ./HTAP_results/latest/results_LOCATOR/readIO.txt | sed -n '2,9p')" using ($0 + (BOX_WIDTH*5/2) + 0.04):(10 ** ((log10($2/1073741824) + log10($6/1073741824)) / 2)):(sprintf("%.0fx", $2 / $6)) with labels boxed center rotate by 90 font "Calibri, 20" notitle
}

set tmargin at screen 1. - 0.5
set bmargin at screen 1. - 0.87

set xrange [-0.6:7.6]
set yrange [0.03:500]
set ytics nomirror ("0.1"0.1, "1"1, "10"10, "100"100) font "Arial, 20" scale 0.0

set style fill solid 1.00 border -1

unset xtics
set grid y lw 5
plot NaN notitle
unset grid
unset ytics
unset xlabel
unset ylabel

# if (CLICKHOUSE_exists) plot "<(sed -n '10,17p' ../CH_OLAP/OLAP_results/latest/results_ClickHouse/readIO.txt)" using ($0 - (BOX_WIDTH*5/2)):($2/1073741824) with boxes fs solid lt rgb CLICKHOUSE notitle, \
	"<(sed -n '10,17p' ../CH_OLAP/OLAP_results/latest/results_ClickHouse/readIO.txt)" using ($0 - (BOX_WIDTH*5/2)):($2/1073741824) with boxes fs pattern 7 transparent lt rgb 'black' notitle
# if (DUCKDB_exists) plot "<(sed -n '10,17p' ../CH_OLAP/OLAP_results/latest/results_DuckDB/readIO.txt)" using ($0 - (BOX_WIDTH*3/2)):($2/1073741824) with boxes fs solid lt rgb DUCKDB notitle, \
	"<(sed -n '10,17p' ../CH_OLAP/OLAP_results/latest/results_DuckDB/readIO.txt)" using ($0 - (BOX_WIDTH*3/2)):($2/1073741824) with boxes fs pattern 7 transparent lt rgb 'black' notitle
if (CITUS_exists) plot "<(sed -n '10,17p' ../CH_OLAP/OLAP_results/latest/results_CITUS/readIO.txt)" using ($0 - (BOX_WIDTH/2)):($2/1073741824) with boxes fs solid lt rgb CITUS notitle, \
	"<(sed -n '10,17p' ../CH_OLAP/OLAP_results/latest/results_CITUS/readIO.txt)" using ($0 - (BOX_WIDTH/2)):($2/1073741824) with boxes fs pattern 7 transparent lt rgb 'black' notitle
if (VANILLA_exists) plot "<(sed -n '10,17p' ./HTAP_results/latest/results_VANILLA/readIO.txt)" using ($0 + (BOX_WIDTH/2)):($2/1073741824) with boxes fs solid lt rgb VANILLA notitle
if (DIVA_exists) plot "<(sed -n '10,17p' ./HTAP_results/latest/results_DIVA/readIO.txt)" using ($0 + (BOX_WIDTH*3/2)):($2/1073741824) with boxes fs solid lt rgb DIVA notitle
if (LOCATOR_exists) plot "<(sed -n '10,17p' ./HTAP_results/latest/results_LOCATOR/readIO.txt)" using ($0 + (BOX_WIDTH*5/2)):($2/1073741824) with boxes fs solid lt rgb LOCATOR notitle

set xlabel "TPC-CH Query #" font "Arial, 22" offset 0, -0.3
set xtics font "Arial, 22" scale 0.0
plot "" using ($0):(0):xtic(sprintf("%d", $1)) with boxes notitle
unset xlabel
unset xtics

if (DIVA_exists && LOCATOR_exists ) {
	plot "<(sed -n '10,17p' ./HTAP_results/latest/results_DIVA/readIO.txt)" using ($0 + (BOX_WIDTH*3/2)):($2/1073741824):(BOX_WIDTH):(0) with vectors arrowstyle 3 notitle, \
		 "<(join ./HTAP_results/latest/results_DIVA/readIO.txt ./HTAP_results/latest/results_LOCATOR/readIO.txt | sed -n '10,17p')" using ($0 + (BOX_WIDTH*5/2)):($6/1073741824):(0):(($2 - $6)/1073741824) with vectors arrowstyle 2 notitle, \
		 "<(join ./HTAP_results/latest/results_DIVA/readIO.txt ./HTAP_results/latest/results_LOCATOR/readIO.txt | sed -n '10,17p')" using ($0 + (BOX_WIDTH*5/2) + 0.04):(10 ** ((log10($2/1073741824) + log10($6/1073741824)) / 2)):(sprintf("%.0fx", $2 / $6)) with labels boxed center rotate by 90 font "Calibri, 20" notitle
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
plot "" using (0):(0) with boxes fs pattern 7 transparent lt rgb 'black' title "          "

pos = pos + 3
set key at pos, 4.5
plot "" using (0):(0) with boxes fs solid lt rgb DUCKDB title "DuckDB (Parquet)"
plot "" using (0):(0) with boxes fs pattern 7 transparent lt rgb 'black' title "                "

pos = pos + 3.1
set key at pos, 4.5
plot "" using (0):(0) with boxes fs solid lt rgb CITUS title "PostgreSQL(PG) w/ Citus"
plot "" using (0):(0) with boxes fs pattern 7 transparent lt rgb 'black' title "                       "

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