set term epscairo enhanced size 8in, 2.8in background rgb 'white'
set output "figure_11_a.eps"

$tmp << EOD
1 1
EOD

VANILLA = "#37517F"
DIVA = "#af7ac5"
LOCATOR = "#58d68d"

dir_exists(dir) = system("[ -d '".dir."' ] && echo '1' || echo '0'") + 0

VANILLA_exists = dir_exists("./HTAP_results/latest/results_VANILLA")
DIVA_exists = dir_exists("./HTAP_results/latest/results_DIVA")
LOCATOR_exists = dir_exists("./HTAP_results/latest/results_LOCATOR")

set multiplot

set lmargin at screen 0.11
set rmargin at screen 0.99

set style line 1 lc rgb VANILLA lt 4 dt 1 lw 5 pt 7 ps 1.5 pointinterval -10
set style line 2 lc rgb DIVA lt 4 dt 9 lw 5 pt 1 ps 1.5 pointinterval -10
set style line 4 lc rgb LOCATOR lt 4 dt 1 lw 5 pt 2 ps 1.5 pointinterval -10

# plot 1
set tmargin at screen 1. - 0.08
set bmargin at screen 1. - 0.525
set yrange[0:1.5]
set ylabel "OLTP\ntpm (x10^6)" font "Arial-Bold, 22" offset -3.5, 0
set ytics nomirror 0.5, 0.5, 1.0 font "Arial, 22" scale 0.0 offset 0.5, 0
set xrange[0.1:1200 - 0.1]
unset xtics

set style arrow 1 heads size screen 0.008,90 ls 2 lw 3 lc rgb "#000000"
set style arrow 2 head filled size 20, 20 ls 2 lw 3 lc rgb "#000000"
set style arrow 3 heads size screen 0.008,0 ls 7 lt 3 dt 2 lw 4 lc rgb "#444444"

set label sprintf('OLTP') at 260, 0.15 right font "Arial, 20"
set label sprintf('HTAP') at 340, 0.15 left font "Arial, 20"
plot "$tmp" using (300):(0):(0):(1.0) with vectors arrowstyle 3 notitle, \
	 "$tmp" using (300-27.5):(0.15):(55):(0) with vectors heads filled size screen 0.008,30 lw 4 lc black notitle

unset label
unset ytics
unset ylabel

if ( VANILLA_exists ) plot "./HTAP_results/latest/results_VANILLA/tpm.txt" using ($1):($2 / 1e6) with lines ls 1 notitle
if ( DIVA_exists ) plot "./HTAP_results/latest/results_DIVA/tpm.txt" using ($1):($2 / 1e6) with linespoint ls 2 notitle
if ( LOCATOR_exists ) plot "./HTAP_results/latest/results_LOCATOR/tpm.txt" using ($1):($2 / 1e6) with linespoint ls 4 notitle

set style line 1 dt 1 lw 3 lc rgb 'black'
set style fill solid 1.00 noborder

set key reverse Left samplen 3 font "Arial, 20"
pos = 0.0

pos = pos + 330
set key at pos, 1.75
plot "" using ($1):(100) with lines ls 1 title "PostgreSQL(PG)"

pos = pos + 390
set key at pos, 1.75
plot "" using ($1):(100) with linespoint ls 2 title "PG w/ DIVA"

pos = pos + 440
set key at pos, 1.75
plot "" using ($1):(100) with linespoint ls 4 title "PG w/ LOCATOR"

# unset key
unset ylabel

# plot 2
set tmargin at screen 1. - 0.525
set bmargin at screen 1. - 0.855
set style data histograms

set yrange[0:220]
set ytics nomirror 50, 50, 150 font "Arial, 22" scale 0.0 offset 0.5, 0
set ylabel "Space\n(GiB)" font "Arial-Bold, 22" offset -3.5, 0
set xtics 300, 300, 900 scale 0.0 offset 0, 0.2 font "Arial, 22"
set xlabel "Time (sec)" offset 0, 0.2 font "Arial, 22"

plot NaN notitle

unset grid
unset xtics
unset xlabel
unset ytics
unset ylabel

if ( LOCATOR_exists ) plot "./HTAP_results/latest/results_LOCATOR/dbsize.txt" using ($1):($2 / (1024.0 * 1024)) with filledcurves above x1 lc rgb LOCATOR notitle
if ( DIVA_exists ) plot "./HTAP_results/latest/results_DIVA/dbsize.txt" using ($1):($2 / (1024.0 * 1024)) with filledcurves above x1 lc rgb DIVA notitle
if ( VANILLA_exists ) plot "./HTAP_results/latest/results_VANILLA/dbsize.txt" using ($1):($2 / (1024.0 * 1024)) with filledcurves above x1 lc rgb VANILLA notitle

pos = 0.0

pos = pos + 330
set key at pos, 210
plot "" using ($1):(0) with boxes fs solid lt rgb VANILLA title "PostgreSQL(PG)"

pos = pos + 390
set key at pos, 210
plot "" using ($1):(0) with boxes fs solid lt rgb DIVA title "PG w/ DIVA"

pos = pos + 440
set key at pos, 210
plot "" using ($1):(0) with boxes fs solid lt rgb LOCATOR title "PG w/ LOCATOR"
