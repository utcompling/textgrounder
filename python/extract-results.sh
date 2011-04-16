#!/bin/sh

tgp="$TEXTGROUNDER_DIR/python"
$tgp/extract-raw-results.sh run* | grep 'Mean' | $tgp/sort-by-distance.sh > results-sorted-by-mean-error-distance.txt
$tgp/extract-raw-results.sh run* | grep 'Median' | $tgp/sort-by-distance.sh > results-sorted-by-median-error-distance.txt
$tgp/extract-raw-results.sh run* | grep 'Avg-mean-median' | $tgp/sort-by-distance.sh > results-sorted-by-avg-mean-median-error-distance.txt
