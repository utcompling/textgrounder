#!/bin/sh

# Extract results from a number of runs.

# Given output from a run-* front end, extract the mean and median errors
# from each specified file, compute the avg mean/median error, and output
# a line giving the errors along with relevant parameters for that particular
# run.

for x in ${1+"$@"}; do
  echo $x
  echo ""
  thresh="`grep '^Arguments:' $x | perl -pe 's/.*-([0-9]+)-docthresh.*/$1/'`"
  dpc="`grep '^Arguments:' $x | perl -pe 's/.*degrees-per-cell ([^ ]*).*/$1/'`"
  evalset="`grep '^Arguments:' $x | perl -pe 's/.*eval-set ([^ ]*).*/$1/'`"
  strategy="`grep '^Arguments:' $x | perl -pe 's/.*strategy ([^ ]*).*/$1/'`"
  args="evalset: $evalset, thresh: $thresh, grid: $dpc, strategy: $strategy"
  sed -n '/^Final results/,/^Ending final results/p' $x | egrep '(^Final results|true error)' | perl -ne "print '$args', '  ', \$_"
  mean="`sed -n '/^Final results/,/^Ending final results/p' $x | egrep 'Mean true error' | perl -pe 's/.*distance = (.*?) .*/$1/'`"
  median="`sed -n '/^Final results/,/^Ending final results/p' $x | egrep 'Median true error' | perl -pe 's/.*distance = (.*?) .*/$1/'`"
  #echo "Mean: $mean"
  #echo "Median: $median"
  if [ -n "$mean" -a -n "$median" ]; then
    avg=`echo "5k $mean $median + 2/p" | dc`
    echo "$args   Avg-mean-median true error distance = $avg miles"
  fi
done
