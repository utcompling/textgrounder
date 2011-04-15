#!/bin/sh

for x in ${1+"$@"}; do
  echo $x
  echo ""
  args="`grep '^Arguments:' $x | perl -pe 's/.*-([0-9]+)-docthresh.*degrees-per-region (.*?) .*/threshold: $1, grid-size: $2/'`"
  sed -n '/^Final results/,/^Ending final results/p' $x | egrep '(^Final results|true error)' | perl -ne "print '$args', ' ', \$_"
done
