#!/bin/sh

for x in ${1+"$@"}; do
  echo $x
  echo ""
  sed -n '/^Final results/,/^Ending final results/p' $x | egrep '(^Final results|true error)'
done
