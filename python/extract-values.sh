#!/bin/sh

# Extract the results from a bunch of split data files

grep -h '^#.*to predicted' ${1+"$@"} | sed 's/^#//' | sort -n | uniq
