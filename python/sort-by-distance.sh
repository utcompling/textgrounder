#!/bin/sh

# Assuming that input is from stdin, extract the distance, prepend it, and sort numerically
perl -pe 's/^(.* distance = )(.*? )(miles.*)/$2 $1$2$3/' | sort -n
