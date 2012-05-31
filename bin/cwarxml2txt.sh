#!/bin/bash

indir=${1%/}
outdir=${2%/}

for f in $indir/*.xml
do
  filename=$(basename $f)
  filename=${filename%.*}
  grep '<milestone unit="sentence"' $f | sed 's/<[^<>]*>//g' > $outdir/$filename.txt
done
