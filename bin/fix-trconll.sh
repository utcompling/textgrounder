#!/bin/bash

indir=${1%%/}
outdir=${2%%/}

# Fixes countries from CIA World Factbook that had -0 as longitude:
textgrounder run CIAWFBFixer $3 $indir $outdir

# Fixes states with swapped coordinates:
for fullpath in $outdir/*.xml
do
  filename=${fullpath##*/}
  sed -i -e's/\(^.*US_STATE.*lat=\"\)\([^"]*\)\(\".*long=\"\)\([^"]*\)\(\".*$\)/\1\4\3\2\5/' $outdir/$filename
done
