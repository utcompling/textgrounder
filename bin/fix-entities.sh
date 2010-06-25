#!/bin/sh

# Replace symbolic entity references with numeric ones --
# Google Earth (at least on the Mac) chokes on many or all of them
perl -i -p -e 's/&dollar;/\&#36;/g;' -e 's/&equo;/\&#39;/g;' \
           -e 's/&dquo;/\&#34;/g;' -e 's/&dash;/\&#45;/g;' \
           -e 's/&lsqb;/\&#91;/g;' -e 's/&rsqb;/\&#93;/g;' \
           -e 's/&hellip;/\&#8230;/g;' \
           -e 's/&amp;/\&#38;/g;' -e 's/&times;/\&#42;/g;' ${1+"$@"}
