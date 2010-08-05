#!/bin/sh
. textgrounder-env
$JAVA_CMD opennlp.rlda.apps.ConvertToRegionModelFormat $@
