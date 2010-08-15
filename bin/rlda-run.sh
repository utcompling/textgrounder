#!/bin/sh
. textgrounder-env
$JAVA_CMD opennlp.bayesian.apps.TrainRegionModel $@
