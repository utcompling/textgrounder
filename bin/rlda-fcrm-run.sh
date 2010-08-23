#!/bin/sh
. textgrounder-env
$JAVA_CMD opennlp.textgrounder.bayesian.apps.TrainFullyConstrainedRegionModel $@
