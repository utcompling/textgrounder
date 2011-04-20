#!/bin/sh

# TextGrounder-related
TG_DIR="$TEXTGROUNDER_DIR"
TG_LIB="$TG_DIR/target/assembly/work"
TG_CP="$TG_DIR/target/classes:$TG_LIB/trove.jar:$TG_LIB/commons-cli-1.2.jar:$TG_LIB/jdom-1.1.jar"

# Create the combined CLASSPATH
CP="$TG_CP"
JAVA="$JAVA_HOME/bin/java"

# Determine the heap-size flag
if [ -z "$JAVA_MEM_FLAG" ] 
then
    JAVA_MEM_FLAG=-Xmx2500m
fi

# Now prepares the Java command with the right settings
JAVA_CMD="$JAVA $JAVA_MEM_FLAG -classpath $CP"

$JAVA_CMD opennlp.textgrounder.bayesian.apps.TrainSphericalModel $@
