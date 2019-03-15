#!/usr/bin/env bash

# job env
USER_HOME="/home/csun1"
PJ_HOME="$USER_HOME/giraph"
LIB_HOME="$PJ_HOME/libs"

USER_JAR="$LIB_HOME/giraph-algo-1.0-SNAPSHOT.jar"
MAIN_CLASS="com.sunday.giraph.algo.sp.SSSPJobRunner"

# small graph
WORKER_NUM="1"
GIRAPH_INPUT="/path/to/input"
GIRAPH_OUTPUT="/path/to/output"
SOURCE_VERTEX_ID="1"

# hadoop env
QUEUE="spark"
MAPPER_MEM="20480"
MAPPER_JAVA_OPTS="-Xmx16380m"

# remove output directory
hadoop fs -rm -r -skipTrash $GIRAPH_OUTPUT

# run jar
hadoop jar $USER_JAR $MAIN_CLASS \
-w $WORKER_NUM \
-vip $GIRAPH_INPUT \
-op $GIRAPH_OUTPUT \
-ca single.source.shortest.path.source.id=$SOURCE_VERTEX_ID \
-ca mapreduce.job.queuename=$QUEUE \
-ca mapreduce.map.memory.mb=$MAPPER_MEM \
-ca mapreduce.map.java.opts=$MAPPER_JAVA_OPTS