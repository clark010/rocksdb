#!/usr/bin/env bash
export USE_HDFS=1
export JAVA_HOME=/opt/taobao/java

## make sure the hadoop compile with the -fPIC
export HADOOP_HOME=/home/hadoop/hadoop_hbase/hadoop-current
export LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$JAVA_HOME/jre/lib/amd64:$HADOOP_HOME/lib/native

export CLASSPATH=$CLASSPATH:`$HADOOP_HOME/bin/hadoop classpath --glob`
#for f in `find /usr/lib/hadoop-hdfs | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
#for f in `find /usr/lib/hadoop | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
#for f in `find /usr/lib/hadoop/client | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
