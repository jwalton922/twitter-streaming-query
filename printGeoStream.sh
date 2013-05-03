#!/bin/sh

# twitter4j-core, twitter4j-stream, etc.

# java -cp ./target/streaming_geo_tweets-0.1-jar-with-dependencies.jar kafka.producer.PrintGeoStream

#for jar in /home/ccfreed/twitter4j/dev_area/streaming_geo_tweets/lib/*.jar;do
# export CLASSPATH=$CLASSPATH:$jar
#done

for jar in target/*.jar;do
 export CLASSPATH=$CLASSPATH:$jar
done

#. ./setEnv.sh
export MEM_ARGS="-Xms30m -Xmx30m"

echo "CLASSPATH = $CLASSPATH"

RUN_CMD="$JAVA_HOME/bin/java $MEM_ARGS -cp $CLASSPATH kafka.producer.PrintGeoStream"
echo $RUN_CMD ${1+"$@"}
exec $RUN_CMD ${1+"$@"}

