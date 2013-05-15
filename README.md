twitter-streaming
=================

twitter streaming to Kafka/Redis

Contains classes for streaming twitter data to Kafka or Redis.

PrintGeoStream - connects to twitter and recieves a sample stream of geo tweets - geo box set to entire world

To run it:

java -cp <__with_dependencies.jar> kafka.producer.PrintGeoStream  <kafka_zk_host> <kafka_topic>

Standard topic to send tweets to is "live_tweets"

TwitterFilterStream - configurable twitter stream - can configure with names to follow, terms to trend, and lat/lon boxes.  Note that this is also a SAMPLE, meaning you are not guaranteed to receive everything on a follow/trend!  Max rate is ~50 msgs/sec

This will emit all tweets to a kafka topic.  Filter changes come in via a redis topic "alertUpdateChannel" - from the alert_viewer.

To run it:

java -cp ... kafka.producer.FilterTwitterStream  <kafka_host>  <kafka_topic>  <redis_host>
