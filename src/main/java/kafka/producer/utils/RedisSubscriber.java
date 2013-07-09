package kafka.producer.utils;

import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPubSub;

public class RedisSubscriber extends JedisPubSub {

    private static Logger LOG = Logger.getLogger(RedisSubscriber.class);

    @Override
    public void onMessage(String channel, String message) {
        LOG.info("Message received. Channel: " + channel + ", Msg: " + message);
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {

    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {

    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {

    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {

    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {

    }
}
