package kafka.producer.utils.testredis;

import kafka.producer.utils.RedisSubscriber;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class TestRedisPublisher {
    public static final String CHANNEL_NAME = "alertUpdateChannel";
    
    private static Logger logger = Logger.getLogger(TestRedisPublisher.class);
 
    public static void main(String[] args) throws Exception {
 
        JedisPoolConfig poolConfig = new JedisPoolConfig();
 
        JedisPool jedisPool = new JedisPool(poolConfig, "localhost", 6379, 0);
 
        //final Jedis subscriberJedis = jedisPool.getResource();
 
        final RedisSubscriber subscriber = new RedisSubscriber();
 
        new Thread(new Runnable() {
            @Override
            public void run() {
                Jedis subscriberJedis = new Jedis("localhost", 6379, 0);
                try {
                    logger.info("Subscribing to \"commonChannel\". This thread will be blocked.");
                    subscriberJedis.subscribe(subscriber, CHANNEL_NAME);
                    logger.info("Subscription ended.");
                    
                } catch (Exception e) {
                    logger.error("Subscribing failed.", e);
                }
            }
        }).start();
 
        Jedis publisherJedis = jedisPool.getResource();
 
        //new Publisher(publisherJedis, CHANNEL_NAME).start();
        new PublisherFromFile(publisherJedis, CHANNEL_NAME).start();
 
        subscriber.unsubscribe();
        //jedisPool.returnResource(subscriberJedis);
        jedisPool.returnResource(publisherJedis);
    }
}
