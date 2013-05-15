/*
 * Copyright 2007 Yusuke Yamamoto
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.producer;

//import twitter4j.*;
import twitter4j.FilterQuery;
import twitter4j.ResponseList;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;

import backtype.storm.utils.Utils;

import com.google.gson.Gson;
import com.google.gson.stream.MalformedJsonException;

import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import kafka.producer.utils.AlertUpdate;
import kafka.producer.utils.TweetInfo;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//Import log4j classes.

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
/**
 * This will:
 *   - Connect to twitter stream, and stream in filtered tweets
 *   - Accept tweet filter updates from Redis topic
 *   - Push every tweet to a Kafka topic
 *   - Push every tweet to a Redis channel
 *   
 * Stats:
 *   - With no Kafka publishing, and full geo enabled (e.g. world) - incoming
 *       message rate is a steady 50/s
 * 
 * Limits:
 *   - Follow 
 *       - can only follow public accounts e.g. not mrcy_intel
 *       - on multiples: e.g. @cnallen @freedos123
 *           - with geo on - first guy usually get those - second guy never!       
 *       - With full geo on - this is VERY spotty (e.g. you don't always get your tweets!)       
 *       - full geo plus name plus term - rarely get it! 
 *       - single name with multiple terms - full geo - flaky 
 *       - single name, multiple terms, no geo - good    
 *   
 *   - Terms:
 *       - Multiple terms works, but again it is a sample - so you may not get EVERY
 *           time someone tweets a term
 *       - No commas! e.g. @freedos123 chocolate, bitter, kobe will alert on terms:
 *           'chocolate,' 'bitter,' 'kobe'
 *           
 *   - Once you start having issues with multiple users/terms - changing order
 *       does not help, but putting in single user sometimes fixes it
 *   - Restarting the filter process, and resubmitting the alert criteria, often
 *       fixes the filtering not working
 *   - Sometimes re-setting filter with geo off fixes things
 *       
 *       
 *    
 *           
 *   
 *
 */
public final class FilterTwitterStream {
    
    static Logger LOG = Logger.getLogger(FilterTwitterStream.class);
    
    // Limits on twitter filter parms
    static final int MAX_USER_IDS = 400;
    static final int MAX_HASHTAGS = 200;
    static final int MAX_ROI_BOXES = 10;
    
    // does queue need to be any bigger than 100?
    static LinkedBlockingQueue<String> redisMessageQueue = new LinkedBlockingQueue<String>(100);
    static String redisAlertUpdateTopic = "alertUpdateChannel";
    
    static Twitter twitter = null;
    static TwitterStream twitterStream = null;
    
    static Gson gson = null;
    
    static String lastUpdateMessage = null;
    
    static class RedisListenerThread extends Thread {
        //LinkedBlockingQueue<String> queue;
        JedisPool pool;
        String topic;
        
        //public RedisListenerThread(LinkedBlockingQueue<String> queue, String channel) {
        public RedisListenerThread(JedisPool jPool, String channel) {
            //this.queue = queue;
            this.pool = jPool;
            this.topic = channel;
        }
        
        public void run() {
            
            JedisPubSub listener = new JedisPubSub() {

                @Override
                public void onMessage(String channel, String message) {
                    LOG.info("GOT A MESSAGE: " + message);
                    //queue.offer(message);
                    onUpdateFilterMessage(message);
                }

                @Override
                public void onPMessage(String pattern, String channel, String message) {
                    //queue.offer(message);
                }

                @Override
                public void onPSubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub                    
                }

                @Override
                public void onPUnsubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub                    
                }

                @Override
                public void onSubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub                    
                }

                @Override
                public void onUnsubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub                    
                }
            };
            
            Jedis subscriberJedis = pool.getResource();
            //Jedis subscriberJedis = new Jedis("localhost", 6379, 0);
            try {
                subscriberJedis.subscribe(listener, topic);
                //jedis.psubscribe(listener, pattern);
            } catch (Exception e) {
                LOG.error("Redis Alert Udate Subscribing failed.", e);
            } finally {
                pool.returnResource(subscriberJedis);
            }
        }
    }
    
    /**
     * Main entry of this application.
     *
     * @param args follow(comma separated user ids) track(comma separated filter terms)
     * @throws twitter4j.TwitterException
     */
    public static void main(String[] args) throws TwitterException {

        if (args.length != 3) {
            System.out.println("Usage: java twitter4j.examples.PrintFilterStream  <kafka host>  <kafka topic>  <redis host>");
            System.exit(-1);
        }
        
        final String kafkaZKHost = args[0];
        final String kafkaTopic = args[1];
        final String redisHost = args[2];
        
        
        JedisPoolConfig poolConfig = new JedisPoolConfig();        
        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, 6379, 0);
        

        BasicConfigurator.configure();
        gson = new Gson();
        
        StatusListener twitterListener = new StatusListener() {
            //Gson gson = new Gson();
            
            Properties kafkaProps = new Properties();
            //kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
            //kafkaProps.put("zk.connect", "localhost:2181");
            Producer<Integer, String> kafkaProducer = null;
            int tweetCount = 0;
            int interimCount = 0;
            long currentTime = 0;
            long lastTime = 0;
            
            //Jedis publisherJedis = new Jedis("localhost", 6379, 0);
            
            
            @Override
            public void onStatus(Status status) {
                if ( kafkaProducer == null ) {
                    kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
                    //kafkaProps.put("zk.connect", "localhost:2181");
                    kafkaProps.put("zk.connect", kafkaZKHost.concat(":2181"));
                    kafkaProducer = new Producer<Integer, String>(new ProducerConfig(kafkaProps));
                    
                }
                tweetCount++;
                interimCount++;
                if ( interimCount == 100 ) {
                    LOG.info(" total tweets received: " + tweetCount);
                    currentTime = System.currentTimeMillis()/1000;
                    LOG.info("incoming msg rate: " + 100/(currentTime - lastTime));
                    interimCount = 0;
                    lastTime = currentTime;
                }
                
                // System.out.println("@" + status.getUser().getScreenName() + " - "); // + status.getText());
                //if ( status.getPlace() != null ) {
                //    System.out.println("coordinates = " + status.getPlace().getGeometryCoordinates());
                //}
                
                TweetInfo tweetInfo = new TweetInfo();
                tweetInfo.populate(status);
                String message = gson.toJson(tweetInfo, TweetInfo.class);
                
                //kafkaProducer.send(new ProducerData<Integer, String>("live_tweets", message));
                kafkaProducer.send(new ProducerData<Integer, String>(kafkaTopic, message));
                //LOG.info("screen_name: " + tweetInfo.getUserScreen() + ": lat/lon: " + tweetInfo.getLat() + "/" + 
                //         tweetInfo.getLon() + "  place: " + tweetInfo.getPlaceName() + " - text: " + tweetInfo.getTweetText());
                //kafkaProducer.send(new ProducerData<Integer, String>("live_tweets_full_geo", message));
                
                // Note this spits JSON string to the channel, find out if we just want a subset of this instead!
                // user_screen, tweet_created, url(avatar), country, lat/lon, tweet_text
                //publisherJedis.publish("live_tweets_redis", message);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                //System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                //System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        /*
         * - use Redis to pass filter messages in
         
         * 
         * 
         */
        
        // start message listener
        RedisListenerThread redis = new RedisListenerThread(jedisPool, redisAlertUpdateTopic);
        redis.start();

        twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(twitterListener);
        //twitterStream.

        // filter() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
        //twitterStream.filter(new FilterQuery(0, followArray, trackArray));
        FilterQuery fQuery = new FilterQuery();
        double[][] smallBox = {{-1, -1},{1, 1}};
        //double[][] theWorld = {{-180, -90},{180, 90}};
        fQuery.locations(smallBox);
        twitterStream.filter(fQuery);
        
        // Now listen for stuff on the alert update channel
        /*
        while ( true ) {
            String ret = redisMessageQueue.poll();
            if(ret==null) {
                Utils.sleep(5000);
                LOG.info("nothing on the queue");
            } else {
                LOG.info("GOT alert update message of: " + ret);            
            }
        }
        */
        
    }
    
    private static long[] convertNamesToIds(String[] names) {
        long[] back = null;
        if ( names != null && names.length > 0 ) {
            back = new long[names.length];
            
            try {
                if ( twitter == null ) {
                    twitter = new TwitterFactory().getInstance();
                }
                ResponseList<User> users = twitter.lookupUsers(names);
                int jj = 0;
                for (User user : users) {                    
                     back[jj] = user.getId();
                     LOG.info("Successfully looked up @" + user.getScreenName() + " - Id: " + user.getId());                    
                }                
            } catch (TwitterException te) {
                te.printStackTrace();
                System.out.println("Failed to lookup users: " + te.getMessage());
            }
            
        }    
        return back;
    }
    
    /* - Read in filter JSON with parms for:
     *     - lat/lon boxj
     *     - screen names to follow
     *     - hashtags to track
     * - Convert screen names to userIds
     * - Kick off filter with new FilterQuery        
     */
    public static void onUpdateFilterMessage(String json) {
        // First check that this was an update to last update, then reset        
        LOG.info("received update message:" + json);
        
        if ( lastUpdateMessage == null || !lastUpdateMessage.equals(json) ) {        
            AlertUpdate update = null;                
            BufferedReader br = new BufferedReader(new StringReader(json));
            update = gson.fromJson(br, AlertUpdate.class);
            LOG.info("AFTER JSON parsing of alert msg");
            if ( update == null ) {
                LOG.info((" update is null!"));
            } else {
                                  
                long[] userIds = null;
                String[] hashtags = null;
                double[][] locations = null;
                //double[][] locations = null;
                if ( update.getUserNames() != null ) {
                    LOG.info("converting usernames ");
                    userIds = convertNamesToIds(update.getusernamesNoAtSigns());
                }
                if ( update.getHashtags() != null ) {
                    hashtags = update.getHashtags();
                }
                if ( update.getLatLonBoxes() != null ) {
                    locations = update.getLocations();
                }
                double[][] theWorld = {{-180, -90},{180, 90}};
                LOG.info("Setting FilterQuery with userIds:" + userIds + " - terms: " + hashtags +
                        " - location box: " + locations);
                
                
                FilterQuery filterQuery = new FilterQuery(0, userIds, hashtags, locations);
                   
                //FilterQuery filterQuery = new FilterQuery();
                //filterQuery.locations(theWorld);
                twitterStream.filter(filterQuery);
            }                
        
        } else {
            if ( lastUpdateMessage == null )
                LOG.info("lastUpdateMessage = null!");
            else
                LOG.info("REPEAT ALERT - skip it!");
        }        
      lastUpdateMessage = json;
    }
    
}
