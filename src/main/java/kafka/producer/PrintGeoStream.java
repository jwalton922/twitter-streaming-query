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

import com.google.gson.Gson;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.utils.TweetInfo;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import twitter4j.*;

import java.util.Properties;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//Import log4j classes.

/**
 * This will emit JSON formatted tweets to a Kafka topic - "live_tweets_full_geo".  No filtering
 * is done, only settings are the location box (set to the world).
 */
public final class PrintGeoStream {

    static Logger LOG = Logger.getLogger(PrintGeoStream.class);

    /**
     * Main entry of this application.
     *
     * @param args <kafka host> <kafka topic>
     * @throws twitter4j.TwitterException
     */
    public static void main(String[] args) throws TwitterException {

        if (args.length != 2) {
            System.out.println("Usage: java twitter4j.examples.PrintFilterStream  <kafka host>  <kafka topic>");
            System.exit(-1);
        }

        final String kafkaZKHost = args[0];
        final String kafkaTopic = args[1];

        BasicConfigurator.configure();

        StatusListener listener = new StatusListener() {

            Gson gson = new Gson();

            Properties kafkaProps = new Properties();
            //kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
            //kafkaProps.put("zk.connect", "localhost:2181");
            Producer<Integer, String> kafkaProducer = null;
            int tweetCount = 0;
            int interimCount = 0;


            @Override
            public void onStatus(Status status) {
                if (kafkaProducer == null) {
                    kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
                    //kafkaProps.put("zk.connect", "localhost:2181");
                    kafkaProps.put("zk.connect", kafkaZKHost.concat(":2181"));
                    kafkaProducer = new Producer<Integer, String>(new ProducerConfig(kafkaProps));

                }
                tweetCount++;
                interimCount++;
                if (interimCount == 100) {
                    LOG.info(" total tweets received: " + tweetCount);
                    interimCount = 0;
                }

                //System.out.println("@" + status.getUser().getScreenName() + " - "); // + status.getText());
                //if ( status.getPlace() != null ) {
                //    System.out.println("coordinates = " + status.getPlace().getGeometryCoordinates());
                //}

                TweetInfo tweetInfo = new TweetInfo();
                tweetInfo.populate(status);
                String message = gson.toJson(tweetInfo, TweetInfo.class);

                kafkaProducer.send(new ProducerData<Integer, String>(kafkaTopic, message));
                //kafkaProducer.send(new ProducerData<Integer, String>("live_tweets", message));
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                //System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }

            @Override
            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }

            @Override
            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        /*
         * - use Redis to pass filter messages in - or ZK?
         * - Read in filter JSON with parms for:
         *     - lat/lon boxj
         *     - screen names to follow
         *     - hashtags to track
         * - Convert screen names to userIds
         *     - Twitter twitter = new TwitterFactory().getInstance();
         *       ResponseList<User> users = twitter.lookupUsers(args[0].split(","));
         * - Kick off filter with new FilterQuery
         *     - periodically check, and if filter updated, restart the TwitterStream:
         *         .shutdown()
         *         new
         *         .addListener
         * 
         * 
         */

        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
        twitterStream.addListener(listener);
        /*
         * For CONUS only: 
         *  "lowerLat": "22.0",
         *  "lowerLon": "-127.0",
         *  "upperLat": "47.5",
         *  "upperLon": "-67.0"
         */
        FilterQuery fQuery = new FilterQuery();
        double[][] theWorld = {{-180, -90}, {180, 90}};
        fQuery.locations(theWorld);
        twitterStream.filter(fQuery);

    }

}
