package kafka.producer.utils;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import twitter4j.UserMentionEntity;

import java.util.LinkedHashMap;
import java.util.Map;

public class TweetInfo {

    //private final String[] tupleHeader = {"user_id","user_created","user_screen","user_desc",
    //        "user_lang","tweet_id","tweet_created","tweet_text","place_name","place_country",
    //        "place_cc","lat","lon","mentions","hashtags"};

    private long user_id;
    private String user_created;
    private String user_screen;
    private String user_desc;
    private String user_lang;
    private String user_url;
    private String user_profile_image_url;  // 'avatar' url
    private long tweet_id;
    private String tweet_created;
    private String tweet_text;
    private String place_name;
    private String place_country;
    private String place_cc;
    private double lat;
    private double lon;
    private String mentions;
    private String hashtags;

    private Map<String, String> tupleMap; // = new LinkedHashMap();

    // Think you have to have this for Gson to serialize tweet objects
    public TweetInfo() {
        user_id = 0;
        user_created = null;
        user_screen = null;
        user_desc = null;
        user_lang = null;
        user_url = null;
        user_profile_image_url = null;
        tweet_id = 0;
        tweet_created = null;
        tweet_text = null;
        place_name = null;
        place_country = null;
        place_cc = null;
        lat = 0;
        lon = 0;
        mentions = null;
        hashtags = null;

        tupleMap = new LinkedHashMap<String, String>();
    }

    /*
     * populate TweetInfo from a twitter4j Status (a fully populated tweet)
     * this is somewhat ugly, but did not want to use reflection for field grabbing/populating, as this can be slow
     */
    public void populate(Status status) {
        // Note: converting everything to String that gets tuple-ized - Tuple.getIntByField() does not seem to be working
        //tupleMap = new LinkedHashMap<String, String>();

        this.user_id = status.getUser().getId(); // long - user_id
        tupleMap.put("user_id", Long.toString(user_id));

        this.user_created = status.getUser().getCreatedAt().toString(); // String - user_created
        tupleMap.put("user_created", user_created);

        this.user_screen = status.getUser().getScreenName(); // String - user_screen
        tupleMap.put("user_screen", user_screen);

        this.user_desc = status.getUser().getDescription(); // String - user_desc
        tupleMap.put("user_desc", user_desc);

        this.user_lang = status.getUser().getLang(); // String - user_lang
        tupleMap.put("user_lang", user_lang);

        this.user_url = status.getUser().getURL();
        tupleMap.put("user_url", user_url);

        this.user_profile_image_url = status.getUser().getBiggerProfileImageURL();
        tupleMap.put("user_profile_image_url", user_profile_image_url);

        this.tweet_id = status.getId();  // long = tweet_id
        tupleMap.put("tweet_id", Long.toString(tweet_id));

        this.tweet_created = status.getCreatedAt().toString(); // String - tweet_created
        tupleMap.put("tweet_created", tweet_created);

        this.tweet_text = status.getText(); // String - tweet_text (hopefully not the whole blob!)
        tupleMap.put("tweet_text", tweet_text);

        if (status.getPlace() != null) {
            this.place_name = status.getPlace().getFullName();
            tupleMap.put("place_name", place_name);

            this.place_country = status.getPlace().getCountry(); // String - place_country
            tupleMap.put("place_country", place_country);

            this.place_cc = status.getPlace().getCountryCode(); // String - place_cc
            tupleMap.put("place_cc", place_cc);

        } else {
            tupleMap.put("place_name", null);
            tupleMap.put("place_country", null);
            tupleMap.put("place_cc", null);
        }
        if (status.getGeoLocation() != null) {
            this.lat = status.getGeoLocation().getLatitude();
            tupleMap.put("lat", Double.toString(lat));
            this.lon = status.getGeoLocation().getLongitude();
            tupleMap.put("lon", Double.toString(lon));
        } else {
            tupleMap.put("lat", Double.toString(lat));
            tupleMap.put("lon", Double.toString(lon));
        }
        UserMentionEntity[] umes = status.getUserMentionEntities();
        String theUmes = null;
        if (umes != null && umes.length > 0) {
            theUmes = new String();
            for (int jj = 0; jj < umes.length; jj++) {
                umes[jj].getId(); // long - mention
                theUmes = theUmes.concat(Long.toString(umes[jj].getId()));
                if (jj < umes.length - 1) {
                    theUmes = theUmes.concat(",");  // String - mentions
                }
            }
        }
        this.mentions = theUmes;
        tupleMap.put("mentions", mentions);

        //MediaEntity me = new MediaEntity();
        HashtagEntity[] hEntities = status.getHashtagEntities();
        String theHashes = null;
        if (hEntities != null && hEntities.length > 0) {
            theHashes = new String();
            for (int jj = 0; jj < hEntities.length; jj++) {
                hEntities[jj].getText(); // String - hashtag without the #
                theHashes = theHashes.concat(hEntities[jj].getText());
                if (jj < hEntities.length - 1) {
                    theHashes = theHashes.concat(",");  // String - hashtags
                }
            }
        }
        this.hashtags = theHashes;
        tupleMap.put("hashtags", hashtags);


    }


    public long getUserId() {
        return user_id;
    }

    public String getUserCreated() {
        return user_created;
    }

    public String getUserScreen() {
        return user_screen;
    }

    public String getUserDesc() {
        return user_desc;
    }

    public String getUserLang() {
        return user_lang;
    }

    public String getURL() {
        return user_url;
    }

    public String getUserProfileImageURL() {
        return user_profile_image_url;
    }

    public long getTweetId() {
        return tweet_id;
    }

    public String getTweetCreated() {
        return tweet_created;
    }

    public String getTweetText() {
        return tweet_text;
    }

    public String getPlaceName() {
        return place_name;
    }

    public String getPlaceCountry() {
        return place_country;
    }

    public String getPlaceCC() {
        return place_cc;
    }

    public double getLat() {
        return lat;
    }

    public double getLon() {
        return lon;
    }

    public String getMentions() {
        return mentions;
    }

    public String getHashtags() {
        return hashtags;
    }

    public Map<String, String> getTupleMap() {
        return tupleMap;
    }


}
