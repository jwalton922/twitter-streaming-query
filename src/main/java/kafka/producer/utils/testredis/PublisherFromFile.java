package kafka.producer.utils.testredis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import redis.clients.jedis.Jedis;

public class PublisherFromFile {
    private static final Logger LOG = Logger.getLogger(PublisherFromFile.class);
    
    private final Jedis publisherJedis;
 
    private final String channel;
 
    public PublisherFromFile(Jedis publisherJedis, String channel) {
        this.publisherJedis = publisherJedis;
        this.channel = channel;
    }
    
    /*
     * Converts contents of file to String.  Note file is deleted after completion!
     */
    private String fileToStringUsingStringBuilder(File inFile) throws IOException {
        StringBuilder  sb = null;
        BufferedReader reader = null;
        //File inFile = null;
        try {
            //inFile = new File(filename);
            //reader = new BufferedReader( new FileReader (filename));
            reader = new BufferedReader(new FileReader(inFile));
        
            String  line = null;
            sb = new StringBuilder();
            String  ls = System.getProperty("line.separator");
      
            while( ( line = reader.readLine() ) != null ) {
                //System.out.println("Processing line: " + line);
                sb.append( line );
                sb.append( ls );
            }
        
        } finally {
            if ( reader != null )
                reader.close();
            
            // Delete the file
            if ( inFile != null )
                inFile.delete();
        }  
        // To clean up "bla bla \"  bla bla"
        return sb.toString().replace("\\", "\\\\");   
        
    }
 
    public void start() {
        LOG.info("Type your message (quit for terminate)");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
 
            String dirname = "/home/ccfreed/twitter4j/dev_area/streaming_geo_tweets/UPDATE_IN";
            File folder = new File(dirname);
            File[] filesToRead;
            String message = null;
            
            try {
                LOG.info("Listening in " + dirname + " for incoming JSON update messages ...");
                while (true) {
                    filesToRead = folder.listFiles();
                    if ( filesToRead != null && filesToRead.length > 0 ) {
                        // Run thru these, pipe into a string, and send
                        for ( File file : filesToRead ) {
                            message = fileToStringUsingStringBuilder(file);
                            LOG.info("publishing JSON update message");
                            publisherJedis.publish(channel, message);
                        }
                        
                    }// else {
                    //    LOG.info("EMPTY - try again");
                    //}                    
                    
                    Thread.sleep(5000);
                }
                
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
 
        } catch (IOException e) {
            LOG.error("IO failure while reading input, e");
        }
    }
}
