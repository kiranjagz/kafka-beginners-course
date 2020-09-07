package kafka.twitter.tutorial;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import kafka.tutorial1.ConsumerDemoGroups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName());

    String consumerKey = "";
    String consumerSecret = "";
    String token = "";
    String secret = "";

    public TwitterProducer(){

    }

    public static void main(String[] args) {
       new TwitterProducer().run();
    }

    public void run(){
        logger.info("Application started");

        /** Not the best of code, but just needed to read from a file and set api settings, else its stored in code. */
        BufferedReader reader;
        {
            try {
                reader = new BufferedReader(new FileReader("c:\\twitter-api.txt"));

                StringBuilder sb = new StringBuilder();
                String line = reader.readLine();

                while (line != null){
                    sb.append(line);
                    sb.append(System.lineSeparator());
                    line = reader.readLine();

                    if (line == null){
                        break;
                    }

                    int index = line.indexOf(':');

                    if (line.contains("APIKEY")){
                        consumerKey = line.substring(index + 1);
                    }else if (line.contains("SECRET")){
                        consumerSecret = line.substring(index + 1);
                    }else if (line.contains("ACCESST")){
                        token = line.substring(index + 1);
                    }else if (line.contains("TOKEN")) {
                        secret = line.substring(index + 1);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10000);

        // create a client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create kafka producer

        // loop to send tweets to kafka

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try{
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            }catch (InterruptedException e){
                e.printStackTrace();
                client.stop();
            }

            if (msg != null){
                logger.info(msg);
            }
        }

        logger.info("Application stopped");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.
        return hosebirdClient;
    }
}
