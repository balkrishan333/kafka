package learn.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {

    public static void main(String[] args) {
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k,jsonTweet) -> extractUserFollowersFromTweet(jsonTweet) > 10000
                        //filter for tweets which has over 10,000 followers
        );
        filteredStream.to("important_tweets");

        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        //start streams application
        kafkaStreams.start();
    }

    private static int extractUserFollowersFromTweet(String jsonString) {
        try {
            return JsonParser.parseString(jsonString).getAsJsonObject().get("user").
                    getAsJsonObject().get("followers_count").getAsInt();
        } catch (Exception e) {
            return 0;
        }
    }
}
