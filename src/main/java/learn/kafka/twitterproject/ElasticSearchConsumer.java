package learn.kafka.twitterproject;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        RestHighLevelClient client = createClient();

//        String jsonString = "{\n" +
//                "  \"name\": \"Balkrishan\",\n" +
//                "\"course\": \"kafka\"\n" +
//                "}";
//        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);
//        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//
//        String id = indexResponse.getId();
//        logger.info(id);

        KafkaConsumer<String, String> consumer = createConsumer();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("Received " + records.count() + " records.");
            for (ConsumerRecord<String, String> record : records) {
                String jsonString = record.value();

                //pass id in index request to make out consumer idempotent. passing id will make sure elastic search
                //will not insert same tweet twice

                //2 strategies for id
                //strategy 1 - applicable to all messages, if you can't find application specific id
                //String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                String id = extractIdFromTweet(jsonString);

                IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(jsonString, XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);

                String elasticSearchId = indexResponse.getId();
                logger.info(elasticSearchId);
                Thread.sleep(1000);
            }
            consumer.commitSync();
            logger.info("Offsets committed.");
            Thread.sleep(5000);
        }
//        client.close();
    }

    private static String extractIdFromTweet(String jsonString) {
        return JsonParser.parseString(jsonString).getAsJsonObject().get("id_str").getAsString();
    }

    public static KafkaConsumer<String, String> createConsumer() {
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "kafka-demo-es";
        String topic = "twitter_tweets";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit of offset
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5"); //disable auto commit of offset

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static RestHighLevelClient createClient() {
        //https://eoe3fhq8cn:b8izk9cukh@kafka-twitter-poc-2834400809.ap-southeast-2.bonsaisearch.net:443
        String host = "kafka-twitter-poc-2834400809.ap-southeast-2.bonsaisearch.net";
        String userName = "eoe3fhq8cn";
        String password = "b8izk9cukh";

        //don't do if you run a local ES
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, 443, "https"))
                                        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                                            @Override
                                            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                            }
                                        });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }
}
