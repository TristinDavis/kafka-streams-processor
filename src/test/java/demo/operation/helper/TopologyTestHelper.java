package demo.operation.helper;

import demo.operation.configuration.KafkaStreamProperties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collections;
import java.util.Properties;

public class TopologyTestHelper {

    public static final String SOURCE_TOPIC = "plain.text";

    public static final String SINK_TOPIC = "word.count";

    public static KafkaStreamProperties kafkaStreamProperties() {
        KafkaStreamProperties properties = new KafkaStreamProperties();

        properties.setApplicationId("kafka-stream-demo-test");
        properties.setBootstrapServers(Collections.singletonList("localhost:9092"));
        properties.setProcessingGuarantee("exactly_once");
        properties.setKeySerializer(Serdes.LongSerde.class);
        properties.setValueSerializer(Serdes.StringSerde.class);
        properties.getTopic().setSourceTopic(Collections.singletonList("plain.text"));
        properties.getTopic().setSinkTopic("word.count");

        return properties;
    }

    public static Properties streamProperties() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return config;
    }
}
