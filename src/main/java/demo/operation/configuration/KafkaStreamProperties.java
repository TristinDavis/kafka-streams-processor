package demo.operation.configuration;

import lombok.Data;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@Validated
@ConfigurationProperties(prefix = "kafka.stream")
public class KafkaStreamProperties {

    private final Map<String, String> properties = new HashMap<>();
    private final Topic topic = new Topic();
    private final Producer producer = new Producer();
    private List<String> bootstrapServers;
    private String clientId;
    private String applicationId;
    private String processingGuarantee;
    private Class<?> keySerializer = Serdes.String().getClass();
    private Class<?> valueSerializer = Serdes.String().getClass();
    private Long commitIntervalMs;
    private Integer numberOfThreads;
    private Integer numberOfStandbyReplicas;

    public List<String> getSourceTopic() {
        return topic.getSourceTopic();
    }

    public String getSinkTopic() {
        return topic.getSinkTopic();
    }

    public StreamsConfig buildProperties() {
        Map<String, Object> properties = new HashMap<>();
        if (this.bootstrapServers != null) {
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        }
        if (this.clientId != null) {
            properties.put(StreamsConfig.CLIENT_ID_CONFIG, clientId);
        }
        if (this.applicationId != null) {
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        }
        if (this.processingGuarantee != null) {
            properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee);
        }
        if (this.keySerializer != null) {
            properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerializer);
        }
        if (this.valueSerializer != null) {
            properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerializer);
        }
        if (this.commitIntervalMs != null) {
            properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        }
        if (this.numberOfThreads != null) {
            properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, numberOfThreads);
        }
        if (this.numberOfStandbyReplicas != null) {
            properties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numberOfStandbyReplicas);
        }
        if (this.producer.acks != null) {
            properties.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), this.producer.acks);
        }
        properties.putAll(this.properties);
        return new StreamsConfig(properties);
    }

    @Data
    public static class Producer {
        private String acks = "all"; // change default value 1 -> all
    }

    @Data
    public static class Topic {
        private List<String> sourceTopic;

        private String sinkTopic;
    }
}
