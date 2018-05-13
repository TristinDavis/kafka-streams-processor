package demo.operation.stream;

import demo.operation.configuration.KafkaStreamProperties;
import demo.operation.stream.processor.CountProcessorSupplier;
import demo.operation.stream.processor.SplitProcessorSupplier;
import demo.operation.stream.processor.selectkey.SelectKey;
import demo.operation.stream.support.AbstractManageableStream;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class WordCountStream extends AbstractManageableStream {

    static final String WORD_COUNT_STORE = "counts";

    private KafkaStreamProperties kafkaStreamProperties;

    @Autowired
    public WordCountStream(KafkaStreamProperties kafkaStreamProperties) {
        this.kafkaStreamProperties = Objects.requireNonNull(kafkaStreamProperties);
    }

    @Override
    protected StreamsConfig buildStreamConfig() {
        return kafkaStreamProperties.buildProperties();
    }

    @Override
    protected Topology buildTopology() {
        Topology builder = new Topology();

        builder.addSource("source", kafkaStreamProperties.getSourceTopic().toArray(new String[]{}))
               .addProcessor("split", new SplitProcessorSupplier(), "source")
               .addProcessor("selectKey", new SelectKey<Long, String, String>((k, v) -> v), "split")
               .addProcessor("count", new CountProcessorSupplier<String, String>(WORD_COUNT_STORE), "selectKey")
               .addStateStore(countStoreBuilder(), "count")
               .addSink("sink", kafkaStreamProperties.getSinkTopic(),
                        new StringSerializer(), new LongSerializer(), "count");

        return builder;
    }

    private StoreBuilder<KeyValueStore<String, Long>> countStoreBuilder() {
        return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(WORD_COUNT_STORE),
                                           Serdes.String(),
                                           Serdes.Long())
                     .withLoggingDisabled();
    }

}
