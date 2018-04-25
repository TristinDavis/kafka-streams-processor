package demo.operation.stream;

import demo.operation.configuration.KafkaStreamProperties;
import demo.operation.stream.processor.CountProcessor;
import demo.operation.stream.processor.SplitProcessor;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

import static demo.operation.stream.processor.CountProcessor.WORD_COUNT_STORE;

@Component
public class WordCountStream implements DisposableBean, InitializingBean {

    private KafkaStreams kafkaStreams;

    @Autowired
    public WordCountStream(KafkaStreamProperties kafkaStreamProperties) {
        this.kafkaStreams = new KafkaStreams(buildTopology(kafkaStreamProperties), kafkaStreamProperties.buildProperties());
    }

    private Topology buildTopology(KafkaStreamProperties kafkaStreamProperties) {
        Topology builder = new Topology();

        builder.addSource("source", kafkaStreamProperties.getSourceTopic().toArray(new String[]{}))
               .addProcessor("split", SplitProcessor::new, "source")
               .addProcessor("count", CountProcessor::new, "split")
               .addStateStore(countStoreBuilder(), "count")
               .addSink("sink", kafkaStreamProperties.getSinkTopic(),
                        new StringSerializer(), new LongSerializer(), "count");

        return builder;
    }

    private StoreBuilder countStoreBuilder() {
        return Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(WORD_COUNT_STORE),
                                           Serdes.String(),
                                           Serdes.Long());
    }

    @Override
    public void destroy() throws Exception {
        this.kafkaStreams.close();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Objects.requireNonNull(this.kafkaStreams);
        this.kafkaStreams.cleanUp();
        this.kafkaStreams.start();
    }

}
