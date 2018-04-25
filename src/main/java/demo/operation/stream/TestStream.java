package demo.operation.stream;

import demo.operation.configuration.KafkaStreamProperties;
import demo.operation.support.AbstractOperationStream;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class TestStream extends AbstractOperationStream {

    @Autowired
    public TestStream(KafkaStreamProperties kafkaStreamProperties) {
        this.topology = buildTopology();
        this.kafkaStreams = new KafkaStreams(this.topology, kafkaStreamProperties.buildProperties());
    }

    private Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Long, String> source = builder.stream(Arrays.asList("test.a", "test.b", "test.c"));
        source.to("sink.topic");

        return builder.build();
    }
}
