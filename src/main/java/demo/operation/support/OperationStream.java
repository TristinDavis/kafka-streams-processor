package demo.operation.support;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

public interface OperationStream {
    String getKafkaStreamsName();

    KafkaStreams getKafkaStreams();

    Topology getTopology();
}
