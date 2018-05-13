package demo.operation.stream.support;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

public interface ManageableStream {

    Topology getTopology();

    KafkaStreams getKafkaStreams();

    boolean isRunning();
}
