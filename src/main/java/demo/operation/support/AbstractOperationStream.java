package demo.operation.support;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Objects;

public abstract class AbstractOperationStream implements OperationStream, DisposableBean, InitializingBean {
    protected String kafkaStreamsName;

    protected KafkaStreams kafkaStreams;

    protected Topology topology;

    @Override
    public String getKafkaStreamsName() {
        return kafkaStreamsName;
    }

    @Override
    public KafkaStreams getKafkaStreams() {
        return kafkaStreams;
    }

    @Override
    public Topology getTopology() {
        return topology;
    }

    @Override
    public void destroy() throws Exception {
        this.kafkaStreams.close();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Objects.requireNonNull(this.kafkaStreams);
        Objects.requireNonNull(this.kafkaStreamsName);
        Objects.requireNonNull(this.topology);
        this.kafkaStreams.cleanUp();
        this.kafkaStreams.start();
    }

}
