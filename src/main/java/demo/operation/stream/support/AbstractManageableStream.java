package demo.operation.stream.support;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import java.util.Objects;

public abstract class AbstractManageableStream implements ManageableStream, InitializingBean, DisposableBean {

    protected KafkaStreams kafkaStreams;

    protected StreamsConfig streamsConfig;

    protected Topology topology;

    protected volatile boolean running;

    protected abstract StreamsConfig buildStreamConfig();

    protected abstract Topology buildTopology();

    @Override
    public Topology getTopology() {
        return this.topology;
    }

    @Override
    public KafkaStreams getKafkaStreams() {
        return this.kafkaStreams;
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public void destroy() throws Exception {
        this.kafkaStreams.close();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.streamsConfig = Objects.requireNonNull(buildStreamConfig());
        this.topology = Objects.requireNonNull(buildTopology());

        this.kafkaStreams = new KafkaStreams(topology, streamsConfig);
        this.kafkaStreams.setStateListener(((newState, oldState) -> running = newState == KafkaStreams.State.RUNNING));
        this.kafkaStreams.cleanUp();
        this.kafkaStreams.start();
    }
}
