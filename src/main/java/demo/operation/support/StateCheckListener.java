package demo.operation.support;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;

import java.util.function.Consumer;

@Slf4j
public class StateCheckListener implements KafkaStreams.StateListener {
    private final String kafkaStreamName;
    private Consumer<Boolean> stateCallback;

    public StateCheckListener(String kafkaStreamName, Consumer<Boolean> stateCallback) {
        this.kafkaStreamName = kafkaStreamName;
        this.stateCallback = stateCallback;
    }

    @Override
    public void onChange(KafkaStreams.State newState, KafkaStreams.State oldState) {
        if (newState.isRunning()) {
            stateCallback.accept(true);
        } else {
            stateCallback.accept(false);
        }
        log.info("{} state is {}", this.kafkaStreamName, newState.toString());
    }
}
