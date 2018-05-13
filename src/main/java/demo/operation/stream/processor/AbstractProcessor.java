package demo.operation.stream.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Objects;

public class AbstractProcessor<K, V> implements Processor<K, V> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public void process(K key, V value) {

    }

    @Override
    public final void punctuate(long timestamp) {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }

    protected final ProcessorContext context() {
        return this.context;
    }
}
