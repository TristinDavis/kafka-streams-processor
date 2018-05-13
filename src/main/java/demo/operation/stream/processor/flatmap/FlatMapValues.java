package demo.operation.stream.processor.flatmap;

import demo.operation.stream.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.function.Function;

public class FlatMapValues<K, V, V1> implements ProcessorSupplier<K, V> {

    private final Function<? super V, ? extends Iterable<? extends V1>> mapper;

    public FlatMapValues(Function<? super V, ? extends Iterable<? extends V1>> mapper) {
        this.mapper = mapper;
    }

    @Override
    public Processor<K, V> get() {
        return new FlatMapValuesProcessor();
    }

    private class FlatMapValuesProcessor extends AbstractProcessor<K, V> {

        @Override
        public void process(K key, V value) {
            final Iterable<? extends V1> newValues = mapper.apply(value);

            for (V1 v : newValues) {
                context().forward(key, v);
            }
        }
    }

}
