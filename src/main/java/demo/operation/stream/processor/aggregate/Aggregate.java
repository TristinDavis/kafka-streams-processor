package demo.operation.stream.processor.aggregate;

import demo.operation.stream.processor.AbstractProcessor;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.function.Supplier;

public class Aggregate<K, V, T> implements ProcessorSupplier<K, V> {

    private final String storeName;

    private final Supplier<T> initializer;

    private final Aggregator<? super K, ? super V, T> aggregator;

    public Aggregate(String storeName, Supplier<T> initializer, Aggregator<? super K, ? super V, T> aggregator) {
        this.storeName = storeName;
        this.initializer = initializer;
        this.aggregator = aggregator;
    }

    @Override
    public Processor<K, V> get() {
        return new AggregatorProcessor();
    }

    private class AggregatorProcessor extends AbstractProcessor<K, V> {

        private KeyValueStore<K, T> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<K, T>) context.getStateStore(storeName);
        }

        @Override
        public void process(K key, V value) {
            if (key == null) {
                return;
            }

            T oldAgg = store.get(key);

            if (oldAgg == null) {
                oldAgg = initializer.get();
            }

            T newAgg = aggregator.apply(key, value, oldAgg);

            store.put(key, newAgg);
            context().forward(key, newAgg);
        }
    }
}
