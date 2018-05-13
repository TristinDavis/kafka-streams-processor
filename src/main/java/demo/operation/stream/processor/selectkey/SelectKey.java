package demo.operation.stream.processor.selectkey;

import demo.operation.stream.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.function.BiFunction;

public class SelectKey<K1, V, K2> implements ProcessorSupplier<K1, V> {

    private final BiFunction<? super K1, ? super V, K2> function;

    public SelectKey(BiFunction<? super K1, ? super V, K2> function) {
        this.function = function;
    }

    @Override
    public Processor<K1, V> get() {
        return new SelectKeyProcessor();
    }

    private class SelectKeyProcessor extends AbstractProcessor<K1, V> {

        @Override
        public void process(K1 key, V value) {
            K2 newKey = function.apply(key, value);
            context().forward(newKey, value);
        }
    }
}
