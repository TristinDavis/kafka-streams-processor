package demo.operation.stream.wordcount;

import demo.operation.stream.processor.aggregate.Aggregate;

public class CountProcessorSupplier<K, V> extends Aggregate<K, V, Long> {

    public CountProcessorSupplier(String storeName) {
        super(storeName, () -> 0L, (key, value, oldValue) -> oldValue + 1);
    }

}
