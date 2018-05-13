package demo.operation.stream.processor;

import demo.operation.stream.processor.flatmap.FlatMapValues;
import org.yaml.snakeyaml.util.ArrayUtils;

public class SplitProcessorSupplier extends FlatMapValues<Long, String, String> {
    public SplitProcessorSupplier() {
        super(input -> ArrayUtils.toUnmodifiableList(input.split("\\W+")));
    }
}
