package demo.operation.stream.processor;

import demo.operation.stream.processor.flatmap.FlatMapValues;
import org.yaml.snakeyaml.util.ArrayUtils;

public class SplitProcessor extends FlatMapValues<Long, String, String> {
    public SplitProcessor() {
        super(input -> ArrayUtils.toUnmodifiableList(input.split("\\W+")));
    }
}
