package demo.operation.stream.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.Arrays;

public class SplitProcessor implements Processor<Long, String> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        // do nothing
        this.context = context;
    }

    @Override
    public void process(Long key, String value) {
        String[] values = value.split("\\W+");
        if (values.length == 0) {
            return;
        }
        Arrays.stream(values)
              .forEach(v -> context.forward(key, v));
    }

    @Override
    public void punctuate(long timestamp) {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }
}
