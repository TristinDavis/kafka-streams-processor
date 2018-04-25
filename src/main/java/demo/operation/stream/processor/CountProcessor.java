package demo.operation.stream.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Optional;

@Slf4j
public class CountProcessor implements Processor<Long, String> {

    public static final String WORD_COUNT_STORE = "counts";

    private ProcessorContext context;

    private KeyValueStore<String, Long> counts;

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.counts = (KeyValueStore<String, Long>) context.getStateStore(WORD_COUNT_STORE);
    }

    @Override
    public void process(Long key, String word) {
        Long count = getCountAndIncrement(word);

        counts.put(word, count);
        log.info("word : {}, count : {}", word, count);
        context.forward(word, count);
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {

    }

    private long getCountAndIncrement(String word) {
        return Optional.ofNullable(counts.get(word))
                       .orElse(0L) + 1;
    }
}
