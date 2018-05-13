package demo.operation.stream.processor.aggregate;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AggregateTest {

    private Aggregate<Integer, Integer, Integer> aggregator;

    private ProcessorContext context;

    private Processor<Integer, Integer> processor;

    private KeyValueStore<Integer, Integer> store;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        aggregator = new Aggregate<>("test", () -> 0, (key, value, oldValue) -> value + oldValue);

        context = mock(ProcessorContext.class);
        store = mock(KeyValueStore.class);

        given(context.getStateStore("test")).willReturn(store);

        processor = aggregator.get();
        processor.init(context);
    }

    @Test
    public void process_이전값이없는경우() throws Exception {
        given(store.get(0)).willReturn(null);

        processor.process(0, 10);

        verify(store).put(0, 10);
        verify(context).forward(0, 10);
    }

    @Test
    public void process_이전값이있는경우() throws Exception {
        given(store.get(0)).willReturn(5);

        processor.process(0, 10);

        verify(store).put(0, 15);
        verify(context).forward(0, 15);
    }

    @Test
    public void process_키가널인경우() throws Exception {
        processor.process(null, 10);

        verify(store, never()).put(anyInt(), anyInt());
        verify(context, never()).forward(anyInt(), anyInt());
    }

}