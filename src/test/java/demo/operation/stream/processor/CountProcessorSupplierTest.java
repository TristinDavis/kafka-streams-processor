package demo.operation.stream.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CountProcessorSupplierTest {

    private ProcessorContext context;

    private Processor<String, String> processor;

    private KeyValueStore<String, Long> store;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        CountProcessorSupplier<String, String> supplier = new CountProcessorSupplier<>("test");

        store = mock(KeyValueStore.class);
        context = mock(ProcessorContext.class);
        given(context.getStateStore("test")).willReturn(store);

        processor = supplier.get();
        processor.init(context);
    }

    @Test
    public void process_이전데이터가없을경우() throws Exception {
        given(store.get("test")).willReturn(null);

        processor.process("test", "test");

        verify(store).put("test", 1L);
        verify(context).forward("test", 1L);
    }

    @Test
    public void process_이전데이터가있을경우() throws Exception {
        given(store.get("test")).willReturn(10L);

        processor.process("test", "test");

        verify(store).put("test", 11L);
        verify(context).forward("test", 11L);
    }
}