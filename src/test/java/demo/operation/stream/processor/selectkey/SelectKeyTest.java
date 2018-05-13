package demo.operation.stream.processor.selectkey;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SelectKeyTest {

    private Processor<Long, String> processor;

    private ProcessorContext context;

    @Before
    public void setUp() throws Exception {
        SelectKey<Long, String, String> selectKey = new SelectKey<>((k, v) -> v);

        context = mock(ProcessorContext.class);
        processor = selectKey.get();
        processor.init(context);
    }

    @Test
    public void process() throws Exception {
        processor.process(1L, "test");

        verify(context).forward("test", "test");
    }
}