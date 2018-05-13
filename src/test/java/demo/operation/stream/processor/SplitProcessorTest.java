package demo.operation.stream.processor;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SplitProcessorTest {

    private Processor<Long, String> processor;

    private ProcessorContext context;

    @Before
    public void setUp() throws Exception {
        SplitProcessor splitProcessor = new SplitProcessor();

        context = mock(ProcessorContext.class);
        processor = splitProcessor.get();
        processor.init(context);
    }

    @Test
    public void process() throws Exception {
        processor.process(1L, "test split process");

        verify(context).forward(1L, "test");
        verify(context).forward(1L, "split");
        verify(context).forward(1L, "process");
    }
}