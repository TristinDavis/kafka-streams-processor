package demo.operation.stream.processor.flatmap;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.util.ArrayUtils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class FlatMapValuesTest {

    private Processor<Long, String> processor;

    private ProcessorContext context;

    @Before
    public void setUp() throws Exception {
        FlatMapValues<Long, String, String> flatMapValues
            = new FlatMapValues<>(input -> ArrayUtils.toUnmodifiableList(input.split("\\W+")));
        context = mock(ProcessorContext.class);

        processor = flatMapValues.get();
        processor.init(context);
    }

    @Test
    public void process() throws Exception {
        processor.process(1L, "test flatmap values");

        verify(context).forward(1L, "test");
        verify(context).forward(1L, "flatmap");
        verify(context).forward(1L, "values");
    }

}