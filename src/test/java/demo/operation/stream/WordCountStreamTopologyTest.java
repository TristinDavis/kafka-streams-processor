package demo.operation.stream;

import demo.operation.helper.TopologyTestHelper;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static demo.operation.helper.TopologyTestHelper.SOURCE_TOPIC;
import static demo.operation.stream.processor.CountProcessor.WORD_COUNT_STORE;
import static org.assertj.core.api.Assertions.assertThat;

public class WordCountStreamTopologyTest {

    private WordCountStream wordCountStream;

    private TopologyTestDriver testDriver;

    private ConsumerRecordFactory<Long, String> factory;

    @Before
    public void setUp() throws Exception {
        wordCountStream = new WordCountStream(TopologyTestHelper.kafkaStreamProperties());
        testDriver = new TopologyTestDriver(wordCountStream.buildTopology(), TopologyTestHelper.streamProperties());
        factory = new ConsumerRecordFactory<>(SOURCE_TOPIC, new LongSerializer(), new StringSerializer());
    }

    @Test
    public void 토포로지_테스트() throws Exception {
        sendMessage(1L, "hi my name is gunju");
        sendMessage(2L, "hi my name");
        sendMessage(3L, "hi my");
        sendMessage(4L, "gunju is man");
        sendMessage(5L, "gunju have a computer");
        sendMessage(6L, "hello gunju");
        sendMessage(7L, "hi");

        Map<String, Long> results = new HashMap<>();
        results.put("hi", 4L);
        results.put("gunju", 4L);
        results.put("hello", 1L);
        results.put("my", 3L);
        results.put("name", 2L);
        results.put("is", 2L);
        results.put("a", 1L);
        results.put("man", 1L);
        results.put("computer", 1L);

        assertResultMessage(results);
    }

    private void assertResultMessage(Map<String, Long> results) {
        KeyValueStore<String, Long> store = testDriver.getKeyValueStore(WORD_COUNT_STORE);
        assertThat(store).isNotNull();

        results.forEach((word, expectedValue) -> {
            long count = store.get(word);
            assertThat(count).isEqualTo(expectedValue);
        });
    }

    private void sendMessage(long key, String value) {
        testDriver.pipeInput(factory.create(key, value));
    }

    @After
    public void after() throws Exception {
        testDriver.close();
    }
}