import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Properties;

public class KafkaTradeConsumerTest {
    @Test
    public void testBootstrapServers() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        assertEquals("localhost:9092", props.getProperty("bootstrap.servers"));
    }
    @Test
    public void testGroupId() {
        Properties props = new Properties();
        props.put("group.id", "test-group");
        assertEquals("test-group", props.getProperty("group.id"));
    }
    @Test
    public void testDeserializers() {
        Properties props = new Properties();
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", props.getProperty("key.deserializer"));
        assertEquals("org.apache.kafka.common.serialization.StringDeserializer", props.getProperty("value.deserializer"));
    }
    @Test
    public void testAutoOffsetReset() {
        Properties props = new Properties();
        props.put("auto.offset.reset", "earliest");
        assertEquals("earliest", props.getProperty("auto.offset.reset"));
    }
    @Test
    public void testPropertiesNotNull() {
        Properties props = new Properties();
        assertNotNull(props);
    }
}
