import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;

public class TradeWindowJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "market.trades",
                new SimpleStringSchema(),
                props);

        DataStream<String> stream = env.addSource(consumer);

        DataStream<String> windowedCounts = stream
                .map(new SafeTradeParser())       // парсим без null
                .filter(trade -> trade.symbol != null && !trade.symbol.isEmpty())
                .keyBy(trade -> trade.symbol)
                .timeWindow(Time.seconds(5))
                .apply(new TradeCountWindow());

        windowedCounts.print();

        env.execute("Trade Window Job");
    }

    // Класс сделки
    public static class Trade {
        public String symbol;
        public double price;
        public double quantity;

        public Trade() {}
    }

    // Безопасный парсер: извлекает symbol из JSON
    public static class SafeTradeParser implements MapFunction<String, Trade> {
        @Override
        public Trade map(String value) {
            Trade trade = new Trade();
            try {
                // Извлекаем "symbol":"BTCUSDT" через регулярку
                java.util.regex.Pattern p = java.util.regex.Pattern.compile("\"symbol\"\\s*:\\s*\"([^\"]+)\"");
                java.util.regex.Matcher m = p.matcher(value);
                if (m.find()) {
                    trade.symbol = m.group(1);
                }
                // Аналогично price
                p = java.util.regex.Pattern.compile("\"price\"\\s*:\\s*([0-9.]+)");
                m = p.matcher(value);
                if (m.find()) {
                    trade.price = Double.parseDouble(m.group(1));
                }
                // quantity
                p = java.util.regex.Pattern.compile("\"quantity\"\\s*:\\s*([0-9.]+)");
                m = p.matcher(value);
                if (m.find()) {
                    trade.quantity = Double.parseDouble(m.group(1));
                }
                if (trade.symbol == null) {
                    System.err.println("Failed to parse symbol from: " + value);
                }
            } catch (Exception e) {
                System.err.println("Error parsing message: " + value + " | " + e.getMessage());
            }
            return trade;
        }
    }

    // Оконная функция
    public static class TradeCountWindow implements WindowFunction<Trade, String, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Trade> input, Collector<String> out) {
            int count = 0;
            for (Trade trade : input) {
                count++;
            }
            out.collect(key + " | " + count + " trades in last 5 sec");
        }
    }
}