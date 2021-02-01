package net.zengzhiying.streams;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * kafka streams pipe example
 * @author zengzhiying
 *
 */
public class PipeExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.182.130:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // 设置起始偏移, 最新: latest 最早: earliest 默认: earliest
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 设置group id, 默认不设置以application.id为准
        // props.put(ConsumerConfig.GROUP_ID_CONFIG, "streams_test");
        
        final StreamsBuilder builder = new StreamsBuilder();
        
        KStream<String, String> source = builder.stream("streamsTest");
        source.flatMapValues(value -> Arrays.asList(value.split("\\W+")))
            .groupBy((key, word) -> word)
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
            .toStream()
            .to("streamsOutput", Produced.with(Serdes.String(), Serdes.Long()));
        
        source.flatMapValues(value -> Arrays.asList(value.split("\\W+")))
            .to("streamsOutput");
        
        // 自定义处理 1
        source.flatMapValues(new ValueMapper<String, Iterable<String>>() {

            @Override
            public Iterable<String> apply(String value) {
                // process
                System.out.println(value);
                char c = value.substring(3).charAt(0);
                int v = c + 3;
                System.out.println(v);
                return Arrays.asList(value.split(" "));
            }
        }).foreach((k, v) -> {
            System.out.println(k);
            System.out.println(v);
        });
        
        // 自定义处理 2
        source.foreach((key, value) -> {
            // 进行分布式处理
            try {
                InetAddress addr = InetAddress.getLocalHost();
                System.out.println(addr.getHostName().toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
//            System.out.println(key);
            System.out.println(value);
            if(value != null && !value.equals("")) {
                char c = value.charAt(0);
                int v = c + 3;
                System.out.println(v);
            }
        });
        
        final Topology topology = builder.build();
        
        final KafkaStreams streams = new KafkaStreams(topology, props);
        
        final CountDownLatch latch = new CountDownLatch(1);
        
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
                System.out.println("streams close.");
            }
        });
 
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
