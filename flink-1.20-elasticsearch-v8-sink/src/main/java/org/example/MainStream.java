package org.example;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.monchickey.sink.ElasticsearchSink;
import com.monchickey.sink.ElasticsearchSinkBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;

import java.util.Map;

public class MainStream {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        DataStreamSource<User> streamSource = env.fromData(new User("1", "zjbscBsks9121", 23, 8.2f, "篮球"),
//                new User("2", "hbsgvcUMbvsc93810", 32, 8.3f, "足球"));
        SingleOutputStreamOperator<User> streamSource = env.socketTextStream("192.168.146.81", 9999)
                .map(m -> {
                    System.out.println(m);
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
                    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                    return mapper.readValue(m, User.class);
                });

        SingleOutputStreamOperator<String> mapStream = streamSource.map(x -> {
            ObjectMapper mapper = new ObjectMapper();
            mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            return mapper.writeValueAsString(x);
        });

        System.out.println(env.getParallelism());

        mapStream.print();

        ElasticsearchSink esSink = ElasticsearchSinkBuilder.builder()
                .setUsername("elastic")
                .setPassword("123456")
                .setHttpHost(new HttpHost("127.0.0.1", 9200, "https"))
                .setBatchSize(100)
                .setTimeInterval(10000L)
                .build();

        streamSource.map(user -> new BulkOperation.Builder().index(
                u -> {
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
                    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                    return u.id(user.getUserId())
                            .index("user_index")
                            .document(mapper.convertValue(user, new TypeReference<Map<String, Object>>() {
                            }));
                }
        ).build()).sinkTo(esSink).setParallelism(2);

        env.execute("user stream");
    }
}
