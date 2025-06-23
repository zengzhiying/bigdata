package com.monchickey.sink;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

public class ElasticsearchSink implements Sink<BulkOperation> {
    private final ElasticsearchConfigFactory factory;
    private final Integer batchSize;
    private final Long timeInterval;

    public ElasticsearchSink(ElasticsearchConfigFactory factory, Integer batchSize, Long timeInterval) {
        this.factory = factory;
        this.batchSize = batchSize;
        this.timeInterval = timeInterval;
    }

    @Override
    public SinkWriter<BulkOperation> createWriter(InitContext initContext) {
        try {
            // 和之前的 open 方法一致，每个并行度初始化一次
            return new ElasticsearchSinkWriter(factory, batchSize, timeInterval);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (KeyStoreException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (KeyManagementException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
