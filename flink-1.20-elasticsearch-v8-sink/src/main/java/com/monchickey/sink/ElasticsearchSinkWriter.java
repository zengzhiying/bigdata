package com.monchickey.sink;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class ElasticsearchSinkWriter implements SinkWriter<BulkOperation> {
    private final static Logger log = LogManager.getLogger(ElasticsearchSink.class);
    private transient ElasticsearchClient client;
    private final ElasticsearchConfigFactory factory;
    private Long lastTime;
    private final Object lock = new Object();
    private final Integer batchSize;
    private final Long timeIntervalMs;
    private final List<BulkOperation> operations;
    private final BlockingQueue<BulkOperation> queue = new LinkedBlockingQueue<>(10000);
    private final ExecutorService writeExecutor = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService queueExecutor = Executors.newSingleThreadScheduledExecutor();

    public ElasticsearchSinkWriter(ElasticsearchConfigFactory factory, Integer batchSize, Long timeIntervalMs) throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        this.factory = factory;
        this.batchSize = batchSize;
        this.timeIntervalMs = timeIntervalMs;
        operations = new ArrayList<>(batchSize);
        client = factory.create();
        lastTime = System.currentTimeMillis();
        consumer();
        queueExecutor.schedule(() -> {
            if(queue.size() > 1000) {
                log.warn("Elasticsearch sink writer queue size: {}", queue.size());
            }
        }, 60, TimeUnit.SECONDS);
    }

    private void consumer() {
        writeExecutor.submit(() -> {
            while(!Thread.interrupted()) {
                try {
                    BulkOperation item = queue.poll(100, TimeUnit.MILLISECONDS);
                    if(item != null) {
                        operations.add(item);
                    }
                    if(operations.size() >= batchSize) {
                        flush(false);
                        lastTime = System.currentTimeMillis();
                    } else if(System.currentTimeMillis() - lastTime >= timeIntervalMs) {
                        flush(false);
                        lastTime = System.currentTimeMillis();
                    }
                } catch (InterruptedException e) {
                    log.warn("The current thread is interrupted! {}", e.toString());
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    @Override
    public void write(BulkOperation bulkOperation, Context context) throws InterruptedException {
        queue.put(bulkOperation);
    }

    @Override
    public void flush(boolean b) {
        if(operations.isEmpty()) {
            return;
        }
        // 开始写入
        try {
            BulkResponse response = client.bulk(new BulkRequest.Builder()
                    .operations(operations).build());
            if(response.errors()) {
                for (BulkResponseItem item: response.items()) {
                    if (item.error() != null) {
                        log.error(item.error().reason());
                    }
                }
            } else {
                log.debug("Elasticsearch flush count: " + response.items().size());
            }
        } catch (IOException e) {
            log.error("Elasticsearch sink flush operations({}) error: {}",
                    operations.size(), ExceptionTrace.getStackTraceAsString(e));
        }
        operations.clear();
    }

    @Override
    public void close() throws Exception {
        queueExecutor.shutdown();
        writeExecutor.shutdown();
        queueExecutor.awaitTermination(5, TimeUnit.SECONDS);
        writeExecutor.awaitTermination(5, TimeUnit.SECONDS);
        log.warn("Last refresh count: {}", operations.size());
        flush(true);
    }
}
