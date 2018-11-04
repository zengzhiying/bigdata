import com.jsoniter.output.JsonStream;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * elasticsearch 高级api使用demo
 * @author zengzhiying
 */
public class ElasticSearchDemo {
    public static void main(String[] args) throws IOException, InterruptedException {
        HttpHost[] httpHosts = new HttpHost[2];
        httpHosts[0] = new HttpHost("192.168.4.211", 9200, "http");
        httpHosts[1] = new HttpHost("192.168.4.212", 9200, "http");
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("192.168.4.211", 9200, "http"),
                        new HttpHost("192.168.4.211", 9200, "http")
                        // builder传入httpHosts也可以
                )
        );

        System.out.println("连接成功.");

        // indexTest(client);
//         getById(client, "str1");
//        getById(client, "0036");
//        idExists(client, "str2");
//        deleteById(client, "0012");
//        updateTest(client);
//        bulkTest(client);
        multiGetByIds(client, new String[] {"0012", "0023", "0036", "test1", "test"});
        try {
            client.close();
            System.out.println("close.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void indexTest(RestHighLevelClient client) throws IOException, InterruptedException {
        IndexRequest request = new IndexRequest("test1", "_doc", "150293821283922");
        Map<String, Object> m = new HashMap<>();
        m.put("failed1", "H0872");
        m.put("ctime", System.currentTimeMillis()/1000);
        m.put("uid", 3708231832091L);
        m.put("yid", 3827);
        String message = JsonStream.serialize(m);
        System.out.println(message);
        // 写入json
        request.source(message, XContentType.JSON);

        // 写入map
        IndexRequest request1 = new IndexRequest("test2", "_doc", "0D7E5D6AC78A");
        m.put("failed1", "DH8632");
        request1.source(m);

        // builder方式自动编译json写入
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("failed1", "ES6380");
            builder.field("ctime", 10282);
            builder.field("laid", 378021);
            builder.field("yid", 782);
            builder.field("url", "http://www.baidu.com");
        }
        builder.endObject();
        IndexRequest request2 = new IndexRequest("test3", "_doc", "0012")
                .source(builder);
        // 设置超时
        request.timeout(TimeValue.timeValueSeconds(3));
        // 设置optype 默认: INDEX
        request.opType(DocWriteRequest.OpType.CREATE);

        // 同步提交
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        String indexName = response.getIndex();
        String indexType = response.getType();
        String indexId = response.getId();
        long indexVersion = response.getVersion();
        System.out.println("index: " + indexName + " type: " + indexType + " id: " +
                indexId + " version: " + indexVersion);
        if(response.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功！");
        } else if(response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        if(shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("不相等！");
        }
        if(shardInfo.getFailed() > 0) {
            for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
                System.out.println("其他错误: " + reason);
            }
        }

        // 异步提交
        ActionListener<IndexResponse> listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println("异步提交成功！" + indexResponse.getId());
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("异步提交失败！" + e.getStackTrace());
            }
        };
        client.indexAsync(request1, RequestOptions.DEFAULT, listener);
        System.out.println("request1 async.");
        client.indexAsync(request2, RequestOptions.DEFAULT, listener);
        System.out.println("request2 async.");
        Thread.sleep(10000);
    }


    public static void getById(RestHighLevelClient client, String id) throws IOException {
        GetRequest getRequest = new GetRequest("test1", "_doc", id);
        // 是否带source
        getRequest.fetchSourceContext(FetchSourceContext.FETCH_SOURCE);
        // 配置存储字段, 创建索引时需要存储数据
        // getRequest.storedFields("title", "name");
        // 是否刷新数据 默认不刷新
        getRequest.refresh(true);
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
        String indexName = getResponse.getIndex();
        String type = getResponse.getType();
        String indexId = getResponse.getId();
        if(getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            System.out.println("source: " + sourceAsString + "\nmap: " + sourceAsMap);
            // 当存储字段时可以使用下面获取结果
//            String captureTime = getResponse.getField("capture_time").getValue();
//            System.out.println("capture time: " + captureTime);
        } else {
            System.out.println("数据不存在.");
        }
    }

    public static void idExists(RestHighLevelClient client, String id) throws IOException {
        GetRequest getRequest = new GetRequest("test2", "_doc", id);
        // 禁用source和存储字段, 提高性能
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);
        if(exists)
            System.out.println("id: " + id + " 存在.");
        else
            System.out.println("id: " + id + " 不存在.");
    }

    public static void deleteById(RestHighLevelClient client, String id) throws IOException {
        DeleteRequest request = new DeleteRequest("test3", "_doc", id);
        request.timeout(TimeValue.timeValueSeconds(3));
        // 指定删除的版本
        // request.version(102);
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        String index = response.getIndex();
        String type = response.getType();
        String indexId = response.getId();
        long version = response.getVersion();
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        if(shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("成功碎片小于碎片总数.");
        }
        // 其他错误处理
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }
        if(response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
            System.out.println("id: " + id + " 不存在！ " + indexId);
        }

        if(response.getResult() == DocWriteResponse.Result.DELETED) {
            System.out.println("id: " + id + "删除成功！ " + indexId);
        }

    }

    public static void updateTest(RestHighLevelClient client) throws IOException {
        UpdateRequest request = new UpdateRequest("test6", "_doc", "0023");
        Map<String, Object> m = new HashMap<>();
        m.put("ptype", "3");
        m.put("laid", 9912);
        request.doc(m);
        request.timeout(TimeValue.timeValueSeconds(3));
        // 如果冲突, 重试次数
        request.retryOnConflict(3);
        // 获得source 默认关闭
        request.fetchSource(true);
        // 是否指定upsert, 即如果文档不存在, 则部分插入, 默认关闭
        // request.docAsUpsert(true);
        // 禁用noop检测
//        request.detectNoop(false);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        if(response.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功！");
        } else if(response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        } else if(response.getResult() == DocWriteResponse.Result.DELETED) {
            System.out.println("删除成功！");
        } else if(response.getResult() == DocWriteResponse.Result.NOOP) {
            System.out.println("空操作.");
        }
        GetResult result = response.getGetResult();
        if(result.isExists()) {
            String sourceAsString = result.sourceAsString();
            Map<String, Object> sourceAsMap = result.sourceAsMap();
            System.out.println("source: " + new String(result.source()));
        } else {
            System.out.println("结果不存在.");
        }
    }

    public static void bulkTest(RestHighLevelClient client) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        String index = "test7";
        String type = "_doc";
        IndexRequest request1 = new IndexRequest(index, type, "0036");
        request1.source(XContentType.JSON, "lp", "P9B7",
                "ctime", 153246781L, "laid", 1001211L,
                "yid", 382);
        UpdateRequest request2 = new UpdateRequest(index, type, "0023");
        request2.doc(XContentType.JSON, "laid", 9028);
//        DeleteRequest request3 = new DeleteRequest(index, type, "0010");
        // 添加操作
        bulkRequest.add(request1);
        bulkRequest.add(request2);
        // 配置参数
        bulkRequest.timeout(TimeValue.timeValueMinutes(2));
        // 操作前必须处于活动的分片副本数 默认为: ActiveShardCount.DEFAULT
        // ActiveShardCount.ALL 全部, ActiveShardCount.ONE 1个, 可以自定义个数
        // bulkRequest.waitForActiveShards(2);
        // 同步操作 批量执行
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        // 另外async用法和索引类似
        for(BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            if(bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                    || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                System.out.println("创建成功！ id: " + indexResponse.getId());
            } else if(bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                System.out.println("更新成功！ id: " + updateResponse.getId());
            } else if(bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                System.out.println("删除成功！ id: " + deleteResponse.getId());
            }
        }
    }

    public static void multiGetByIds(RestHighLevelClient client, String[] ids) throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        for(String id:ids) {
            request.add(new MultiGetRequest.Item("test8", "_doc", id)
                .fetchSourceContext(FetchSourceContext.FETCH_SOURCE));
        }
        MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
        MultiGetItemResponse[] items = response.getResponses();
        for(MultiGetItemResponse item:items) {
            assert item.getFailure() == null;
            // ElasticsearchException e = (ElasticsearchException) item.getFailure().getFailure();
            GetResponse itemResponse = item.getResponse();
            String index = item.getIndex();
            String type = item.getType();
            String id = item.getId();
            if(itemResponse.isExists()) {
                long version = itemResponse.getVersion();
                String sourceAsString = itemResponse.getSourceAsString();
                Map<String, Object> sourceAsMap = itemResponse.getSourceAsMap();
                System.out.println("id: " + id + " source: " + new String(itemResponse.getSourceAsBytes()));
            } else {
                System.out.println("id: " + id + "不存在！");
            }
        }
    }
}
