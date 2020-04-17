package example;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.arangodb.*;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.MultiDocumentEntity;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.util.MapBuilder;
import com.arangodb.velocypack.VPackSlice;

import java.io.IOException;
import java.util.*;

public class ArangoExample {
    public static void main(String[] args) {
        System.out.println("ArangoExample.");
        final ArangoDB arangoDB = new ArangoDB.Builder()
                .host("192.168.0.201", 8529)
                .user("example").password("example")
                .build();
        ArangoDatabase database = arangoDB.db("example");
        ArangoCollection collection = database.collection("java_example");
//        System.out.println(collection.exists());
        if(!collection.exists()) {
            collection.create();
//            database.createCollection("java_example");
        }
        // database.collection("abc").drop();
        //insertDocuments(collection);

        readDocumetByKey(collection);
        try {
            updateDocumentByKey(collection);
        } catch (ArangoDBException e) {
            System.out.println("更新error");
            e.printStackTrace();
        }
        //deleteDocumentByKey(collection);
        aqlQuery(database);

        // 插入edge 其余操作edge的方法和document一样
        // 和graph的edge api操作效果相同
        JSONObject jo = new JSONObject();
        jo.put("_key", "11");
        jo.put("_from", "java_example/1");
        jo.put("_to", "java_example/2");
        jo.put("type", 3);
        try {
            database.collection("example_edge").insertDocument(jo.toJSONString());
        } catch (ArangoDBException e) {
            e.printStackTrace();
        }

        arangoDB.shutdown();
    }

    /**
     * 插入多条文档, 插入单条用法也一致
     * @param collection
     */
    private static void insertDocuments(ArangoCollection collection) {
        DocumentBean doc1 = new DocumentBean();
        doc1.setName("小明");
        doc1.setAge(23);
        doc1.setIdNumber("3701");
        // 手动设置key 不设置使用自增
        doc1.setKey("1");
        DocumentBean doc2 = new DocumentBean();
        doc2.setName("张三");
        doc2.setAge(26);
        doc2.setIdNumber("3702");
        doc2.setKey("2");
        // 插入POJO类
        //List<DocumentBean> l = new ArrayList<>(2);
        //l.add(0, doc1);
        //l.add(1, doc2);
        // 插入json字符串
        List<String> l = new ArrayList<>(2);
        l.add(0, JSON.toJSONString(doc1));
        l.add(1, JSON.toJSONString(doc2));
        //MultiDocumentEntity<DocumentCreateEntity<DocumentBean>> results = collection.insertDocuments(l);
        MultiDocumentEntity<DocumentCreateEntity<String>> results = collection.insertDocuments(l);
        Collection<DocumentCreateEntity<String>> docs = results.getDocuments();
        Iterator<DocumentCreateEntity<String>> iter = docs.iterator();
        while (iter.hasNext()) {
            DocumentCreateEntity<String> r = iter.next();
            System.out.println("id: " + r.getId() + " key:" + r.getKey());
        }
    }

    /**
     * 按照key读取文档, 1个或者多个用法类似
     * @param collection
     */
    private static void readDocumetByKey(ArangoCollection collection) {
        // 获取basedocument
        BaseDocument doc1 = collection.getDocument("1", BaseDocument.class);
        System.out.println("doc1: " + doc1.getId() + " map: " + doc1.getProperties().toString());
        System.out.println(doc1.getAttribute("id_number").toString());

        // 获取vpack
        VPackSlice doc2 = collection.getDocument("2539", VPackSlice.class);
        System.out.println("doc2: " + doc2.get("_key").getAsString() + " age: " + doc2.get("age").getAsInt());
        System.out.println(doc2.get("id_number").getAsString());
        System.out.println(doc2.get("name").getAsString());

        // 获取普通对象 字段一般不能直接对应
        JSONObject doc3 = collection.getDocument("2429", JSONObject.class);
        System.out.println(doc3.toJSONString());
        System.out.println(doc3.toJavaObject(DocumentBean.class).toString());

        // 获取不存在的对象
        String doc4 = collection.getDocument("kkkk", String.class);
        System.out.println(doc4);
        if(doc4 == null) {
            System.out.println("key kkkk 不存在");
        }
        // 或者先判断
        System.out.println(collection.documentExists("kkkk"));


    }

    private static void updateDocumentByKey(ArangoCollection collection) {
        DocumentBean doc = new DocumentBean();
        // 这里设置Key无效
        doc.setKey("1111");
        doc.setAge(30);
        doc.setIdNumber("8888");
        collection.updateDocument("2429" ,JSON.toJSONString(doc));

        BaseDocument bd = new BaseDocument();
        Map<String, Object> m = new HashMap<>();
        Map<String, Integer> m1 = new HashMap<>();
        m1.put("dd", 3);
        //m1.put("dd", 4);
        //注意更新子文档不会覆盖之前的, 但是array会替换
        m.put("a", m1);
        ArrayList l = new ArrayList();
        l.add(m1);
        m.put("b", l);
        bd.setProperties(m);
        collection.updateDocument("2", bd);
    }

    private static void deleteDocumentByKey(ArangoCollection collection) {
        collection.deleteDocument("2242");
    }

    private static void aqlQuery(ArangoDatabase db) {
        String query = "FOR v IN @@collection FILTER v.id_number == @id_number RETURN v";
        Map<String, Object> bindVars = new MapBuilder().put("id_number", "3702")
                .put("@collection", "java_example").get();
        //ArangoCursor<DocumentBean> cursor = db.query(query, bindVars, new AqlQueryOptions(), DocumentBean.class);
        ArangoCursor<String> cursor = db.query(query, bindVars, null, String.class);
        while (cursor.hasNext()) {
            JSONObject jsonObj = JSON.parseObject(cursor.next());
            System.out.println(jsonObj.toJSONString());
        }
        try {
            cursor.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        query = "FOR v IN java_example FILTER v.age == @age REMOVE v IN java_example LET removed = OLD RETURN removed";
        bindVars = new MapBuilder().put("age", "'30'").get();
        ArangoCursor<BaseDocument> cursor1 = db.query(query, bindVars, null, BaseDocument.class);
        while(cursor1.hasNext()) {
            BaseDocument row = cursor1.next();
            System.out.println("delete key: " + row.getKey());
            System.out.println(row.getProperties());
        }
        try {
            cursor1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
