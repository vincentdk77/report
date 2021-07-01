package com.jiatuobao.mongo;

import com.alibaba.fastjson.JSONObject;
import com.mongodb.*;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import org.apache.log4j.Logger;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * 报错: The $changeStream stage is only supported on replica sets' on server node9:28018
 * Description : 监听mongo的变更事件
 */
public class MongoListener {

    private static final Logger logger = Logger.getLogger(MongoListener.class);

    private static final MongoDatabase database;

    private static final String HOST = "node9";
    private static final int PORT = 28018;
    private static final String DATABASE = "crm";
    private static final String USERNAME = "kemai";
    private static final String PASSWORD = "keimai@123!";

    static {
        ServerAddress address = new ServerAddress(HOST, PORT);
        MongoCredential credential = MongoCredential.createCredential(USERNAME, DATABASE, PASSWORD.toCharArray());
        MongoClientOptions options = MongoClientOptions.builder().sslEnabled(false).build();
        MongoClient client = new MongoClient(address, credential, options);
        database = client.getDatabase(DATABASE);
    }

    public static void watchDB() {

        Bson match = Aggregates.match(Filters.or(
                Document.parse("{'fullDocument.username': 'process_engine'}"),
                Filters.in(
                        "operationType",
                        Arrays.asList("insert", "update", "delete"))));

        List<Bson> pipeline = singletonList(match);

        MongoCursor<ChangeStreamDocument<Document>> cursor = database.getCollection("test")
                .watch(pipeline)
                .iterator();

        logger.info(cursor.next().getResumeToken());

        while (cursor.hasNext()) {

            ChangeStreamDocument<Document> next = cursor.next();
//            KafkaUtil.produce("hello-kafka",next.toString());
            logger.info(next.toString());

            // 集合名称
            String collection = null;
            MongoNamespace namespace = next.getNamespace();
            if (namespace != null) {
                collection = next.getNamespace().getCollectionName();
            }

            // 操作类型
            String operation = next.getOperationType().getValue();

            // 主键id
            String pk_id = null;
            BsonDocument documentKey = next.getDocumentKey();
            if (documentKey != null) {
                pk_id = JSONObject.parseObject(JSONObject.parseObject(documentKey.toString()).getString("_id")).getString("$oid");
            }

            // 同步插入数据的操作
            Document fullDocument = next.getFullDocument();
            logger.info("fullDocument:"+fullDocument.toJson());

            if (fullDocument != null && operation.matches("insert")) {
//                KafkaUtil.produce("hello-kafka", fullDocument.toString());
                logger.info(pk_id);
                logger.info("fullDocument:"+fullDocument.toJson());
            }

            // 同步修改数据的操作
            UpdateDescription description = next.getUpdateDescription();
            if (description != null && operation.matches("update")) {
                BsonDocument updatedFields = description.getUpdatedFields();
                logger.info(description.toString());
                if (updatedFields != null) {
                    logger.info(pk_id);
                    logger.info(updatedFields.toString());
                }
            }

            // 同步删除数据的操作
            if (description == null && operation.matches("delete")) {
                logger.info(pk_id);
            }
        }

//        String[] tables = {"test", "test2"};
//        ExecutorService threadPool = Executors.newFixedThreadPool(2);
//        for (String table : tables) {
//            threadPool.submit(() -> {
//
//            });
//        }
    }

    public static void main(String[] args) {
        watchDB();
    }
}
