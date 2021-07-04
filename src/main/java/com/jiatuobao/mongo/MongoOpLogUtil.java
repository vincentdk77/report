package com.jiatuobao.mongo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.jiatuobao.util.Constant;
import com.jiatuobao.util.MyKafkaSink;
import com.jiatuobao.util.MyRedisUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 1、删除
 * {
 *     "lsid": {
 *       "id": "0823d145-54e8-4b14-bf55-49363360df35",
 *       "uid": {"$binary": {"base64": "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=", "subType": "00"}}
 *     },
 *     "ns": "test.user",
 *     "o": {
 *       "_id": {"$oid": "60de7890f00a760c1de6f2c9"}
 *     },
 *     "op": "d",
 *     "prevOpTime": {
 *       "ts": {"$timestamp": {"t": 0, "i": 0}},
 *       "t": -1
 *     },
 *     "stmtId": 0,
 *     "t": 2,
 *     "ts": {"$timestamp": {"t": 1625192619, "i": 1}},
 *     "txnNumber": 5,
 *     "ui": "e41e1bdc-c352-4a63-b3df-4385fea42adc",
 *     "v": 2,
 *     "wall": {"$date": "2021-07-02T02:23:39.466Z"}
 *   }
 *
 * 2、修改
 * {
 *     "lsid": {
 *       "id": "0823d145-54e8-4b14-bf55-49363360df35",
 *       "uid": {"$binary": {"base64": "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=", "subType": "00"}}
 *     },
 *     "ns": "test.user",
 *     "o": {
 *       "$v": 1,
 *       "$set": {
 *         "address": "平舆县射桥镇单老集5"
 *       }
 *     },
 *     "o2": {
 *       "_id": {"$oid": "60de75fc786f243cce531376"}
 *     },
 *     "op": "u",
 *     "prevOpTime": {
 *       "ts": {"$timestamp": {"t": 0, "i": 0}},
 *       "t": -1
 *     },
 *     "stmtId": 0,
 *     "t": 2,
 *     "ts": {"$timestamp": {"t": 1625192522, "i": 1}},
 *     "txnNumber": 2,
 *     "ui": "e41e1bdc-c352-4a63-b3df-4385fea42adc",
 *     "v": 2,
 *     "wall": {"$date": "2021-07-02T02:22:02.089Z"}
 *   }
 *
 * 3、新增
 * {
 *     "ns": "test.user",
 *     "o": {
 *       "_id": {"$oid": "60de75fc786f243cce531376"},
 *       "id": 1,
 *       "name": "jack"
 *     },
 *     "op": "i",
 *     "t": 1,
 *     "ts": {"$timestamp": {"t": 1625191932, "i": 2}},
 *     "ui": "e41e1bdc-c352-4a63-b3df-4385fea42adc",
 *     "v": 2,
 *     "wall": {"$date": "2021-07-02T02:12:12.602Z"}
 *   }
 *
 */
@Slf4j
public class MongoOpLogUtil {
    public static void main(String[] args) {
        OpLogTest();
    }


    private static BsonTimestamp queryTs;
    private static List<String> nameSpaces = Lists.newArrayList("test.user");

    @Test
    public static  void OpLogTest() {
        Jedis jedis = MyRedisUtil.getJedisClient();
        String ts = jedis.get("mongo:ts");

//        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://admin:kemai%40startup!!@node11:27001"));
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://admin:admin@hadoop102:27018"));
        MongoCollection<Document> opLogCollection = mongoClient.getDatabase("local")
                .getCollection("oplog.rs");

        if(StringUtils.isNotBlank(ts)){
//            queryTs = JSON.parseObject(ts, BsonTimestamp.class);
            Document ts1 = JSON.parseObject(ts, Document.class);
            Long value = ts1.getLong("value");
            queryTs = new BsonTimestamp(value);
            log.warn("redisTs:"+JSON.toJSONString(queryTs));
        }else{
            //如果是首次订阅，需要使用自然排序查询，获取第最后一次操作的操作时间戳。如果是续订阅直接读取记录的值赋值给queryTs即可
            FindIterable<Document> tsCursor = opLogCollection.find().sort(new BasicDBObject("$natural", -1)).limit(1);
            Document tsDoc = tsCursor.first();
            queryTs = (BsonTimestamp) tsDoc.get("ts");
            log.warn("查询mongo最新ts:"+JSON.toJSONString(queryTs));
        }

        Integer i =0;

        while (true) {
//            try {
                //构建查询语句,查询大于当前查询时间戳queryTs的记录
                BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", queryTs));// TODO: 2021/7/2 只查指定的表
                MongoCursor<Document> opDocuments = opLogCollection.find(query)
                        .cursorType(CursorType.TailableAwait) //没有数据时阻塞休眠
                        .noCursorTimeout(true) //防止服务器在不活动时间（10分钟）后使空闲的游标超时。
                        .oplogReplay(true) //结合query条件，获取增量数据，这个参数比较难懂，见：https://docs.mongodb.com/manual/reference/command/find/index.html
                        .maxAwaitTime(1, TimeUnit.SECONDS) //设置此操作在服务器上的最大等待执行时间
                        .iterator();

                while (opDocuments.hasNext()) {

                    String message = "";
                    Document document = opDocuments.next();
                    //TODO 在这里接收到数据后通过订阅数据路由分发
                    String ns = document.getString("ns");

                    if(nameSpaces.contains(ns)){
                        log.warn("oplog:"+JSON.toJSONString(document));
                        log.warn("i="+ ++i);
                        String op = document.getString("op");// i、u、d
                        String database = ns.substring(0, ns.indexOf("."));
                        String tableName = ns.substring(ns.indexOf(".")+1);
                        Document context = (Document) document.get("o");//文档内容
                        Document where = null;

                        //处理新增
                        if (op.equals("i")) {
                            message = resembleJsonFields(context,database,tableName,"insert");
                            log.warn("insert:"+message);
                            MyKafkaSink.send(Constant.report_maxwell_topic(),message);
                        }

                        //处理修改
                        if (op.equals("u")) {
                            where = (Document) document.get("o2");//更新操作-查询条件
                            if (context != null) {
                                context = (Document) context.get("$set");
                            }

                            MongoCollection<Document> collection = mongoClient.getDatabase(database)
                                    .getCollection(tableName);
                            FindIterable<Document> documents = collection.find(where);

                            for (Document document1 : documents) {
                                message = resembleJsonFields(document1,database,tableName,"update");
                                log.warn("update:"+message);
                                MyKafkaSink.send(Constant.report_maxwell_topic(),message);
                            }
                        }

                        //处理删除
                        if (op.equals("d")) {
                            where = context;
                            MongoCollection<Document> testCollection = mongoClient.getDatabase(database)
                                    .getCollection(tableName);
                            FindIterable<Document> documents = testCollection.find(where);

                            for (Document document1 : documents) {
                                message = resembleJsonFields(document1,database,tableName,"delete");
                                log.warn("delete:"+message);
                                MyKafkaSink.send(Constant.report_maxwell_topic(),message);
                            }
                        }

                        //更新时间戳
                        queryTs = (BsonTimestamp) document.get("ts");

                        //保存时间戳
                        jedis.set("mongo:ts", JSON.toJSONString(queryTs));
//                        jedis.close();

//                        log.warn("操作时间戳：" + queryTs.getTime());
//                        log.warn("操作类型：" + op);
//                        log.warn("数据库.集合：" + ns);
//                        if (op.equals("u")) {
//                            log.warn("更新条件：" + where.getObjectId("_id").toHexString());
//                        }
////                        log.warn("更新条件：" + JSON.toJSONString(where));
//                        log.warn("文档内容：" + JSON.toJSONString(context));

                    }

                }
//            } catch (Exception e) {
//                e.printStackTrace();
//                log.error("出现异常，"+e.getMessage());
//                log.error("出现异常，"+e.getMessage());
//
//            }
        }
    }

    private static String resembleJsonFields(Document document1,String database,String tableName,String op) {
        JSONObject jsonObj = new JSONObject();
        String _id = document1.getObjectId("_id").toHexString();
        document1.put("_id",_id);
        jsonObj.put("data",document1);
        jsonObj.put("database",database);
        jsonObj.put("table",tableName);
        jsonObj.put("type",op);

        return JSON.toJSONString(jsonObj);
    }
}
