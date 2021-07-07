package com.jiatuobao.mongo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.jiatuobao.utils.Constant;
import com.jiatuobao.utils.MyKafkaSink;
import com.jiatuobao.utils.MyMongoUtil;
import com.jiatuobao.utils.MyRedisUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoIterable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
public class MongoOpLogUtilIncrementShard2 {
    private static BsonTimestamp queryTs;
    private static List<String> nsLists = Lists.newArrayList();

    public static void main(String[] args) throws InterruptedException {
        Jedis jedis = MyRedisUtil.getJedisClient();
        MongoClient mongoClient1 = MyMongoUtil.getMongoShard1Client();
        MongoClient mongoClient2 = MyMongoUtil.getMongoShard2Client();

        OpLogTest(jedis,mongoClient2,mongoClient1);
    }

    @Test
    public static  void OpLogTest(Jedis jedis, MongoClient mongoClient2,MongoClient mongoClient1) throws InterruptedException {
//        Jedis jedis = MyRedisUtil.getJedisClient();
        String ts = jedis.get("mongo:ts");

//        //连接mongo shard地址
//        MongoClient mongoShard1Client = MyMongoUtil.getMongoShard1Client();
//        MongoClient mongoShard2Client = MyMongoUtil.getMongoShard2Client();

        //获取命名空间list // TODO: 2021/7/6 shard2上没有crm库，无法查询有哪些crm表
        MongoIterable<String> collectionNames = mongoClient1.getDatabase("crm").listCollectionNames();

        for (String collectionName : collectionNames) {
            String ns = "crm."+collectionName;
            if(StringUtils.isNotBlank(ns)){
                nsLists.add(ns);
            }
        }
        nsLists = nsLists.stream().filter(ns ->{
            if(ns.startsWith("crm.saas_clue")
                    || ns.startsWith("crm.saas_customer")
                    || ns.startsWith("crm.saas_contact")
                    || ns.startsWith("crm.saas_opport")
                    || ns.startsWith("crm.saas_agreement")
                    || ns.startsWith("crm.saas_record")){
//                if(ns.startsWith("crm.saas_opport.20664")){//测试单表
                return true;
            }else{
                return false;
            }
        }).collect(Collectors.toList());
        System.out.println("查询oplog的命名空间list:"+nsLists);

        //获取oplog时间戳
        MongoCollection<Document> opLogCollection = mongoClient2.getDatabase("local")
                .getCollection("oplog.rs");

        if(StringUtils.isNotBlank(ts)){
//            queryTs = JSON.parseObject(ts, BsonTimestamp.class);
            Document ts1 = JSON.parseObject(ts, Document.class);
            Long value = ts1.getLong("value");
            queryTs = new BsonTimestamp(value);
            log.warn("redisTs:"+JSON.toJSONString(queryTs));
        }else{
            //如果是首次订阅，需要使用自然排序查询，获取第最后一次操作的操作时间戳。
            // 如果是续订阅直接读取记录的值赋值给queryTs即可
            FindIterable<Document> tsCursor = opLogCollection.find().sort(new BasicDBObject("$natural", -1)).limit(1);
            Document tsDoc = tsCursor.first();
            queryTs = (BsonTimestamp) tsDoc.get("ts");
            log.warn("查询mongo最新ts:"+JSON.toJSONString(queryTs));
        }

        Long i = 0L;

        while (true) {
//            try {
                //构建查询语句,查询大于当前查询时间戳queryTs的记录
                BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", queryTs))
                        .append("ns",new BasicDBObject("$in", nsLists));
                MongoCursor<Document> opDocuments = opLogCollection.find(query).limit(2000)//每次读2千条数据，防止内存崩了
                        .cursorType(CursorType.TailableAwait) //没有数据时阻塞休眠
                        .noCursorTimeout(true) //防止服务器在不活动时间（10分钟）后使空闲的游标超时。
                        .oplogReplay(true) //结合query条件，获取增量数据，这个参数比较难懂，见：https://docs.mongodb.com/manual/reference/command/find/index.html
                        .maxAwaitTime(1, TimeUnit.SECONDS) //设置此操作在服务器上的最大等待执行时间
                        .iterator();
                if(opDocuments.hasNext()){
                    while (opDocuments.hasNext()) {

                        String message = "";
                        Document document = opDocuments.next();
                        //TODO 在这里接收到数据后通过订阅数据路由分发
                        String ns = document.getString("ns");

                        log.warn("i="+ ++i);
                        log.warn("oplog:"+JSON.toJSONString(document));
                        String op = document.getString("op");// i、u、d
                        String database = ns.substring(0, ns.indexOf("."));
                        String realTableName = ns.substring(ns.indexOf(".")+1);
//                        String sendTableName = ns.substring(ns.indexOf(".")+1,ns.lastIndexOf("."));
                        Document context = (Document) document.get("o");//文档内容
                        Document where = null;

                        //处理新增
                        if (op.equals("i")) {
                            message = resembleJsonFields(context,database,realTableName,"insert");
                            log.warn("insert: realTableName="+realTableName+":"+message);
                            MyKafkaSink.send(Constant.report_maxwell_topic(),message);
                        }

                        //处理修改
                        if (op.equals("u")) {
                            where = (Document) document.get("o2");//更新操作-查询条件
                            if (context != null) {
                                context = (Document) context.get("$set");
                            }

                            MongoCollection<Document> collection = mongoClient2.getDatabase(database)
                                    .getCollection(realTableName);
                            FindIterable<Document> documents = collection.find(where);

                            for (Document document1 : documents) {
                                message = resembleJsonFields(document1,database,realTableName,"update");
                                log.warn("update: realTableName="+realTableName+":"+message);
                                MyKafkaSink.send(Constant.report_maxwell_topic(),message);
                            }
                        }

                        //处理删除
                        if (op.equals("d")) {
                            where = context;
                            MongoCollection<Document> testCollection = mongoClient2.getDatabase(database)
                                    .getCollection(realTableName);
                            FindIterable<Document> documents = testCollection.find(where);

                            for (Document document1 : documents) {
                                message = resembleJsonFields(document1,database,realTableName,"delete");
                                log.warn("delete: realTableName="+realTableName+":"+message);
                                MyKafkaSink.send(Constant.report_maxwell_topic(),message);
                            }
                        }

                        //更新时间戳
                        queryTs = (BsonTimestamp) document.get("ts");

                        //保存时间戳
                        jedis.set("mongo:ts", JSON.toJSONString(queryTs));
                        System.out.println("ts:"+queryTs);
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
                }else{
                    log.warn("没有变更数据，休眠10s");
                    Thread.sleep(10*1000);
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
//        String _id = document1.getObjectId("_id").toHexString();
        document1.remove("_id");
        jsonObj.put("data",document1);
        jsonObj.put("database",database);
        jsonObj.put("table",tableName);
        jsonObj.put("type",op);

        return JSON.toJSONString(jsonObj);
    }
}
