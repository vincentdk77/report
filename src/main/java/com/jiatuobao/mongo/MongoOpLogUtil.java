package com.jiatuobao.mongo;

import com.alibaba.fastjson.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * 1、删除
 * 操作时间戳：1625138511
 * 操作类型：d
 * 数据库.集合：ent.ent_patent
 * 更新条件：null
 * 文档内容：{"_id":{"counter":12351127,"date":1603642041000,"machineIdentifier":13065176,"processIdentifier":7005,"time":1603642041000,"timeSecond":1603642041,"timestamp":1603642041}}
 *
 * 2、修改
 * 操作时间戳：1625138511
 * 操作类型：u
 * 数据库.集合：ent.ent_shareholder
 * 更新条件：{"_id":{"counter":15877654,"date":1598653290000,"machineIdentifier":1112187,"processIdentifier":9721,"time":1598653290000,"timeSecond":1598653290,"timestamp":1598653290}}
 * 文档内容：{"ratio":50.0,"source":"爱企查","sourceUrl":"https://aiqicha.baidu.com/detail/basicAllDataAjax?pid=97862760071105","updatedAt":1625138511}
 *
 * 3、新增
 * 操作时间戳：1625141025
 * 操作类型：i
 * 数据库.集合：ent.ent_names
 * 更新条件：null
 * 文档内容：{"_id":{"counter":13979284,"date":1625141024000,"machineIdentifier":4006702,"processIdentifier":-7959,"time":1625141024000,"timeSecond":1625141024,"timestamp":1625141024},"entName":"河北神冠建材有限公司","entType":1,"sn":"神冠建材","updatedAt":1625141024,"createdAt":1625141024}
 *
 */
public class MongoOpLogUtil {
    public static void main(String[] args) {
        OpLogTest();
    }


    private static BsonTimestamp queryTs;

    @Test
    public static  void OpLogTest() {
        MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://admin:kemai%40startup!!@node11:27001"));
        MongoCollection<Document> collection = mongoClient.getDatabase("local")
                .getCollection("oplog.rs");

        //如果是首次订阅，需要使用自然排序查询，获取第最后一次操作的操作时间戳。如果是续订阅直接读取记录的值赋值给queryTs即可
        FindIterable<Document> tsCursor = collection.find().sort(new BasicDBObject("$natural", -1)).limit(1);
        Document tsDoc = tsCursor.first();
        queryTs = (BsonTimestamp) tsDoc.get("ts");
        while (true) {
            try {
                //构建查询语句,查询大于当前查询时间戳queryTs的记录
                BasicDBObject query = new BasicDBObject("ts", new BasicDBObject("$gt", queryTs));
                MongoCursor<Document> docCursor = collection.find(query)
                        .cursorType(CursorType.TailableAwait) //没有数据时阻塞休眠
                        .noCursorTimeout(true) //防止服务器在不活动时间（10分钟）后使空闲的游标超时。
                        .oplogReplay(true) //结合query条件，获取增量数据，这个参数比较难懂，见：https://docs.mongodb.com/manual/reference/command/find/index.html
                        .maxAwaitTime(1, TimeUnit.SECONDS) //设置此操作在服务器上的最大等待执行时间
                        .iterator();
                while (docCursor.hasNext()) {
                    Document document = docCursor.next();
                    //更新查询时间戳
                    queryTs = (BsonTimestamp) document.get("ts");
                    //TODO 在这里接收到数据后通过订阅数据路由分发

                    String op = document.getString("op");
                    String database = document.getString("ns");
                    Document context = (Document) document.get("o");
                    Document where = null;
                    if (op.equals("u")) {
                        where = (Document) document.get("o2");
                        if (context != null) {
                            context = (Document) context.get("$set");
                        }
                    }
                    System.err.println("操作时间戳：" + queryTs.getTime());
                    System.err.println("操作类型：" + op);
                    System.err.println("数据库.集合：" + database);
                    System.err.println("更新条件：" + JSON.toJSONString(where));
                    System.err.println("文档内容：" + JSON.toJSONString(context));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
