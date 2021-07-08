package com.jiatuobao.mongo

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.jiatuobao.mongo.MongoOpLogUtilAll.{OpLogTest, log, nsLists, queryTs}
import com.jiatuobao.utils.{Constant, MyKafkaSink, MyMongoUtil, MyRedisUtil}
import com.mongodb.client.{FindIterable, MongoCollection, MongoCursor, MongoIterable}
import com.mongodb.{BasicDBObject, CursorType, MongoClient}
import org.apache.commons.lang3.StringUtils
import org.bson.{BsonTimestamp, Document}
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer

object MongoOpLogUtilIncrementShard1 {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)
  private var queryTs:BsonTimestamp = null
  private var nsLists: ListBuffer[String] = ListBuffer()

  def main(args: Array[String]): Unit = {
    val jedis: Jedis = MyRedisUtil.getJedisClient
    val mongoClient: MongoClient = MyMongoUtil.getMongoShard1Client

    OpLogTest(jedis, mongoClient)
  }


  def OpLogTest(jedis: Jedis, mongoClient: MongoClient): Unit = {

    //获取命名空间list
    val collectionNames: MongoIterable[String] = mongoClient.getDatabase("crm").listCollectionNames
    import scala.collection.JavaConversions._
    for (collectionName <- collectionNames) {
      val ns: String = "crm." + collectionName
      if (StringUtils.isNotBlank(ns)) {
        nsLists.append(ns)
      }
    }
    nsLists = nsLists.filter(ns => {
      if (ns.startsWith("crm.saas_clue")
        || ns.startsWith("crm.saas_customer")
        || ns.startsWith("crm.saas_contact")
        || ns.startsWith("crm.saas_opport")
        || ns.startsWith("crm.saas_agreement")
        || ns.startsWith("crm.saas_record")) {
        //        if(ns.startsWith("crm.saas_opport.20664")){//测试单表
        true
      }else{
        false
      }
    })
    println("查询oplog的命名空间list:" + nsLists)

    //获取oplog时间戳
    val ts:String = jedis.get("mongo:ts");
    val opLogCollection: MongoCollection[Document] = mongoClient.getDatabase("local").getCollection("oplog.rs")
    if(StringUtils.isNotBlank(ts)){
//            queryTs = JSON.parseObject(ts, BsonTimestamp.class);
        val ts1: Document = JSON.parseObject(ts, classOf[Document])
        val value = ts1.getLong("value");
        queryTs = new BsonTimestamp(value);
        log.warn("redisTs:"+JSON.toJSONString(queryTs,SerializerFeature.DisableCircularReferenceDetect));
    }else{
        //如果是首次订阅，需要使用自然排序查询，获取最后一次操作的操作时间戳。如果是续订阅直接读取记录的值赋值给queryTs即可
        val tsCursor: FindIterable[Document] = opLogCollection.find.sort(new BasicDBObject("$natural", 1)).limit(1) //从第一条记录开始查
        val tsDoc: Document = tsCursor.first
        queryTs = tsDoc.get("ts").asInstanceOf[BsonTimestamp]
        log.warn("查询mongo最新ts:" + JSON.toJSONString(queryTs,SerializerFeature.DisableCircularReferenceDetect))
    }

    var i = 0L
    while (true) {
      //            try {
        //构建查询语句,查询大于当前查询时间戳queryTs的记录
      import scala.collection.JavaConverters._
      val query: BasicDBObject = new BasicDBObject("ts", new BasicDBObject("$gt", queryTs)).append("ns", new BasicDBObject("$in", nsLists.asJava))
      import scala.collection.JavaConverters._
        val opDocuments: MongoCursor[Document] = opLogCollection.find(query).limit(2000)//每次读2千条数据，防止内存崩了
              .cursorType(CursorType.TailableAwait) //没有数据时阻塞休眠
              .noCursorTimeout(true) //防止服务器在不活动时间（10分钟）后使空闲的游标超时。
              .oplogReplay(true) //结合query条件，获取增量数据，这个参数比较难懂，见：https://docs.mongodb.com/manual/reference/command/find/index.html
              .maxAwaitTime(1, TimeUnit.SECONDS) //设置此操作在服务器上的最大等待执行时间
              .iterator()
        if(opDocuments.hasNext){
            for (document: Document <- opDocuments) {
              var message = ""
              //TODO 在这里接收到数据后通过订阅数据路由分发
              val ns: String = document.getString("ns")
              i +=1
              log.warn("i="+ i)
              log.warn("oplog:" + JSON.toJSONString(document,SerializerFeature.DisableCircularReferenceDetect))
              val op: String = document.getString("op") // i、u、d
              val database: String = ns.substring(0, ns.indexOf("."))
              val realTableName: String = ns.substring(ns.indexOf(".") + 1)
              val sendTableName: String = ns.substring(ns.indexOf(".") + 1, ns.lastIndexOf("."))
              var context: Document = document.get("o").asInstanceOf[Document] //文档内容
              var where: Document = null

              //处理新增
              if (op == "i") {
                message = resembleJsonFields(context, database, realTableName, "insert")
                log.warn("insert: realTableName=" + realTableName + ":" + message)
                MyKafkaSink.send(Constant.report_maxwell_topic, message)
              }

              //处理修改
              if (op == "u") {
                where = document.get("o2").asInstanceOf[Document] //更新操作-查询条件

                if (context != null) {
                  context = context.get("$set").asInstanceOf[Document]
                }

                val collection: MongoCollection[Document] = mongoClient.getDatabase(database).getCollection(realTableName)
                val documents: FindIterable[Document] = collection.find(where)

                import scala.collection.JavaConversions._
                for (document1 <- documents) {
                  message = resembleJsonFields(document1, database, realTableName, "update")
                  log.warn("update: realTableName=" + realTableName + ":" + message)
                  MyKafkaSink.send(Constant.report_maxwell_topic, message)
                }
              }
              //处理删除
              if (op == "d") {
                where = context
                val testCollection: MongoCollection[Document] = mongoClient.getDatabase(database).getCollection(realTableName)
                val documents: FindIterable[Document] = testCollection.find(where)

                import scala.collection.JavaConversions._
                for (document1 <- documents) {
                  message = resembleJsonFields(document1, database, realTableName, "delete")
                  log.warn("delete: realTableName=" + realTableName + ":" + message)
                  MyKafkaSink.send(Constant.report_maxwell_topic, message)
                }
              }

              //更新时间戳
              queryTs = document.get("ts").asInstanceOf[BsonTimestamp]
              //保存时间戳
              jedis.set("mongo:ts", JSON.toJSONString(queryTs,SerializerFeature.DisableCircularReferenceDetect))
              log.warn("ts:" + queryTs)
              //                        jedis.close();
              //                        log.warn("操作时间戳：" + queryTs.getTime());
              //                        log.warn("操作类型：" + op);
              //                        log.warn("数据库.集合：" + ns);
              //                        if (op.equals("u")) {
              //                            log.warn("更新条件：" + where.getObjectId("_id").toHexString());
              //                        }
              ////                        log.warn("更新条件：" + JSON.toJSONString(where,SerializerFeature.DisableCircularReferenceDetect));
              //                        log.warn("文档内容：" + JSON.toJSONString(context,SerializerFeature.DisableCircularReferenceDetect));

            }
        }else {
          log.warn("没有变更数据，休眠10s")
          Thread.sleep(10 * 1000)
        }

    }
  }

  private def resembleJsonFields(document1: Document, database: String, tableName: String, op: String) = {
    val jsonObj = new JSONObject
    document1.remove("_id")//去掉_id,ES不能有这个字段!
    jsonObj.put("data", document1)
    jsonObj.put("database", database)
    jsonObj.put("table", tableName)
    jsonObj.put("type", op)
    JSON.toJSONString(jsonObj,SerializerFeature.DisableCircularReferenceDetect)
  }


}
