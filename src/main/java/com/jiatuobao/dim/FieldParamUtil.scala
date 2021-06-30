//package com.jiatuobao.dim
//
//import com.alibaba.fastjson.serializer.SerializerFeature
//import com.alibaba.fastjson.{JSON, JSONObject}
//import com.jiatuobao.util.{Constant, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.TopicPartition
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
//import redis.clients.jedis.Jedis
//
//object FieldParamUtil {
//
//  def process(ssc: StreamingContext, topic: String, groupId: String,tableName:String) = {
//    //从redis读取offset
//    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)
//
//    var recordStream: InputDStream[ConsumerRecord[String, String]] = null
//    if(offsetMap!= null && offsetMap.size>0){
//      recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
//    }else{
//      recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
//    }
//
//    //获取offsetRange对象，方便最后存储offset
//    var offsetRanges = Array[OffsetRange]()
//    val offsetStream = recordStream.transform(rdd => {
//      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
//      rdd
//    })
//
//    //map转换
//    val jsonObjStream = offsetStream.map(record => {
//      var jsonObj:JSONObject = null
//      val value = record.value()
//      if(value != null){
//        jsonObj = JSON.parseObject(value)
//      }
//      jsonObj
//    })
//
//    //存入redis中
//    jsonObjStream.foreachRDD(rdd =>{
//      //driver端周期执行
//      rdd.foreachPartition(iter =>{
//        //executor端多次执行
//        val jedis: Jedis = MyRedisUtil.getJedisClient()
//        val list: List[JSONObject] = iter.toList
//        for (json <- list) {
////          val id = json.getInteger(Constant.id)
//          val tenant_id = json.getInteger(Constant.tenant_id)
//          val field_id = json.getInteger(Constant.field_id)
//          val isdel = json.getInteger(Constant.isdel)
//          val field_key = json.getString(Constant.field_key)
//          val jsonStr: String = JSON.toJSONString(json,SerializerFeature.DisableCircularReferenceDetect)
//          println(jsonStr)
//          jedis.hset("crmReport:dim:"+tableName,  tenant_id+":"+field_id  ,  field_key)
//          jedis.hset("crmReport:dim:"+tableName,  tenant_id+":"+field_id+":"  ,  jsonStr)
//        }
//        jedis.close()
//      })
//
//      //保存offset(driver端周期执行)
//      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
//    })
//  }
//
//}
