package com.jiatuobao.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.google.common.collect.{Lists, Maps}
import com.jiatuobao.util.{MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

class ProcessUtil {

  def process(ssc: StreamingContext, topic: String, groupId: String,tableName:String) = {
    //从redis读取offset
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!= null && offsetMap.size>0){
      recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, offsetMap, groupId)
    }else{
      recordStream = MyKafkaUtil.getKafkaStream(topic, ssc, groupId)
    }

    //获取offsetRange对象，方便最后存储offset
    var offsetRanges = Array[OffsetRange]()
    val offsetStream = recordStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    //map转换
    val jsonObjStream = offsetStream.map(record => {
      var jsonObj:JSONObject = null
      val value = record.value()
      if(value != null){
        jsonObj = JSON.parseObject(value)

//        //根据tenantId查出
//        val map = //字段名称map
//        for(item <- map){
//          if(jsonObj.containsKey("key")){
//
//            val value1 = map.getOrDefault("key", "")
//            jsonObj.put("key"+"Name",value1)
//          }
//        }


      }
      jsonObj
    })

    //存入redis中
    jsonObjStream.foreachRDD(rdd =>{
      //driver端周期执行
      rdd.foreachPartition(iter =>{
        //executor端多次执行
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val list: List[JSONObject] = iter.toList
        for (json <- list) {
          val id = json.getInteger("id")
          val jsonStr: String = JSON.toJSONString(json,SerializerFeature.DisableCircularReferenceDetect)
          println("topic: "+topic+", redisKey: crmReport:dwd:"+tableName+", json: "+jsonStr)
          jedis.hset("crmReport:dim:"+tableName,  id+""  ,  jsonStr)
        }
        jedis.close()
      })

      //保存offset(driver端周期执行)
      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
    })
  }

}
