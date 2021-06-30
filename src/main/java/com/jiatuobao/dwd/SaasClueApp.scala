package com.jiatuobao.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.google.common.collect.Sets
import com.jiatuobao.dim.ProcessUtil
import com.jiatuobao.util.{Constant, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable

object SaasClueApp {

  def main(args: Array[String]): Unit = {
    val tableName = Constant.saas_clue
    val topic = "ods_"+tableName
    val groupId = "ods_"+tableName+"_group"

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SaasClueApp")
    val ssc = new StreamingContext(conf, Seconds(5))

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
      }
      jsonObj
    })

//    import collection.JavaConverters._
//    val jedis: Jedis = MyRedisUtil.getJedisClient()
//    val fieldMap: util.Map[String, String] = jedis.hgetAll("crmReport:dim:" + Constant.clue_saas_crm_customer_field)
//    //      val fieldList: List[String] = fieldMap.asScala.map(_._2).toList
//    val fieldMapMap: mutable.Map[String, String] = fieldMap.asScala.map(item => {
//      val nObject: JSONObject = JSON.parseObject(item._2)
//      val tenant_id: String = nObject.getString(Constant.tenant_id)
//      val field_id: String = nObject.getString(Constant.field_id)
//      val field_key: String = nObject.getString(Constant.field_key)
//      (tenant_id + ":" + field_id, field_key)
//    })
////    println("fieldMap:"+fieldMap)
////    println("fieldMapMap:"+fieldMapMap)
//
//    val paramKeySet: util.HashSet[String] = Sets.newHashSet()
//    val paramMap: util.Map[String, String] = jedis.hgetAll("crmReport:dim:" + Constant.clue_saas_crm_customer_param_field)
//
//    val paramMapMap: mutable.Map[String, String] = paramMap.asScala.map(item => {
//      val nObject: JSONObject = JSON.parseObject(item._2)
//      val tenant_id: String = nObject.getString(Constant.tenant_id)
//      val field_id: String = nObject.getString(Constant.field_id)
//      val param_id: String = nObject.getString(Constant.param_id)
//      val display: String = nObject.getString(Constant.display)
//      val field_key: String = fieldMapMap.getOrElse(tenant_id + ":" + field_id, null)
//      paramKeySet.add(tenant_id + ":" + field_key)
//      (tenant_id + ":" + field_key + ":" + param_id, display)
//    })
//
////    println("paramMap:"+paramMap)
////    println("paramMapMap:"+paramMapMap)
//    println("paramKeySet:"+paramKeySet)

//    val params: Broadcast[util.HashSet[String]] = ssc.sparkContext.broadcast(paramKeySet)

    //关联 字段维度与字段参数维度
    val resultJsonStream: DStream[JSONObject] = jsonObjStream.mapPartitions(iter => {
      val jsonList: List[JSONObject] = iter.toList
      val paramKeySet: util.HashSet[String] = Sets.newHashSet()

      val jedis: Jedis = MyRedisUtil.getJedisClient()

      import collection.JavaConverters._
      val fieldMap: util.Map[String, String] = jedis.hgetAll("crmReport:dim:" + Constant.clue_saas_crm_customer_field)
      val fieldMapMap: mutable.Map[String, String] = fieldMap.asScala.map(item => {
        val nObject: JSONObject = JSON.parseObject(item._2)
        val tenant_id: String = nObject.getString(Constant.tenant_id)
        val field_id: String = nObject.getString(Constant.field_id)
        val field_key: String = nObject.getString(Constant.field_key)
        (tenant_id + ":" + field_id, field_key)
      })

      val paramMap: util.Map[String, String] = jedis.hgetAll("crmReport:dim:" + Constant.clue_saas_crm_customer_param_field)
      val paramMapMap: mutable.Map[String, String] = paramMap.asScala.map(item => {
        val nObject: JSONObject = JSON.parseObject(item._2)
        val tenant_id: String = nObject.getString(Constant.tenant_id)
        val field_id: String = nObject.getString(Constant.field_id)
        val param_id: String = nObject.getString(Constant.param_id)
        val display: String = nObject.getString(Constant.display)
        val field_key: String = fieldMapMap.getOrElse(tenant_id + ":" + field_id, null)
        paramKeySet.add(field_key)
        (tenant_id + ":" + field_key + ":" + param_id, display)
      })

      println("paramMapMap：" + paramMapMap)
      println("paramKeySet：" + paramKeySet)

      val publicSeaMap: util.Map[String, String] = jedis.hgetAll("crmReport:dim:" + Constant.clue_saas_customer_public_sea)

      for (jsonObj <- jsonList) {
        val tenantId: String = jsonObj.getString(Constant.tenantId)

        //组装各种fieldParam参数
        for (fieldKey <- paramKeySet.asScala) {
          if (jsonObj.containsKey(fieldKey)) { //本条数据包含参数字段
            println("包含该字段!!"+fieldKey)
            val fieldValue: String = jsonObj.getString(fieldKey)
            val display = paramMapMap.getOrElse(tenantId + ":" + fieldKey + ":" + fieldValue, null)
            println("key:"+tenantId + ":" + fieldKey + ":" + fieldValue)
            println("display:"+display)
            jsonObj.put(fieldKey + "Name", display)
          }
        }

        //组装公海字段
        val publicSeaId: String = jsonObj.getString(Constant.publicSeaId)
        if(StringUtils.isNotBlank(publicSeaId)){
          val seaJsonStr: String = publicSeaMap.getOrDefault(publicSeaId, "")
          if(StringUtils.isNotBlank(seaJsonStr)){
            val seaJsonObj: JSONObject = JSON.parseObject(seaJsonStr)
            val public_seas_name: String = seaJsonObj.getString(Constant.public_seas_name)
            if(StringUtils.isNotBlank(public_seas_name)){
              jsonObj.put(Constant.publicSeasName, public_seas_name)
            }
          }
        }



      }

      jedis.close()
      jsonList.toIterator
    })

    //存入redis中
    resultJsonStream.foreachRDD(rdd =>{
      //driver端周期执行
      rdd.foreachPartition(iter =>{
        //executor端多次执行
        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val list: List[JSONObject] = iter.toList
        for (json <- list) {
          val id = json.getJSONObject("_id").getString("$oid")
          val jsonStr: String = JSON.toJSONString(json,SerializerFeature.DisableCircularReferenceDetect)
          println(jsonStr)
          jedis.hset("crmReport:dwd:"+tableName,  id+""  ,  jsonStr)
        }
        jedis.close()
      })

      //保存offset(driver端周期执行)
      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
