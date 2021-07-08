package com.jiatuobao.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.alibaba.fastjson.serializer.SerializerFeature
import com.google.common.collect.Sets
import com.jiatuobao.dim.ProcessUtil
import com.jiatuobao.entity.SysUser2
import com.jiatuobao.utils.{Constant, MyESUtil, MyKafkaUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.commons.lang.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.client.RestHighLevelClient
import org.slf4j.{Logger, LoggerFactory}
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SaasClueApp {
  private val log: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val tableName = Constant.saas_clue
    val topic = "ods_" + tableName
    val groupId = topic + Constant.group

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

      //参数维度
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

      //公海维度
      val publicSeaMap: util.Map[String, String] = jedis.hgetAll("crmReport:dim:" + Constant.clue_saas_customer_public_sea)

      //用户维度
      val userMap: util.Map[String, String] = jedis.hgetAll("crmReport:dim:" + Constant.sys_user2)

      for (jsonObj <- jsonList) {
        val tenantId: Integer = jsonObj.getInteger(Constant.tenantId)
        val creatorId: Integer = jsonObj.getInteger(Constant.creatorId)
        val lastCharger: Integer = jsonObj.getInteger(Constant.lastCharger)
        val belongToId: Integer = jsonObj.getInteger(Constant.belongToId)

        //组装各种fieldParam参数
        for (fieldKey <- paramKeySet.asScala) {
          if (jsonObj.containsKey(fieldKey)) { //本条数据包含参数字段
//            println("包含该字段!!"+fieldKey)
            val fieldValue: String = jsonObj.getString(fieldKey)
            if(StringUtils.isNotBlank(fieldValue)){
              val display = paramMapMap.getOrElse(tenantId + ":" + fieldKey + ":" + fieldValue, "")
//              println("key:"+tenantId + ":" + fieldKey + ":" + fieldValue)
//              println("display:"+display)
              if(StringUtils.isNotBlank(display)){
                jsonObj.put(fieldKey + "Name", display)
              }
            }
          }
        }

        //组装公海字段
        val publicSeaId: String = jsonObj.getString(Constant.publicSeaId)
        if(StringUtils.isNotBlank(publicSeaId)){
          val seaJsonStr: String = publicSeaMap.getOrDefault(publicSeaId, "")
          if(StringUtils.isNotBlank(seaJsonStr)){
            val seaJsonObj: JSONObject = JSON.parseObject(seaJsonStr)
            if(seaJsonObj!= null){
              val public_seas_name: String = seaJsonObj.getString(Constant.public_seas_name)
              if(StringUtils.isNotBlank(public_seas_name)){
                jsonObj.put(Constant.publicSeasName, public_seas_name)
              }
            }
          }
        }

        //组装用户维度
        if(creatorId!= null){
          val creatorIdJsonStr: String = userMap.getOrDefault(creatorId+"", "")
          if(StringUtils.isNotBlank(creatorIdJsonStr)){
            val user: SysUser2 = JSON.parseObject(creatorIdJsonStr, classOf[SysUser2])
            if(user != null){
              jsonObj.put(Constant.creatorId+"Name",user.realName)
            }
          }
        }

        if(lastCharger!= null){
          val lastChargerJsonStr: String = userMap.getOrDefault(lastCharger+"", "")
          if(StringUtils.isNotBlank(lastChargerJsonStr)){
            val user: SysUser2 = JSON.parseObject(lastChargerJsonStr, classOf[SysUser2])
            if(user != null){
              jsonObj.put(Constant.lastCharger+"Name",user.realName)
            }
          }
        }

        if(belongToId!= null){
          val belongToIdJsonStr: String = userMap.getOrDefault(belongToId+"", "")
          if(StringUtils.isNotBlank(belongToIdJsonStr)){
            val user: SysUser2 = JSON.parseObject(belongToIdJsonStr, classOf[SysUser2])
            if(user != null){
              jsonObj.put(Constant.belongToId+"Name",user.realName)
            }
          }
        }
      }

      jedis.close()
      jsonList.toIterator
    })

    //存入redis、ES中
    resultJsonStream.foreachRDD(rdd =>{
      //driver端周期执行
      rdd.foreachPartition(iter =>{
        //executor端多次执行
//        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val highLevelClient: RestHighLevelClient = MyESUtil.getClient()
        val list: List[JSONObject] = iter.toList
        val esList: ListBuffer[JSONObject] = ListBuffer()
        val redisList: ListBuffer[JSONObject] = ListBuffer()

        for (json <- list) {
          val id = json.getString("_id")
          val jsonStr: String = JSON.toJSONString(json,SerializerFeature.DisableCircularReferenceDetect)
          log.warn("topic: "+topic+", redisKey: crmReport:dwd:"+tableName+", json: "+jsonStr)
//          jedis.hset("crmReport:dwd:"+tableName,  id+""  ,  jsonStr)
//          jedis.hmset("crmReport:dwd:"+tableName,json)

          esList.append(json)
          if(esList.size ==1000){
            MyESUtil.bulkInsert(highLevelClient,tableName,esList)
            esList.clear()
          }
        }

        if(esList.size>0){
          MyESUtil.bulkInsert(highLevelClient,tableName,esList)
        }


//        jedis.close()
        highLevelClient.close()
      })

      //保存offset(driver端周期执行)
      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
