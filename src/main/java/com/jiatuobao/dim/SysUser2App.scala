package com.jiatuobao.dim

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.jiatuobao.entity.SysUser2
import com.jiatuobao.util.{MyKafkaUtil, MyPropertiesUtil, MyRedisUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf, streaming}
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

/**
  * Desc: 从Kafka中读取数据，保存到Phoenix
  */
object SysUser2App {

  def main(args: Array[String]): Unit = {
    //1、从kafka读数据（1、从redis获取offset，2、转换类型，3、存入redis中或者hbase中）
    val topic = "test"

    val groupId = "userInfoGroup"

    val conf = new SparkConf().setMaster("local[4]").setAppName("UserInfoApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    //只在刚开始执行时，执行一次，不会周期执行
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    var recordDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offsetMap!=null && offsetMap.size >0){
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,offsetMap,groupId)
    }else{
      recordDStream = MyKafkaUtil.getKafkaStream(topic,ssc,groupId)
    }

    //获取当前采集周期中读取的主题对应的分区以及偏移量
    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]
    println("driver端，执行一次")
    val offsetDStream: DStream[ConsumerRecord[String, String]] = recordDStream.transform {
      rdd => {
        //在driver端周期性执行
        println("driver端，周期性执行")
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    
    //map转换类型
    val user2Stream: DStream[SysUser2] = offsetDStream.map(record => {
      val value: String = record.value()
      val user: SysUser2 = JSON.parseObject(value, classOf[SysUser2])
      user
    })

    //写入Redis或者Hbase
    user2Stream.foreachRDD(user2Rdd =>{

      user2Rdd.foreachPartition(iter =>{

        val jedis: Jedis = MyRedisUtil.getJedisClient()
        val userList: List[SysUser2] = iter.toList

        userList.foreach(user =>{
          /**
           * Error:(75, 31) ambiguous reference to overloaded definition,
           * both method toJSONString in object JSON of type (x$1: Any, x$2: com.alibaba.fastjson.serializer.SerializerFeature*)String
           * and  method toJSONString in object JSON of type (x$1: Any)String
           * match argument types (cn.itcast.shop.realtime.etl.bean.DimGoodsDBEntity) and expected result type String
           * val json: String = JSON.toJSONString(goodsDBEntity)
           */
          val json: String = JSON.toJSONString(user, SerializerFeature.DisableCircularReferenceDetect)

          jedis.hset("crmReportDim:sysUser2",user.id,json)
        })
        jedis.close()

      })

      //提交offset
      OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges);
    })




    ssc.start()
    ssc.awaitTermination()
  }
}
