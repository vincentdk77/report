package com.jiatuobao.dim

import com.alibaba.fastjson.JSON
import com.jiatuobao.util.{MyKafkaUtil, MyPropertiesUtil, OffsetManagerUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.{SparkConf, streaming}

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

/**
  * Desc: 从Kafka中读取数据，保存到Phoenix
  */
object SysUser2App {
  private val properties: Properties = MyPropertiesUtil.load("config.properties")

  val broker_list = properties.getProperty("kafka.broker.list")
  var kafkaParam = collection.mutable.Map(
    "bootstrap.servers" -> broker_list,//用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "gmall2020_group",
    //latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "latest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  def main(args: Array[String]): Unit = {
    //1、从kafka读数据（1、从redis获取offset，2、转换类型，3、存入redis中或者hbase中）
    val topic = "test"

    val groupId = "userInfoGroup"


    val offsetMaps: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic, groupId)

    val conf = new SparkConf().setMaster("local[4]").setAppName("UserInfoApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    kafkaParam.put("group.id",groupId)
    val inputRdd: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsetMaps))

    inputRdd.map(item =>item.value()).print()

    inputRdd




    ssc.start()
    ssc.awaitTermination()
  }
}
