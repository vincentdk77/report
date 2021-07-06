package com.jiatuobao.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

import java.util

/**
  * Desc: 维护偏移量的工具类
  */
object OffsetManagerUtil {

  //从Redis中获取偏移量
  // type:hash   key: offset:topic:groupId   field:partition   value: 偏移量
  def getOffset(topic:String,groupId:String): Map[TopicPartition,Long]={
    //获取客户端连接
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    //拼接操作redis的key     offset:topic:groupId
    var offsetKey = "crmReport:offset:" + topic + ":" + groupId

    //获取当前消费者组消费的主题  对应的分区以及偏移量
    val offsetMap: util.Map[String, String] = jedis.hgetAll(offsetKey)
    //关闭客户端
    jedis.close()

    //todo 将java的map转换为scala的map
    import scala.collection.JavaConverters._
    val oMap: Map[TopicPartition, Long] = offsetMap.asScala.map {
      case (partition, offset) => {
        println("读取分区偏移量：topic:"+topic+",partition:" + partition + ",offset:" + offset)
        //Map[TopicPartition,Long]
        (new TopicPartition(topic, partition.toInt), offset.toLong)
      }
    }.toMap //todo  将可变的map转成不可变的map
    oMap
  }


  //将偏移量信息保存到Redis中
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    //拼接redis中操作偏移量的key
    var offsetKey = "crmReport:offset:" + topic + ":" + groupId
    //定义java的map集合，用于存放每个分区对应的偏移量
    val offsetMap: util.HashMap[String, String] = new util.HashMap[String,String]()
    //对offsetRanges进行遍历，将数据封装offsetMap todo 数组Array转map
    for (offsetRange <- offsetRanges) {
      val partitionId: Int = offsetRange.partition
      val fromOffset: Long = offsetRange.fromOffset
      val untilOffset: Long = offsetRange.untilOffset
      offsetMap.put(partitionId.toString,untilOffset.toString)
      println("保存分区,topic:"+topic+",partitionId:" + partitionId + ",fromOffset:" + fromOffset + "----->untilOffset:" + untilOffset)
    }
    val jedis: Jedis = MyRedisUtil.getJedisClient()
    jedis.hmset(offsetKey,offsetMap)
    jedis.close()
  }

}