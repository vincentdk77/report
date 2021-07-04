package com.jiatuobao.ods

import com.alibaba.fastjson.{JSON, JSONObject}
import com.google.common.collect.Lists
import com.jiatuobao.util.{Constant, MyKafkaSink, MyKafkaUtil, OffsetManagerUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

/**
 * 数据格式:
 *     {
        "database": "gmall-2020-04",
        "table": "z_user_info",
        "type": "update",
        "ts": 1589385508,
        "xid": 83206,
        "xoffset": 0,
        "data": {
          "id": 30,
          "user_name": "wang55",
          "tel": "13810001010"
        },
        "old": {
          "user_name": "zhang3"
        }
      }
  * Desc: 从Kafka中读取数据，根据表名进行分流处理（maxwell）
  */
object BaseDBMaxwellApp {

  private val tableNames: util.ArrayList[String] = Lists.newArrayList(
    "sys_user2",//用户表
    "t_country", "t_province", "t_city", "t_area",//地区表
    "clue_saas_customer_public_sea", "saas_customer_public_sea",//线索、客户公海表
    "clue_saas_crm_customer_field", "clue_saas_crm_customer_param_field",//线索字段表、线索字段参数表
    "user"

  )

  private val mongoTableNames: util.ArrayList[String] = Lists.newArrayList(
    "saas_clue", "saas_customer"//线索表、客户表
  )



  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("BaseDBMaxwellApp").setMaster("local[4]")
    val ssc = new StreamingContext(conf,Seconds(5))

    var topic = Constant.report_maxwell_topic
    var groupId = topic+Constant.group

    //从Redis中获取偏移量
    val offsetMap: Map[TopicPartition, Long] = OffsetManagerUtil.getOffset(topic,groupId)

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
        println("driver端，周期性执行")
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    }

    //对读取的数据进行结构的转换   ConsumerRecord<K,V> ==>V(jsonStr)==>V(jsonObj)
    val jsonObjDStream: DStream[JSONObject] = offsetDStream.map {
      record => {
        val jsonStr: String = record.value()
//        println("----"+jsonStr)
        //将json字符串转换为json对象
        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
        jsonObj
      }
    }.filter(obj =>obj != null)

    //分流
    jsonObjDStream.foreachRDD{
      rdd=>{
//        if(rdd.count()>0){
          rdd.foreach{
            jsonObj=>{
              //获取操作类型
              val opType: String = jsonObj.getString("type")
              //获取操作的数据
              val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
              //获取表名
              var tableName: String = jsonObj.getString("table")

//              println(jsonObj.toJSONString)

              if(dataJsonObj!=null && !dataJsonObj.isEmpty ){
                var sendTopic = ""
                if(tableNames.contains(tableName)){
                  //拼接要发送到的主题
                  sendTopic = "ods_" + tableName

                  println(sendTopic+":"+dataJsonObj.toString)
                  MyKafkaSink.send(sendTopic,dataJsonObj.toString)

                }else if(tableName.indexOf(".")> -1){
                  val tableName1 = tableName.substring(0, tableName.lastIndexOf("."))
                  val tenantId = tableName.substring(tableName.lastIndexOf(".") + 1).toInt

                  sendTopic = "ods_" + tableName1
                  dataJsonObj.put(Constant.tenantId,tenantId)
                  println(sendTopic+":"+dataJsonObj.toString)
                  MyKafkaSink.send(sendTopic,dataJsonObj.toString)
                }


              }
            }
          }
          //手动提交偏移量
          OffsetManagerUtil.saveOffset(topic,groupId,offsetRanges)
        }

//      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

}
