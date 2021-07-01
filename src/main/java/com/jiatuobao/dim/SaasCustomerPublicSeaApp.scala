package com.jiatuobao.dim

import com.jiatuobao.util.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaasCustomerPublicSeaApp {

  def main(args: Array[String]): Unit = {
    val tableName = "saas_customer_public_sea"
    val topic = "ods_" + tableName
    val groupId = topic + Constant.group

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SaasCustomerPublicSeaApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    new ProcessUtil().process(ssc,topic,groupId,tableName)

    ssc.start()
    ssc.awaitTermination()
  }

}
