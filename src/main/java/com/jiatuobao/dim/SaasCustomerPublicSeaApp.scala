package com.jiatuobao.dim

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaasCustomerPublicSeaApp {

  def main(args: Array[String]): Unit = {
    val tableName = "saas_customer_public_sea"
    val topic = "ods_"+tableName
    val groupId = "ods_"+tableName+"_group1"

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("SaasCustomerPublicSeaApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    ProcessUtil.process(ssc,topic,groupId,tableName)

    ssc.start()
    ssc.awaitTermination()
  }

}
