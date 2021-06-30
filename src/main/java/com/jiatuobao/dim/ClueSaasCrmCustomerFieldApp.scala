package com.jiatuobao.dim

import com.jiatuobao.util.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ClueSaasCrmCustomerFieldApp {

  def main(args: Array[String]): Unit = {
    val tableName = Constant.clue_saas_crm_customer_field
    val topic = "ods_"+tableName
    val groupId = "ods_"+tableName+"_group"

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ClueSaasCrmCustomerFieldApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    ProcessUtil.process(ssc,topic,groupId,tableName)

    ssc.start()
    ssc.awaitTermination()
  }

}
