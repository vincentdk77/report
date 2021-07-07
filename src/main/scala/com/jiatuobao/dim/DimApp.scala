package com.jiatuobao.dim

import com.jiatuobao.utils.Constant
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DimApp {
  val clue_saas_crm_customer_field = Constant.clue_saas_crm_customer_field
  val clue_saas_crm_customer_param_field = Constant.clue_saas_crm_customer_param_field
  val clue_saas_customer_public_sea = Constant.clue_saas_customer_public_sea
  val saas_customer_public_sea = Constant.saas_customer_public_sea

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("ClueSaasCrmCustomerFieldApp")
    val ssc = new StreamingContext(conf, Seconds(5))

    //线索字段维度表
    new ProcessUtil().process(ssc,
      "ods_"+clue_saas_crm_customer_field,
      "ods_"+clue_saas_crm_customer_field+Constant.group,
      clue_saas_crm_customer_field)

    //线索字段参数维度表
    new ProcessUtil().process(ssc,
      "ods_"+clue_saas_crm_customer_param_field,
      "ods_"+clue_saas_crm_customer_param_field+Constant.group,
      clue_saas_crm_customer_param_field)

    //线索公海维度表
    new ProcessUtil().process(ssc,
      "ods_"+clue_saas_customer_public_sea,
      "ods_"+clue_saas_customer_public_sea+Constant.group,
      clue_saas_customer_public_sea)

    //客户公海维度表
    new ProcessUtil().process(ssc,
      "ods_"+saas_customer_public_sea,
      "ods_"+saas_customer_public_sea+Constant.group,
      saas_customer_public_sea)

    ssc.start()
    ssc.awaitTermination()
  }

}
