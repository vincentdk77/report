package com.jiatuobao.util

object Constant {

  final val report_maxwell_topic = "report_maxwell"

  final val group = "_group2"

  //tableNames
  final val clue_saas_crm_customer_field = "clue_saas_crm_customer_field"
  final val clue_saas_crm_customer_param_field = "clue_saas_crm_customer_param_field"
  final val clue_saas_customer_public_sea = "clue_saas_customer_public_sea"
  final val saas_customer_public_sea = "saas_customer_public_sea"
  final val saas_clue = "saas_clue"
  final val saas_customer = "saas_customer"
  final val sys_user2 = "sys_user2"


  //字段名
  final val id = "id"
  final val tenant_id = "tenant_id"
  final val field_id = "field_id"
  final val field_key = "field_key"
  final val param_id = "param_id"
  final val display = "display"
  final val status = "status"
  final val isdel = "isdel"
  final val public_seas_name = "public_seas_name"


  final val tenantId = "tenantId"
  final val publicSeaId = "publicSeaId"
  final val publicSeasName = "publicSeasName"
  final val creatorId = "creatorId"
  final val lastCharger = "lastCharger"
  final val belongToId = "belongToId"

  def main(args: Array[String]): Unit = {
    val tableName = "saas_clue.20664"
    val str: String = tableName.substring(0, tableName.lastIndexOf("."))
    println(str)
  }

}
