package com.jiatuobao.util

object Constant {
  //tableNames
  final val clue_saas_crm_customer_field = "clue_saas_crm_customer_field"
  final val clue_saas_crm_customer_param_field = "clue_saas_crm_customer_param_field"
  final val clue_saas_customer_public_sea = "clue_saas_customer_public_sea"
  final val saas_customer_public_sea = "saas_customer_public_sea"
  final val saas_clue = "saas_clue"
  final val saas_customer = "saas_customer"




  //字段名
  final val id = "id"
  final val tenant_id = "tenant_id"
  final val field_id = "field_id"
  final val field_key = "field_key"
  final val param_id = "param_id"
  final val display = "display"
  final val status = "status"
  final val isdel = "isdel"
  final val publicSeasName = "publicSeasName"

  final val tenantId = "tenantId"
  final val publicSeaId = "publicSeaId"
  final val public_seas_name = "public_seas_name"

  def main(args: Array[String]): Unit = {
    val tableName = "saas_clue.20664"
    val str: String = tableName.substring(0, tableName.lastIndexOf("."))
    println(str)
  }

}
