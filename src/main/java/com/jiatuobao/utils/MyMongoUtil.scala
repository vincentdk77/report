package com.jiatuobao.utils

import com.mongodb.{MongoClient, MongoClientURI}

/**
  * Desc: 获取Jedis客户端的工具类
  */
object MyMongoUtil {
  //定义一个连接池对象
  private var mongoClient1:MongoClient = null
  private var mongoClient2:MongoClient = null

  //获取MongoClient客户端
  def getMongoShard1Client():MongoClient ={
    if(mongoClient1 == null){
      val prop = MyPropertiesUtil.load("config.properties")
      val url1 = prop.getProperty("mongo.shard1.url")
      println("url:"+url1)
      mongoClient1 = new MongoClient(new MongoClientURI(url1))
    }
    mongoClient1
  }

  def getMongoShard2Client():MongoClient ={
    if(mongoClient2 == null){
      val prop = MyPropertiesUtil.load("config.properties")
      val url2 = prop.getProperty("mongo.shard2.url")
      println("url:"+url2)
      mongoClient2 = new MongoClient(new MongoClientURI(url2))
    }
    mongoClient2
  }

  def main(args: Array[String]): Unit = {
    val mongoClient1 = getMongoShard1Client()
    val mongoClient2 = getMongoShard2Client()
    import scala.collection.JavaConversions._
    println("--------shard1----------")
    println(mongoClient1.listDatabaseNames().toList.foreach(println(_)))
    println("--------shard2----------")
    println(mongoClient2.listDatabaseNames().toList.foreach(println(_)))
  }
}
