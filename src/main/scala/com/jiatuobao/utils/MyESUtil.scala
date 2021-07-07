package com.jiatuobao.utils

import com.alibaba.fastjson.serializer.{SerializeConfig, SerializerFeature}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.action.DocWriteRequest.OpType
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkRequest, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.ml.PostDataRequest
import org.elasticsearch.client.{HttpAsyncResponseConsumerFactory, RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.cluster.metadata.MetaDataIndexTemplateService.PutRequest
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder
import org.slf4j.{Logger, LoggerFactory}

import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer
/**
  * Desc: 操作ES的客户端工具类
  */
object MyESUtil {
  private val ALI_ES_HOST = "es-cn-6ja1ycolz00179md6.public.elasticsearch.aliyuncs.com" //old
  private val ALI_ES_PORT = 9200
  private val SCHEMA = "http"
  private val ALI_ES_USERNAME = "elastic"
  private val ALI_ES_PASSWORD = "gitbo@1212"
  private val format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
//  private var client: RestHighLevelClient = null
  private var COMMON_OPTIONS: RequestOptions = null
  private var clientBuilder: RestClientBuilder = null

  /**
   * 提供获取RestHighLevelClient客户端的方法
   */
  def getClient(): RestHighLevelClient ={
    if(clientBuilder == null){
      //创建RestHighLevelClient客户端对象
      build()
    }
    new RestHighLevelClient(clientBuilder)
  }

  /**
   * 获取clientBuilder对象和 RequestOptions
   */
  def build(): Unit = {

    val builder: RequestOptions.Builder = RequestOptions.DEFAULT.toBuilder

    // 默认缓存限制为100MB
    builder.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(500 * 1024 * 1024))
    COMMON_OPTIONS = builder.build

    val credentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ALI_ES_USERNAME, ALI_ES_PASSWORD)) // 密码凭证

    clientBuilder =
      RestClient.builder(new HttpHost(ALI_ES_HOST, ALI_ES_PORT, SCHEMA)).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {

        override def customizeRequestConfig(builder: RequestConfig.Builder): RequestConfig.Builder = {
            builder.setConnectTimeout(5 * 60 * 1000) // 连接建立时间，三次握手完成时间（单位：毫秒）

            builder.setSocketTimeout(10 * 60 * 1000) // 数据传输过程中数据包之间间隔的最大时间（单位：毫秒）

            builder.setConnectionRequestTimeout(30 * 1000) // 从连接池中获取可用连接超时（单位：毫秒）

            builder
        }
      }).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
          override def customizeHttpClient(httpAsyncClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
            httpAsyncClientBuilder.setMaxConnTotal(16) // 连接池中的最大连接数

            httpAsyncClientBuilder.setMaxConnPerRoute(16) // 连接同一个route最大的并发数

            httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            httpAsyncClientBuilder
          }
      })
  }

  /**
    * 向ES中批量插入数据
    * @param infoList
    * @param indexName
    */
  def bulkInsert(highLevelClient:RestHighLevelClient, indexName: String, infoList: ListBuffer[JSONObject]): Unit = {

    if(infoList!=null && infoList.size!= 0){
      //获取客户端
//      val highLevelClient: RestHighLevelClient = getClient()
      val bulkRequest = new BulkRequest()

      for (jsonObj <- infoList) {
        val indexRequest: IndexRequest = new IndexRequest(indexName)
        indexRequest.id(jsonObj.getString(Constant.id))
        indexRequest.source(jsonObj, XContentType.JSON)
        indexRequest.opType(OpType.INDEX)//index类型：id相同自动覆盖
        bulkRequest.add(indexRequest)
      }

      var response: BulkResponse = null
      try {
        response = highLevelClient.bulk(bulkRequest, COMMON_OPTIONS)

        if(response.hasFailures){
          for (bulkItemResponse: BulkItemResponse <- response.getItems.toStream) {
            println("ES错误信息:"+bulkItemResponse.getFailureMessage)
          }
        }else{
          println("向ES中插入"+response.getItems.size+"条数据")
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()

        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
//    val jsonStr = "{\"excludes\":[],\"lastFollowUpTime\":\"2021-06-02 21:30:28\",\"chargerChangeTime\":\"2021-06-11 10:39:08\",\"companyName\":{},\"creatorId\":1214,\"remark\":\"444\",\"platform\":\"crm\",\"chargerChangeCount\":1,\"lastCharger\":1214,\"robotCallCount\":0,\"returnToPoolCount\":0,\"id\":\"60b787f4e4b0683b8394d840\",\"emailTouchCount\":0,\"agentCallCount\":0,\"unFollowUpDays\":0,\"followStatus\":11,\"mobile\":\"18855999227\",\"updateTime\":\"2021-07-07 11:54:13\",\"belongToId\":1000,\"createTime\":\"2021-06-02 21:30:28\",\"name\":\"孙硕\",\"tenantId\":2002,\"_id\":\"60b787f4e4b0683b8394d840\",\"isDel\":false,\"marketingTouchCount\":0}"
//    val infoList: ListBuffer[JSONObject] = ListBuffer()
//    val nObject: JSONObject = JSON.parseObject(jsonStr)
//    infoList.append(nObject)
//
//    val indexName = "saas_clue"
//    val client: RestHighLevelClient = getClient()
//    bulkInsert(client,indexName, infoList)
//    client.close()

  }
}

