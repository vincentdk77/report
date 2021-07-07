//package com.jiatuobao.utils;
//
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.google.common.collect.Maps;
//import org.apache.http.HttpHost;
//import org.apache.http.auth.AuthScope;
//import org.apache.http.auth.UsernamePasswordCredentials;
//import org.apache.http.client.CredentialsProvider;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.impl.client.BasicCredentialsProvider;
//import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
//import org.elasticsearch.action.DocWriteRequest;
//import org.elasticsearch.action.bulk.*;
//import org.elasticsearch.action.delete.DeleteRequest;
//import org.elasticsearch.action.get.GetRequest;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.update.UpdateRequest;
//import org.elasticsearch.client.*;
//import org.elasticsearch.common.unit.ByteSizeUnit;
//import org.elasticsearch.common.unit.ByteSizeValue;
//import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.common.xcontent.XContentType;
//import org.elasticsearch.index.query.BoolQueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.index.query.TermQueryBuilder;
//import org.elasticsearch.index.reindex.UpdateByQueryRequest;
//import org.elasticsearch.script.Script;
//import org.elasticsearch.script.ScriptType;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.net.ConnectException;
//import java.net.SocketTimeoutException;
//import java.text.SimpleDateFormat;
//import java.util.Collections;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicReference;
//
///**
// * Created by JarvisKwok
// * Date :2020/11/14
// * Description :
// */
//public class ElasticSearchUtil {
//
//    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUtil.class);
//
//    private static final String CRM_INDEX = "crm";
//    private static final String CRM_CUSTOMER_TYPE = "customer";
//
//    private static final String LOCAL_ES_HOST = "node11";
//    private static final int LOCAL_ES_PORT = 9999;
//
//    private static final String ALI_ES_HOST = "es-cn-6ja1ycolz00179md6.public.elasticsearch.aliyuncs.com";//old
//    private static final int ALI_ES_PORT = 9200;
//    private static final String SCHEMA = "http";
//    private static final String ALI_ES_USERNAME = "elastic";
//    private static final String ALI_ES_PASSWORD = "gitbo@1212";
//
//    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
//
//    public static RestHighLevelClient client;
//
//    private static final RequestOptions COMMON_OPTIONS;
//
//    static {
//        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
//
//        // 默认缓存限制为100MB
//        builder.setHttpAsyncResponseConsumerFactory(
//                new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(500 * 1024 * 1024));
//        COMMON_OPTIONS = builder.build();
//
//        // 1.本地es集群（node11-15）
////        client = new RestHighLevelClient(RestClient.builder(new HttpHost(LOCAL_ES_HOST, LOCAL_ES_PORT, SCHEMA)));
//
//        // 2.阿里云es集群
//        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(ALI_ES_USERNAME, ALI_ES_PASSWORD));   // 密码凭证
//
//        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(ALI_ES_HOST, ALI_ES_PORT, SCHEMA))
//                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
//                    @Override
//                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder builder) {
//                        builder.setConnectTimeout(5 * 60 * 1000);   // 连接建立时间，三次握手完成时间（单位：毫秒）
//                        builder.setSocketTimeout(10 * 60 * 1000);   // 数据传输过程中数据包之间间隔的最大时间（单位：毫秒）
//                        builder.setConnectionRequestTimeout(30 * 1000); // 从连接池中获取可用连接超时（单位：毫秒）
//                        return builder;
//                    }
//                })
//                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                    @Override
//                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
//                        httpAsyncClientBuilder.setMaxConnTotal(16);   // 连接池中的最大连接数
//                        httpAsyncClientBuilder.setMaxConnPerRoute(16);    // 连接同一个route最大的并发数
//                        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                        return httpAsyncClientBuilder;
//                    }
//                });
//
//        client = new RestHighLevelClient(clientBuilder);
//    }
//
//    // 根据主键id查询文档是否存在
//    public static boolean isExist(String indexName, String id) {
//        boolean isExist = false;
//        GetRequest getRequest = new GetRequest(indexName, id);
//        try {
//            isExist = client.exists(getRequest, COMMON_OPTIONS);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return isExist;
//    }
//
//    // 根据主键id删除文档
//    public static void delete(String indexName, String id){
//        DeleteRequest deleteRequest = new DeleteRequest(indexName, id);
//        try {
//            client.delete(deleteRequest,COMMON_OPTIONS);
//        } catch (Exception e) {
//            if ((e instanceof ConnectException) || (e instanceof SocketTimeoutException)) {
//                System.out.println("update: --------------------------------------------------");
//                System.out.println(format.format(System.currentTimeMillis()) + " " + e + " 更新连接超时，补偿一次!");
//
//                try {
//                    client.delete(deleteRequest,COMMON_OPTIONS);
//                } catch (Exception exception) {
//                    exception.printStackTrace();
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿失败，再次超时!");
//                } finally {
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿结束！");
//                }
//            }else {
//                e.printStackTrace();
//                System.out.println("其它异常：" + e);
//            }
//        }
//    }
//
//    // term方式查询文档
//    public static Map<String,String> termQueryByConditions(String indexName,BoolQueryBuilder boolQuery,Integer size,
//            String[] includes,String[] excludes) {
//
//        Map<String,String> idsMap = Maps.newHashMap();
//
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        sourceBuilder.fetchSource(includes, excludes);
//        sourceBuilder.query(boolQuery);
//        sourceBuilder.size(size);
//        searchRequest.source(sourceBuilder);
//
//        try {
//            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
//            SearchHit[] searchHits = response.getHits().getHits();
//
//            for (SearchHit hit : searchHits) {
//                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
//                if (sourceAsMap == null || !sourceAsMap.containsKey("entId")) {
//                    continue;
//                }
//
//                idsMap.put((String)sourceAsMap.get("entId"),hit.getId());
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return idsMap;
//    }
//
//    public static JSONObject termQuery(String indexName, JSONObject queryJson) {
//
//        // 1. 构建搜索请求对象
//        SearchRequest searchRequest = new SearchRequest(indexName);
//        // 2. 构建搜索源对象
//        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        // 3. term查询条件
//        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
//        for (String key : queryJson.keySet()) {
//            TermQueryBuilder termQuery = QueryBuilders.termQuery(key, queryJson.getString(key));
//            boolQuery.must(termQuery);
//        }
//        // 4. 向搜索请求对象中设置搜索源
//        sourceBuilder.query(boolQuery);
//        searchRequest.source(sourceBuilder);
//        // 5. 开始搜索，并获取结果对象
//        JSONObject result = null;
//        try {
//            SearchResponse searchResponse = client.search(searchRequest, COMMON_OPTIONS);
//            SearchHits hits = searchResponse.getHits();
//            SearchHit[] searchHits = hits.getHits();
//            if (searchHits != null && searchHits.length == 1) {
//                for (SearchHit hit : searchHits) {
//                    String sourceAsString = hit.getSourceAsString();
//                    result = JSONObject.parseObject(sourceAsString);
//                }
//            } else if (searchHits != null && searchHits.length > 1) {
//                System.out.println("企业文档重复！");
//            }
//        } catch (Exception e) {
//            if ((e instanceof ConnectException) || (e instanceof SocketTimeoutException)) {
//                System.out.println("termQuery: --------------------------------------------------");
//                System.out.println(format.format(System.currentTimeMillis()) + " " + e + " 查询连接超时，补偿一次!");
//
//                try {
//                    SearchResponse searchResponse = client.search(searchRequest, COMMON_OPTIONS);
//                    SearchHits hits = searchResponse.getHits();
//                    SearchHit[] searchHits = hits.getHits();
//                    if (searchHits != null && searchHits.length == 1) {
//                        for (SearchHit hit : searchHits) {
//                            String sourceAsString = hit.getSourceAsString();
//                            result = JSONObject.parseObject(sourceAsString);
//                        }
//                    } else if (searchHits != null && searchHits.length > 1) {
//                        System.out.println("企业文档重复！");
//                    }
//                } catch (Exception exception) {
//                    exception.printStackTrace();
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿失败，再次超时!");
//                }
//            }
//        }
//        return result;
//    }
//
//    // 根据主键id更新文档
//    public static void update(String tableName, String id, String json) {
//
//        UpdateRequest updateRequest = new UpdateRequest(tableName, id);
//        updateRequest.doc(json, XContentType.JSON);
//        try {
//            client.update(updateRequest, COMMON_OPTIONS);
//        } catch (Exception e) {
//            if ((e instanceof ConnectException) || (e instanceof SocketTimeoutException)) {
//                System.out.println("update: --------------------------------------------------");
//                System.out.println(format.format(System.currentTimeMillis()) + " " + e + " 更新连接超时，补偿一次!");
//
//                try {
//                    client.update(updateRequest, COMMON_OPTIONS);
//                } catch (Exception exception) {
//                    exception.printStackTrace();
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿失败，再次超时!");
//                    writeToFile(JSON.parseObject(json), "/home/bigdata/tmp/update.txt");
//                } finally {
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿结束！");
//                }
//            }else {
//                e.printStackTrace();
//                System.out.println("其它异常：" + e);
//                writeToFile(JSON.parseObject(json), "/home/bigdata/tmp/update.txt");
//            }
//        }
//    }
//
//    // 字段更新
//    public static void updateByField(String tableName, String entId, String field) {
//        GetRequest getRequest = new GetRequest(tableName, entId);
//        try {
//            boolean exists = client.exists(getRequest, COMMON_OPTIONS);
//            if (exists) {
//                UpdateByQueryRequest request = new UpdateByQueryRequest(tableName);
//                request.setQuery(new TermQueryBuilder("entId", entId))
//                        .setScript(new Script(ScriptType.INLINE,
//                                "painless",
//                                "ctx._source." + field,
//                                Collections.<String, Object>emptyMap()));
//                client.updateByQuery(request, COMMON_OPTIONS);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    // 新增字段更新
//    public static void updateBatchTenantIdsByQuery(String indexName, String _id, String fieldValue) {
////        GetRequest getRequest = new GetRequest(indexName, _id);
//        try {
////            boolean exists = client.exists(getRequest, COMMON_OPTIONS);
////            if (exists) {
//                UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);
//                request.setQuery(new TermQueryBuilder("_id", _id))
//                        .setScript(new Script(ScriptType.INLINE,
//                                "painless",
//                                "ctx._source.tenantId='" + fieldValue+"'",
//                                Collections.<String, Object>emptyMap()));
//                client.updateByQuery(request, COMMON_OPTIONS);
////            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public static void updateBatchTenantIdsByQuery(String indexName, String fieldValue) {
//        try {
//            UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);
//            request.setQuery(QueryBuilders.boolQuery().filter(QueryBuilders.existsQuery("tenantId")));
////            request.setScript(new Script(ScriptType.INLINE,
////                            "painless",
////                            "if (ctx._source.tenantId != '') {ctx._source.tenantId='" + fieldValue+"'}",
////                            Collections.<String, Object>emptyMap()));
//            request.setScript(new Script(ScriptType.INLINE,
//                    "painless",
//                    "ctx._source.remove(\"tenantId\")",
//                    Collections.<String, Object>emptyMap()));
//            request.setRefresh(true);
//            client.updateByQuery(request, COMMON_OPTIONS);
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    // 批量插入一个新字段
//    public static void updateBatch(String indexName,List<JSONObject> list) {
//
//        BulkRequest bulkRequest = new BulkRequest();
//        for (JSONObject json : list) {
//            String id = json.getString("id");
//            String tenantId = json.getString("tenantId");
//            UpdateRequest updateRequest = new UpdateRequest(indexName, id);
//            JSONObject realJson = new JSONObject();
//            realJson.put("tenantId",tenantId);
//            updateRequest.doc(realJson, XContentType.JSON);
//            bulkRequest.add(updateRequest);
//        }
//
//        try {
//            BulkResponse responses = client.bulk(bulkRequest, COMMON_OPTIONS);
//            if (responses.hasFailures()) {
//                for (BulkItemResponse item : responses.getItems()) {
//                    if (item.isFailed()) {
//                        System.out.println("batchUpdate: --------------------------------------------------");
//                        System.out.println(format.format(System.currentTimeMillis()) + " " + "bulk请求失败，打印信息如下：");
//                        System.out.println("错误信息：" + item.getFailureMessage() + "\r\n" + "主键id：" + item.getId());
//                    }
//                }
//            }
//        } catch (Exception e) {
//            if ((e instanceof ConnectException) || (e instanceof SocketTimeoutException)) {
//                System.out.println("batchUpdate: --------------------------------------------------");
//                System.out.println(format.format(System.currentTimeMillis()) + " " + e + " 批量插入连接超时，补偿一次!");
//
//                try {
//                    BulkResponse responses = client.bulk(bulkRequest, COMMON_OPTIONS);
//                    if (responses.hasFailures()) {
//                        for (BulkItemResponse item : responses.getItems()) {
//                            if (item.isFailed()) {
//                                System.out.println(format.format(System.currentTimeMillis()) + " " + "bulk请求失败，打印信息如下：");
//                                System.out.println("错误信息：" + item.getFailureMessage() + "\r\n" + "主键id：" + item.getId());
//                            }
//                        }
//                    }
//                } catch (Exception exception) {
//                    exception.printStackTrace();
//                    System.out.println("batchUpdate: --------------------------------------------------");
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿失败，再次超时!");
//                    writeToFile(list, "/home/bigdata/tmp/batchUpdate.txt");
//                } finally {
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿结束！");
//                }
//            } else {
//                e.printStackTrace();
//                System.out.println("batchUpdate: --------------------------------------------------");
//                System.out.println(format.format(System.currentTimeMillis()) + " " + "其它异常：" + e);
//                writeToFile(list, "/home/bigdata/tmp/batchUpdate.txt");
//            }
//        }
//    }
//
//    // 插入文档
//    public static void post(String indexName, JSONObject json) {
//
//        BulkRequest bulkRequest = new BulkRequest();
//        IndexRequest indexRequest = new IndexRequest(indexName);
//        indexRequest.opType(DocWriteRequest.OpType.CREATE);
//        indexRequest.source(json, XContentType.JSON);
//        indexRequest.id(json.getString("routeId"));
//        bulkRequest.add(indexRequest);
//
//        try {
//            BulkResponse responses = client.bulk(bulkRequest, COMMON_OPTIONS);
//            if (responses.hasFailures()) {
//                for (BulkItemResponse item : responses.getItems()) {
//                    if (item.isFailed()) {
//                        System.out.println(format.format(System.currentTimeMillis()) + " " + "bulk请求失败，打印信息如下：");
//                        System.out.println("错误信息：" + item.getFailureMessage() + "\r\n" + "主键id：" + item.getId());
//                    }
//                }
//            }
//        } catch (Exception e) {
//            if ((e instanceof ConnectException) || (e instanceof SocketTimeoutException)) {
//                System.out.println("post: --------------------------------------------------");
//                System.out.println(format.format(System.currentTimeMillis()) + " " + e + " 插入连接超时，补偿一次!");
//
//                try {
//                    BulkResponse responses = client.bulk(bulkRequest, COMMON_OPTIONS);
//                    if (responses.hasFailures()) {
//                        for (BulkItemResponse item : responses.getItems()) {
//                            if (item.isFailed()) {
//                                System.out.println(format.format(System.currentTimeMillis()) + " " + "bulk请求失败，打印信息如下：");
//                                System.out.println("错误信息：" + item.getFailureMessage() + "\r\n" + "主键id：" + item.getId());
//                            }
//                        }
//                    }
//                } catch (Exception exception) {
//                    exception.printStackTrace();
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿失败，再次超时!");
//                    writeToFile(json, "/home/bigdata/tmp/post.txt");
//                } finally {
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿结束！");
//                }
//            } else {
//                e.printStackTrace();
//                System.out.println("其它异常：" + e);
//                writeToFile(json, "/home/bigdata/tmp/post.txt");
//            }
//        }
//    }
//
//    // TODO 需要测试，是否可控制写入带宽
//    public static void bulkProcessor(String indexName, List<JSONObject> list) {
//
//        BulkProcessor bulkProcessor = BulkProcessor.builder(
//                (bulkRequest, bulkResponseActionListener) ->
//                        client.bulkAsync(bulkRequest, COMMON_OPTIONS, bulkResponseActionListener),
//                new BulkProcessor.Listener() {
//                    @Override
//                    public void beforeBulk(long executionId, BulkRequest request) {
//                        logger.info("序号：{} ，开始执行 {} 条数据批量操作。", executionId, request.numberOfActions());
//                    }
//
//                    @Override
//                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
//                        // 在每次执行BulkRequest后调用，通过此方法可以获取BulkResponse是否包含错误
//                        if (response.hasFailures()) {
//                            logger.error("Bulk {} executed with failures", executionId);
//                        } else {
//                            logger.info("序号：{} ，执行 {} 条数据批量操作成功，共耗费{}毫秒。", executionId, request.numberOfActions(), response.getTook().getMillis());
//                        }
//                    }
//
//                    @Override
//                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
//                        logger.error("序号：{} 批量操作失败，总记录数：{} ，报错信息为：{}", executionId, request.numberOfActions(), failure.getMessage());
//                    }
//                })
//                .setBulkActions(1000)    //刷新条数
//                .setBulkSize(new ByteSizeValue(3L, ByteSizeUnit.MB))    //刷新大小
//                .setFlushInterval(TimeValue.timeValueSeconds(1L))  //刷新时间间隔
//                .setConcurrentRequests(0)   //线程并发数
//                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
//                .build();
//
//        for (JSONObject json : list) {
//            IndexRequest indexRequest = new IndexRequest(indexName);
//            indexRequest.opType(DocWriteRequest.OpType.CREATE);
//            indexRequest.source(json, XContentType.JSON);
//            indexRequest.id(json.getString("routeId"));
//            bulkProcessor.add(indexRequest);
//        }
//
//        /// 最后执行一次刷新操作
//        bulkProcessor.flush();
//        // 30秒后关闭BulkProcessor
//        try {
//            bulkProcessor.awaitClose(30, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    // 批量插入
//    public static void batchPost(String indexName, List<JSONObject> list) {
//
//        BulkRequest bulkRequest = new BulkRequest();
//        for (JSONObject json : list) {
//            IndexRequest indexRequest = new IndexRequest(indexName);
//            indexRequest.opType(DocWriteRequest.OpType.CREATE);
//            indexRequest.source(json, XContentType.JSON);
//            indexRequest.id(json.getString("routeId"));
//            bulkRequest.add(indexRequest);
//        }
//
//        try {
//            BulkResponse responses = client.bulk(bulkRequest, COMMON_OPTIONS);
//            if (responses.hasFailures()) {
//                for (BulkItemResponse item : responses.getItems()) {
//                    if (item.isFailed()) {
//                        System.out.println("batchPost: --------------------------------------------------");
//                        System.out.println(format.format(System.currentTimeMillis()) + " " + "bulk请求失败，打印信息如下：");
//                        System.out.println("错误信息：" + item.getFailureMessage() + "\r\n" + "主键id：" + item.getId());
//                    }
//                }
//            }
//        } catch (Exception e) {
//            if ((e instanceof ConnectException) || (e instanceof SocketTimeoutException)) {
//                System.out.println("batchPost: --------------------------------------------------");
//                System.out.println(format.format(System.currentTimeMillis()) + " " + e + " 批量插入连接超时，补偿一次!");
//
//                try {
//                    BulkResponse responses = client.bulk(bulkRequest, COMMON_OPTIONS);
//                    if (responses.hasFailures()) {
//                        for (BulkItemResponse item : responses.getItems()) {
//                            if (item.isFailed()) {
//                                System.out.println(format.format(System.currentTimeMillis()) + " " + "bulk请求失败，打印信息如下：");
//                                System.out.println("错误信息：" + item.getFailureMessage() + "\r\n" + "主键id：" + item.getId());
//                            }
//                        }
//                    }
//                } catch (Exception exception) {
//                    exception.printStackTrace();
//                    System.out.println("batchPost: --------------------------------------------------");
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿失败，再次超时!");
//                    writeToFile(list, "/home/bigdata/tmp/batchPost.txt");
//                } finally {
//                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿结束！");
//                }
//            } else {
//                e.printStackTrace();
//                System.out.println("batchPost: --------------------------------------------------");
//                System.out.println(format.format(System.currentTimeMillis()) + " " + "其它异常：" + e);
//                writeToFile(list, "/home/bigdata/tmp/batchPost.txt");
//            }
//        }
//    }
//
////    public static void batchPostByArray(String prefix, List<JSONObject> list) {
////        try {
////            BulkRequest bulkRequest = new BulkRequest();
////            for (JSONObject json : list) {
////                for (String key : json.keySet()) {
////                    for (String indexName : ConstantUtil.esIndexNames) {
////                        if (key.equals(indexName)) {
////                            // 遍历list，遍历索引名，遍历json数组，塞到BulkRequest中
////                            if (indexName.equals("ent")) {
////                                JSONArray currentArr = json.getJSONArray(key);
////                                for (int i = 0; i < currentArr.size(); i++) {
////                                    JSONObject currentJson = currentArr.getJSONObject(i);
////                                    IndexRequest indexRequest = new IndexRequest(prefix + indexName);
////                                    indexRequest.opType(DocWriteRequest.OpType.CREATE);
////                                    indexRequest.source(currentJson, XContentType.JSON);
////                                    indexRequest.id(currentJson.getString("routeId"));
////                                    bulkRequest.add(indexRequest);
////                                }
////                            } else {
////                                JSONArray currentArr = json.getJSONArray(key);
////                                for (int i = 0; i < currentArr.size(); i++) {
////                                    JSONObject currentJson = currentArr.getJSONObject(i);
////                                    IndexRequest indexRequest = new IndexRequest(prefix + indexName);
////                                    indexRequest.opType(DocWriteRequest.OpType.CREATE);
////                                    indexRequest.source(currentJson, XContentType.JSON);
////                                    indexRequest.routing(currentJson.getString("routeId"));
////                                    bulkRequest.add(indexRequest);
////                                }
////                            }
////                        }
////                    }
////                }
////            }
////            BulkResponse responses = client.bulk(bulkRequest, COMMON_OPTIONS);
////            if (responses.hasFailures()) {
////                for (BulkItemResponse item : responses.getItems()) {
////                    if (item.isFailed()) {
////                        System.out.println(format.format(System.currentTimeMillis()) + " " + "bulk请求失败，打印信息如下：");
////                        System.out.println(
////                                "错误信息：" + item.getFailureMessage() + "\r\n" +
////                                        "索引名：" + item.getIndex() + "\r\n" +
////                                        "当前批次id：" + item.getItemId() + "\r\n" +
////                                        "主键id：" + item.getId());
////                    }
////                }
////            }
////        } catch (Exception e) {
////            if ((e instanceof ConnectException) || (e instanceof SocketTimeoutException)) {
////                System.out.println("--------------------------------------------------");
////                System.out.println(format.format(System.currentTimeMillis()) + " " + e + " 连接超时，补偿一次!");
////
////                BulkRequest bulkRequest = new BulkRequest();
////                for (JSONObject json : list) {
////                    for (String key : json.keySet()) {
////                        for (String indexName : ConstantUtil.esIndexNames) {
////                            if (key.equals(indexName)) {
////                                if (indexName.equals("ent")) {
////                                    JSONArray currentArr = json.getJSONArray(key);
////                                    for (int i = 0; i < currentArr.size(); i++) {
////                                        JSONObject currentJson = currentArr.getJSONObject(i);
////                                        IndexRequest indexRequest = new IndexRequest(prefix + indexName);
////                                        indexRequest.opType(DocWriteRequest.OpType.CREATE);
////                                        indexRequest.source(currentJson, XContentType.JSON);
////                                        indexRequest.id(currentJson.getString("routeId"));
////                                        bulkRequest.add(indexRequest);
////                                    }
////                                } else {
////                                    JSONArray currentArr = json.getJSONArray(key);
////                                    for (int i = 0; i < currentArr.size(); i++) {
////                                        JSONObject currentJson = currentArr.getJSONObject(i);
////                                        IndexRequest indexRequest = new IndexRequest(prefix + indexName);
////                                        indexRequest.opType(DocWriteRequest.OpType.CREATE);
////                                        indexRequest.source(currentJson, XContentType.JSON);
////                                        indexRequest.routing(currentJson.getString("routeId"));
////                                        bulkRequest.add(indexRequest);
////                                    }
////                                }
////                            }
////                        }
////                    }
////                }
////                try {
////                    BulkResponse responses = client.bulk(bulkRequest, COMMON_OPTIONS);
////                    if (responses.hasFailures()) {
////                        for (BulkItemResponse item : responses.getItems()) {
////                            if (item.isFailed()) {
////                                System.out.println(format.format(System.currentTimeMillis()) + " " + "bulk请求失败，打印信息如下：");
////                                System.out.println(
////                                        "错误信息：" + item.getFailureMessage() + "\r\n" +
////                                                "索引名：" + item.getIndex() + "\r\n" +
////                                                "当前批次id：" + item.getItemId() + "\r\n" +
////                                                "主键id：" + item.getId());
////                            }
////                        }
////                    }
////                } catch (IOException ioException) {
////                    ioException.printStackTrace();
////                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿失败，再次超时!");
////
////                    try {
////                        FileUtil.write(list, "/home/bigdata/lostData.txt");
////                        System.out.println("数据写入文件成功!");
////                    } catch (IOException exception) {
////                        System.out.println("数据写入文件失败!");
////                    }
////                } finally {
////                    System.out.println(format.format(System.currentTimeMillis()) + " 补偿结束！");
////                }
////            } else {
////                e.printStackTrace();
////                System.out.println("其它异常：" + e);
////            }
////        }
////    }
//
//    /**
//     * 通过内存队列方式写入，一个队列对应一个索引
//     *
//     * @param prefix：要写入的es索引前缀，用于切换库
//     * @param json：当前队列中存储的批量文档（如：1000个企业 * 每个企业的多条记录）
//     */
////    public static void postByQueue(String prefix, JSONObject json) {
////        AtomicReference<String> indexName = new AtomicReference<>("");
////        try {
////            BulkRequest bulkRequest = new BulkRequest();
////            json.forEach((k, v) -> {
////                indexName.set(k);
////                List<JSONObject> list = (List<JSONObject>) v;
////                list.forEach(c -> {
////                    IndexRequest indexRequest = new IndexRequest(prefix + k);
////                    indexRequest.opType(DocWriteRequest.OpType.CREATE);
////                    indexRequest.source(c, XContentType.JSON);
////                    bulkRequest.add(indexRequest);
////                });
////            });
////            System.out.println("本次bulk请求的大小: " + (bulkRequest.estimatedSizeInBytes() / (1024 * 1024)) + "m");
////            client.bulk(bulkRequest, COMMON_OPTIONS);
////        } catch (IOException e) {
////            e.printStackTrace();
////            System.out.println(e.getMessage());
////            /**
////             * 补偿机制：
////             * 如果出现连接或响应超时，则重新发送一次请求；
////             * 补偿失败后，则将这批数据塞回队列中
////             */
////            if ((e instanceof ConnectException) || (e instanceof SocketTimeoutException)) {
////                System.out.println("--------------------------------------------------");
////                System.out.println("当前索引：" + indexName.get());
////                System.out.println(format.format(System.currentTimeMillis()) + " " + e + " 连接超时，补偿一次!");
////
////                BulkRequest bulkRequest = new BulkRequest();
////                json.forEach((k, v) -> {
////                    JSONArray array = (JSONArray) v;
////                    array.forEach(c -> {
////                        IndexRequest indexRequest = new IndexRequest(k);
////                        indexRequest.opType(DocWriteRequest.OpType.CREATE);
////                        indexRequest.source(c, XContentType.JSON);
////                        bulkRequest.add(indexRequest);
////                    });
////                });
////                try {
////                    client.bulk(bulkRequest, COMMON_OPTIONS);
////                } catch (IOException ioException) {
////                    ioException.printStackTrace();
////                    System.out.println("补偿失败，数据重回队列！");
////                    MemoryQueue.getInstance().sendRequest(json);
////                }
////            }
////        }
////    }
////
////    private static void writeToFile(List<JSONObject> list, String path) {
////        try {
////            for (JSONObject json : list) {
////                FileUtil.write(json, path);
////            }
////            System.out.println("数据写入文件成功!");
////        } catch (IOException exception) {
////            System.out.println("数据写入文件失败!");
////        }
////    }
////
////    private static void writeToFile(JSONObject json, String path) {
////        try {
////            FileUtil.write(json, path);
////            System.out.println("数据写入文件成功!");
////        } catch (IOException exception) {
////            System.out.println("数据写入文件失败!");
////        }
////    }
//
//    public static void main(String[] args) {
//        JSONObject query = new JSONObject();
//        query.put("entId", "5f97739d0395147d8163ccdb");
//        JSONObject json = termQuery("final_ent", query);
//        if (json == null) {
//            System.out.println("不存在");
//        } else {
//            System.out.println(json.getString("entName"));
//        }
//    }
//}