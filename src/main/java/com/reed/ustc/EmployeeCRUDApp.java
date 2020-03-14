package com.reed.ustc;

import org.apache.http.HttpHost;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EmployeeCRUDApp {
    public static void main(String[] args) throws Exception{
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("127.0.0.1", 9200)
        )
                /*.setRequestConfigCallback(
                        new RestClientBuilder.RequestConfigCallback() {

                            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                                return requestConfigBuilder
                                        .setConnectTimeout(5000) //连接超时（默认为1秒）
                                        .setSocketTimeout(60000);//套接字超时（默认为30秒）
                            }
                        }
                )
                .setMaxRetryTimeoutMillis(60000)//调整最大重试超时时间（默认为30秒）
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {

                                                 public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                                                     return httpClientBuilder.setDefaultIOReactorConfig(
                                                             IOReactorConfig.custom().setIoThreadCount(1).build());//线程数
                                                 }
                                             }
                )*/;

        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);

        update(client);

       // createEmployee2(client);

        //likeEmployee(client);
      //  inEmployee(client);
        getEmployee(client);
        client.close();

    }


    private static void inEmployee(RestHighLevelClient client) throws Exception {

        String type = "company";
        String index = "employee";
        // 查询指定的索引库
        SearchRequest searchRequest = new SearchRequest(index,type);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        /**
         *  SELECT * FROM p_test where uid in (1,2)
         * */
        // 设置查询条件
    //    sourceBuilder.query(QueryBuilders.termsQuery("age", 12, 23));
        searchRequest.source(sourceBuilder);

        System.out.println("in查询的DSL语句:"+sourceBuilder.toString());
        // 同步查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 结果
        searchResponse.getHits().forEach(hit -> {
            Map<String, Object> map = hit.getSourceAsMap();
            String string = hit.getSourceAsString();
            System.out.println("in查询的Map结果:" + map);
            System.out.println("in查询的String结果:" + string);
        });

        System.out.println("\n=================\n");
    }

    /*
    company/employee/1
     */
    private static void getEmployee(RestHighLevelClient client) throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("company");
        searchRequest.types("employee");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        boolQueryBuilder.should(QueryBuilders.termQuery("age",23));
        BoolQueryBuilder boolQueryBuilder2 = new BoolQueryBuilder();

        /**
         *  SELECT * FROM test1 where (uid = 1234 or uid =12345)  and phone = 12345678909
         *
        boolQueryBuilder2.should(QueryBuilders.termQuery("uid", 1234));
        boolQueryBuilder2.should(QueryBuilders.termQuery("uid", 12345));
        boolQueryBuilder.must(boolQueryBuilder2);
        boolQueryBuilder.must(QueryBuilders.termQuery("phone", "12345678909"));
         */
        searchSourceBuilder.query(boolQueryBuilder);
        System.out.println("或查询语句:" + searchSourceBuilder.toString());
        searchRequest.source(searchSourceBuilder);
        // 同步查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        searchResponse.getHits().forEach(documentFields -> {

            System.out.println("查询结果:" + documentFields.getSourceAsMap());
        });
    }


    private static void likeEmployee(RestHighLevelClient client) throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("company");
        searchRequest.types("employee");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

     //   boolQueryBuilder.should(QueryBuilders.termQuery("age",23));

        boolQueryBuilder.must(QueryBuilders.wildcardQuery("country", "*in*"));


        searchSourceBuilder.query(boolQueryBuilder);
        System.out.println("或查询语句:" + searchSourceBuilder.toString());
        searchRequest.source(searchSourceBuilder);
        // 同步查询
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        searchResponse.getHits().forEach(documentFields -> {

            System.out.println("查询结果:" + documentFields.getSourceAsMap());
        });
    }


    private static void createEmployee1(RestHighLevelClient client) throws Exception {
        String index = "company";
        String type = "employee";
        // 唯一编号
        String id = "1";
        IndexRequest request = new IndexRequest(index, type, id);
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "reed");
        jsonMap.put("age", 12);
        jsonMap.put("position", "java");
        jsonMap.put("country", "china");
        jsonMap.put("join_date", "2020-03-13");
        request.source(jsonMap);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexResponse.getResult());
    }

    private static void createEmployee2(RestHighLevelClient client) throws Exception {
        String index = "company";
        String type = "employee";
        // 唯一编号
        String id = "2";
        IndexRequest request = new IndexRequest(index, type, id);


        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("name", "june");
            builder.field("age", 23);
            builder.field("position", "senior java");
            builder.field("join_date", "2019-03-14");
            builder.field("country", "china");
        }
        builder.endObject();
        request.source(builder);
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.getResult());
    }


    private static void update(RestHighLevelClient client) throws IOException {
        String type = "company";
        String index = "employee";
        // 唯一编号
        String id = "1";
        UpdateRequest upateRequest = new UpdateRequest();
        upateRequest.id(id);
        upateRequest.index(index);
        upateRequest.type(type);

        // 依旧可以使用Map这种集合作为更新条件
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "reed");
        jsonMap.put("age", 12);
        jsonMap.put("position", "java");
        jsonMap.put("country", "china");
        jsonMap.put("join_date", "2020-03-14");
        upateRequest.doc(jsonMap);
        // upsert 方法表示如果数据不存在，那么就新增一条
        upateRequest.docAsUpsert(true);
        client.update(upateRequest, RequestOptions.DEFAULT);
        System.out.println("更新成功！");

    }
}
