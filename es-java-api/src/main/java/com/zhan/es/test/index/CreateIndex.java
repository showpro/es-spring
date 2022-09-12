package com.zhan.es.test.index;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;

public class CreateIndex {
    public static void main(String[] args) throws IOException {
        // 创建客户端对象
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.133.133", 9200, "http")));

        // 创建索引 - 请求对象
        CreateIndexRequest request = new CreateIndexRequest("user1");
        // 发送请求，获取响应
        CreateIndexResponse response = client.indices()
                .create(request, RequestOptions.DEFAULT);
        // 响应状态
        boolean acknowledged = response.isAcknowledged();
        System.out.println("操作状态 = " + acknowledged);

        // 关闭客户端连接
        client.close();
    }
}