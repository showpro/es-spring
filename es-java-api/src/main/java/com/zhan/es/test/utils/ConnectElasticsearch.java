package com.zhan.es.test.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ConnectElasticsearch {
    public static void connect(ElasticsearchTask task) {
        // 创建客户端对象，并自动关闭连接
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("192.168.133.133", 9200, "http")))) {
            task.doSomething(client);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}