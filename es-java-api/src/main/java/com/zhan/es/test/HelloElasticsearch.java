package com.zhan.es.test;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;


public class HelloElasticsearch {

	public static void main(String[] args) throws IOException {
        /**
         * 在Elasticsearch7.15版本之后，Elasticsearch官方将它的高级客户端RestHighLevelClient标记为弃用状态。
         * 同时推出了全新的Java API客户端Elasticsearch Java API Client，
         * 该客户端也将在Elasticsearch8.0及以后版本中成为官方推荐使用的客户端。
         */
		// 创建ES客户端对象  192.168.133.133ES服务器地址
		RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("192.168.133.133", 9200, "http")));

//		...
		System.out.println(client);

		// 关闭客户端连接
		client.close();




	}
}
