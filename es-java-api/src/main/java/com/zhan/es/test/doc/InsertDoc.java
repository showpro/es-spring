package com.zhan.es.test.doc;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.xcontent.XContentType;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zhan.es.test.model.User;
import com.zhan.es.test.utils.ConnectElasticsearch;

public class InsertDoc {
    public static void main(String[] args) {
        ConnectElasticsearch.connect(client -> {
            // 新增文档 - 请求对象
            IndexRequest request = new IndexRequest();
            // 设置索引及唯一性标识, 如果id存在，则更新
            request.index("user").id("1001");

            // 创建数据对象
            User user = new User();
            user.setName("zhangsan");
            user.setAge(30);
            user.setSex("男");

            // Model -> JSON
            ObjectMapper objectMapper = new ObjectMapper();
            String usertJson = objectMapper.writeValueAsString(user);
            // 向ES添加文档数据，必须将数据格式转换为为 JSON 格式
            request.source(usertJson, XContentType.JSON);
            // 客户端发送请求，获取响应对象
            IndexResponse response = client.index(request, RequestOptions.DEFAULT);

            // 打印结果信息
            System.out.println("_index:" + response.getIndex());
            System.out.println("_id:" + response.getId());
            System.out.println("_result:" + response.getResult());
        });
    }
}