// package com.zhan.es.pojo;
//
// import lombok.Data;
// import org.springframework.data.elasticsearch.annotations.Document;
// import org.springframework.data.elasticsearch.annotations.Field;
// import org.springframework.data.elasticsearch.annotations.FieldType;
//
// /**
//  * @auther:zhanzhan
//  * @Time:2022/9/11 14:21
//  */
// @Data
// @Document(indexName = "filedata")
// public class FileData {
//
//     @Field(type = FieldType.Keyword)
//     private String filePk;
//     @Field(type = FieldType.Keyword)
//     private String fileName;
//     @Field(type = FieldType.Keyword)
//     private Integer page;
//     @Field(type = FieldType.Keyword)
//     private String departmentId;
//     @Field(type = FieldType.Keyword)
//     private String ljdm;
//     @Field(type = FieldType.Text, analyzer = "ik_max_word")
//     private String data;
//     @Field(type = FieldType.Keyword)
//     private String realName;
//     @Field(type = FieldType.Keyword)
//     private String url;
//     @Field(type = FieldType.Keyword)
//     private String type;
// }
