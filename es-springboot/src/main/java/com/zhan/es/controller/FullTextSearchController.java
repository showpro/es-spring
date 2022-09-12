// package com.zhan.es.controller;
//
// import com.alibaba.fastjson.JSON;
// import com.zhan.es.pojo.FileData;
//
// import org.elasticsearch.action.index.IndexRequest;
// import org.elasticsearch.action.index.IndexResponse;
// import org.elasticsearch.action.search.SearchRequest;
// import org.elasticsearch.action.search.SearchResponse;
// import org.elasticsearch.client.RequestOptions;
// import org.elasticsearch.client.RestHighLevelClient;
// import org.elasticsearch.common.text.Text;
// import org.elasticsearch.index.query.QueryBuilders;
// import org.elasticsearch.search.SearchHit;
// import org.elasticsearch.search.builder.SearchSourceBuilder;
// import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
// import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
// import org.elasticsearch.xcontent.XContentType;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
// import org.springframework.data.elasticsearch.core.IndexOperations;
// import org.springframework.data.elasticsearch.core.document.Document;
// import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
// import org.springframework.util.Base64Utils;
// import org.springframework.web.bind.annotation.GetMapping;
// import org.springframework.web.bind.annotation.RequestMapping;
// import org.springframework.web.bind.annotation.RequestParam;
// import org.springframework.web.bind.annotation.RestController;
//
// import java.io.File;
// import java.io.FileInputStream;
// import java.lang.reflect.Method;
// import java.util.ArrayList;
// import java.util.Iterator;
// import java.util.List;
// import java.util.Map;
//
// /**
//  * ES通过Ingest-Attachment插件实现PDF,WORD，EXCEL等主流格式文件的文本抽取及自动导入，实现全文检索
//  *
//  * @auther:zhanzhan
//  * @Time:2022/9/11 14:21
//  */
// //第一步先定义文本抽取管道
// //PUT /_ingest/pipeline/attachment
// // {
// //  "description" : "Extract attachment information",
// //  "processors":[
// //  {
// //     "attachment":{
// //
// //         "field":"data",
// //
// //         "indexed_chars" : -1,
// //
// //         "ignore_missing":true
// //      }
// //  },
// //  {
// //      "remove":{"field":"data"}
// //  }]}
// @RestController
// @RequestMapping(value = "fullTextSearch")
// public class FullTextSearchController {
//     @Autowired
//     private ElasticsearchRestTemplate elasticsearchRestTemplate;
//     @Autowired
//     private RestHighLevelClient restHighLevelClient;
//
//     /**
//      * 创建索引
//      */
//     @GetMapping("createIndex")
//     public void add() {
//
//         IndexOperations indexOperations = elasticsearchRestTemplate.indexOps(IndexCoordinates.of("testindex"));
//         indexOperations.create();
//         Document mapping = indexOperations.createMapping(FileData.class);
//         indexOperations.putMapping(mapping);
//     }
//
//     /**
//      * 删除索引
//      */
//     @GetMapping("deleteIndex")
//     public void deleteIndex() {
//         IndexOperations indexOperations = elasticsearchRestTemplate.indexOps(FileData.class);
//         indexOperations.delete();
//     }
//
//     /**
//      * 上传文档到ES
//      */
//     @GetMapping("uploadFileToEs")
//     public void uploadFileToEs() {
//
//         try {
//             //File file = new File("D:\\desktop\\Java开发工程师-4年-王晓龙-2022-05.pdf");
//             File file = new File("D:\\desktop\\Java开发工程师-4年-王晓龙-2022-05.docx");
//             FileInputStream inputFile = new FileInputStream(file);
//             byte[] buffer = new byte[(int) file.length()];
//             inputFile.read(buffer);
//             inputFile.close();
//             //由于ElasticSearch是基于JSON格式的文档数据库，所以附件文档在插入ElasticSearch之前必须进行Base64编码。
//             String fileString = Base64Utils.encodeToString(buffer);
//
//             FileData fileData = new FileData();
//             fileData.setFileName(file.getName());
//             fileData.setFilePk(file.getName());
//             fileData.setData(fileString);
//
//             IndexRequest indexRequest = new IndexRequest("testindex").id(fileData.getFilePk());
//             indexRequest.source(JSON.toJSONString(fileData), XContentType.JSON);
//             indexRequest.setPipeline("attachment");
//
//             IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
//
//             return;
//
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//     }
//
//     /**
//      * 搜索  localhost:9090/fullTextSearch/search?txt=索引库
//      *
//      * @param txt
//      * @return
//      */
//     @GetMapping("search")
//     public Object search(@RequestParam("txt") String txt) {
//         List list = new ArrayList();
//         try {
//             SearchRequest searchRequest = new SearchRequest("testindex");
//
//             SearchSourceBuilder builder = new SearchSourceBuilder();
//
//             builder.query(QueryBuilders.matchQuery("attachment.content", txt)
//                 .analyzer("ik_max_word"));
//
//             searchRequest.source(builder);
//
//             // 返回实际命中数
//             builder.trackTotalHits(true);
//             //高亮
//             HighlightBuilder highlightBuilder = new HighlightBuilder();
//             highlightBuilder.field("attachment.content");
//             highlightBuilder.requireFieldMatch(false);//多个高亮关闭
//             highlightBuilder.preTags("<span style='color:red'>");
//             highlightBuilder.postTags("</span>");
//             builder.highlighter(highlightBuilder);
//
//             SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
//
//             if (search.getHits() != null) {
//
//                 for (SearchHit documentFields : search.getHits()
//                     .getHits()) {
//                     Map<String, HighlightField> highlightFields = documentFields.getHighlightFields();
//                     HighlightField title = highlightFields.get("attachment.content");
//                     Map<String, Object> sourceAsMap = documentFields.getSourceAsMap();
//                     if (title != null) {
//                         Text[] fragments = title.fragments();
//                         String n_title = "";
//                         for (Text fragment : fragments) {
//                             n_title += fragment;
//                         }
//                         sourceAsMap.put("data", n_title);
//                     }
//                     list.add(dealObject(sourceAsMap, FileData.class));
//                 }
//
//             }
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//         return list;
//     }
//     /*public static void ignoreSource(Map<String, Object> map) {
//         for (String key : IGNORE_KEY) {
//             map.remove(key);
//         }
//     }*/
//
//     public static <T> T dealObject(Map<String, Object> sourceAsMap, Class<T> clazz) {
//         try {
//             // ignoreSource(sourceAsMap);
//             Iterator<String> keyIterator = sourceAsMap.keySet()
//                 .iterator();
//             T t = clazz.newInstance();
//             while (keyIterator.hasNext()) {
//                 String key = keyIterator.next();
//                 String replaceKey = key.replaceFirst(key.substring(0, 1), key.substring(0, 1)
//                     .toUpperCase());
//                 Method method = null;
//                 try {
//                     method = clazz.getMethod("set" + replaceKey, sourceAsMap.get(key)
//                         .getClass());
//                 } catch (NoSuchMethodException e) {
//                     continue;
//                 }
//                 method.invoke(t, sourceAsMap.get(key));
//             }
//             return t;
//         } catch (Exception e) {
//             e.printStackTrace();
//         }
//         return null;
//     }
//
//
// }
