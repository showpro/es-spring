package com.zhan.es.test.doc;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;

import com.zhan.es.test.utils.ConnectElasticsearch;
import com.zhan.es.test.utils.ElasticsearchTask;

public class QueryDoc {
    @FunctionalInterface
    public interface QueryBuild {
        void doBuild(SearchRequest request, SearchSourceBuilder sourceBuilder) throws IOException;
    }

    public static void doQuery(RestHighLevelClient client, QueryBuild build) throws IOException {
        // 创建搜索请求对象
        SearchRequest request = new SearchRequest();
        request.indices("user");
        // 构建查询的请求体
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

        // 请求体的构建由调用者进行
        build.doBuild(request, sourceBuilder);
        request.source(sourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        // 结果输出
        SearchHits hits = response.getHits();
        System.out.println("took:" + response.getTook());
        System.out.println("timeout:" + response.isTimedOut());
        System.out.println("total:" + hits.getTotalHits());
        System.out.println("MaxScore:" + hits.getMaxScore());
        System.out.println("hits========>>");
        // 输出每条查询的结果信息
        for (SearchHit hit : hits)
            System.out.println(hit.getSourceAsString());
        System.out.println("<<========");
    }

    /**
     * 全量查询
     */
    public static final ElasticsearchTask SEARCH_ALL = client -> {
        System.out.println("=======>全量查询<=======");
        doQuery(client, (request, sourceBuilder) -> {
            // 查询全部
            sourceBuilder.query(QueryBuilders.matchAllQuery());
        });
    };
    /**
     * 条件查询
     */
    public static final ElasticsearchTask SEARCH_BY_CONDITION = client -> {
        System.out.println("=======>条件查询<=======");
        doQuery(client, (request, sourceBuilder) -> {
            // 查询条件：age = 30
            sourceBuilder.query(QueryBuilders.termQuery("age", "30"));
        });
    };
    /**
     * 分页查询
     */
    public static final ElasticsearchTask SEARCH_BY_PAGING = client -> {
        System.out.println("=======>分页查询<=======");
        doQuery(client, (request, sourceBuilder) -> {
            // 当前页起始索引(第一条数据的顺序号) from
            sourceBuilder.from(0);
            // 每页显示多少条 size
            sourceBuilder.size(2);
        });
    };
    /**
     * 查询排序
     */
    public static final ElasticsearchTask SEARCH_WITH_ORDER = client -> {
        System.out.println("=======>查询排序<=======");
        doQuery(client, (request, sourceBuilder) -> {
            // 查询全部
            sourceBuilder.query(QueryBuilders.matchAllQuery());
            // 年龄升序
            sourceBuilder.sort("age", SortOrder.ASC);
        });
    };
    /**
     * 查询排序
     */
    public static final ElasticsearchTask SEARCH_FILTER_ITEM = client -> {
        System.out.println("=======>过滤字段<=======");
        doQuery(client, (request, sourceBuilder) -> {
            String[] excludes = {};// 结果中排除 xxx
            String[] includes = {"name"}; // 结果中只有name
            sourceBuilder.fetchSource(includes, excludes);
        });
    };
    /**
     * 组合查询
     */
    public static final ElasticsearchTask SEARCH_BY_BOOL_CONDITION = client -> {
        System.out.println("=======>组合查询<=======");
        doQuery(client, (request, sourceBuilder) -> {
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            // 必须包含: age = 30
            boolQueryBuilder.must(QueryBuilders.matchQuery("age", "30"));
            // 一定不含：name = zhangsan
            boolQueryBuilder.mustNot(QueryBuilders.matchQuery("name", "zhangsan"));
            // 可能包含: sex = 男
            boolQueryBuilder.should(QueryBuilders.matchQuery("sex", "男"));
            sourceBuilder.query(boolQueryBuilder);
        });
    };
    /**
     * 范围查询
     */
    public static final ElasticsearchTask SEARCH_BY_RANGE = client -> {
        System.out.println("=======>范围查询<=======");
        doQuery(client, (request, sourceBuilder) -> {
            RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age");
            // rangeQuery.gte("30"); // age 大于等于 30
            rangeQuery.lte("40"); // age 小于等于 40
            sourceBuilder.query(rangeQuery);
        });
    };
    /**
     * 模糊查询
     */
    public static final ElasticsearchTask SEARCH_BY_FUZZY_CONDITION = client -> {
        System.out.println("=======>模糊查询<=======");
        doQuery(client, (request, sourceBuilder) -> {
            // 模糊查询: name 包含 wanqwu 的数据, 差1个字符就可以匹配成功
            sourceBuilder.query(QueryBuilders.fuzzyQuery("name", "wangwu").fuzziness(Fuzziness.ONE));
        });
    };
    /**
     * 高亮显示查询结果
     */
    public static final ElasticsearchTask SEARCH_WITH_HIGHLIGHT = client -> {
        System.out.println("=======>高亮查询<=======");
        SearchRequest request = new SearchRequest().indices("user");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        // 构建查询方式：高亮查询
        TermsQueryBuilder termsQueryBuilder =
                QueryBuilders.termsQuery("name", "zhangsan");
        // 设置查询方式
        sourceBuilder.query(termsQueryBuilder);
        // 构建高亮字段
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<font color='red'>");//设置标签前缀
        highlightBuilder.postTags("</font>");//设置标签后缀
        highlightBuilder.field("name");//设置高亮字段
        // 设置高亮构建对象
        sourceBuilder.highlighter(highlightBuilder);
        // 设置请求体
        request.source(sourceBuilder);
        // 客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        SearchHits hits = response.getHits();

        for (SearchHit hit : hits) {
            String sourceAsString = hit.getSourceAsString();
            System.out.println(sourceAsString);
            // 打印高亮结果
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            System.out.println(highlightFields);
        }
        System.out.println("<<========");
    };
    /**
     * 聚合查询：最大值查询
     */
    public static final ElasticsearchTask SEARCH_WITH_MAX = client -> {
        System.out.println("=======>最大值查询<=======");
        SearchRequest request = new SearchRequest().indices("user");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.max("maxAge").field("age"));
        request.source(sourceBuilder);

        // 客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
    };
    /**
     * 聚合查询：分组查询
     */
    public static final ElasticsearchTask SEARCH_WITH_GROUP = client -> {
        System.out.println("=======>分组查询<=======");
        SearchRequest request = new SearchRequest().indices("user");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.aggregation(AggregationBuilders.terms("age_groupby").field("age"));
        request.source(sourceBuilder);

        // 客户端发送请求，获取响应对象
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        System.out.println(response);
    };

    public static void main(String[] args) {
        ConnectElasticsearch.connect(SEARCH_ALL); // 全量查询
        ConnectElasticsearch.connect(SEARCH_BY_CONDITION); // 条件查询
        ConnectElasticsearch.connect(SEARCH_BY_PAGING); // 分页查询
        ConnectElasticsearch.connect(SEARCH_WITH_ORDER); // 查询排序
        ConnectElasticsearch.connect(SEARCH_FILTER_ITEM); // 过滤字段
        ConnectElasticsearch.connect(SEARCH_BY_BOOL_CONDITION); // 组合查询
        ConnectElasticsearch.connect(SEARCH_BY_RANGE); // 范围查询
        ConnectElasticsearch.connect(SEARCH_BY_FUZZY_CONDITION); // 模糊查询
        ConnectElasticsearch.connect(SEARCH_WITH_HIGHLIGHT); // 高亮查询
        ConnectElasticsearch.connect(SEARCH_WITH_MAX); // 最大值查询
        ConnectElasticsearch.connect(SEARCH_WITH_GROUP); // 分组查询
    }
}