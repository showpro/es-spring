package com.zhan.es;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.beanutils.BeanUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.index.AliasAction;
import org.springframework.data.elasticsearch.core.index.AliasActionParameters;
import org.springframework.data.elasticsearch.core.index.AliasActions;
import org.springframework.data.elasticsearch.core.index.AliasData;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.data.elasticsearch.core.query.StringQuery;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.*;

@Component
@Slf4j
public class EsUtil {

    private static ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Autowired
    public EsUtil(ElasticsearchRestTemplate elasticsearchRestTemplate) {
        EsUtil.elasticsearchRestTemplate = elasticsearchRestTemplate;
    }

    public static <T> boolean createIndex(Class<T> destinationClass, String indexName, String... withAliases) {
        if (ObjectUtil.isNull(destinationClass))
            throw new RuntimeException("参数类不能为空");
        if (StrUtil.isEmpty(indexName))
            throw new RuntimeException("参数名不能为空");
        if (withAliases == null || withAliases.length <= 0)
            throw new RuntimeException("索引别名至少有一个");
        for (String withAlias : withAliases) {
            if (!withAlias.endsWith("_search")) {
                throw new RuntimeException("索引别名只能以‘_search’结尾");
            }
        }
        return createIndex(destinationClass, indexName, 3, 2, Integer.MAX_VALUE, withAliases);
    }

    /**
     * 创建索引
     *
     * @param destinationClass 映射对象
     * @param withAliases 别名 必须如："***_search"用来搜索
     * @author mar
     * @date 2021/10/28 14:54
     */
    public static <T> boolean createIndex(Class<T> destinationClass, String indexName, Integer shards, Integer replicas,
        Integer maxResult, String... withAliases) {
        if (ObjectUtil.isNull(destinationClass))
            throw new RuntimeException("参数类不能为空");
        if (StrUtil.isEmpty(indexName))
            throw new RuntimeException("参数名不能为空");
        if (withAliases == null || withAliases.length <= 0)
            throw new RuntimeException("索引别名至少有一个");
        for (String withAlias : withAliases) {
            if (!withAlias.endsWith("_search")) {
                throw new RuntimeException("索引别名只能以‘_search’结尾");
            }
        }

        IndexCoordinates of = IndexCoordinates.of(indexName);
        IndexOperations indexOperations = elasticsearchRestTemplate.indexOps(of);
        boolean exists = indexOperations.exists();
        if (exists) {
            indexOperations.delete();
        }
        Map<String, Object> settings = new HashMap<>();
        settings.put("index.number_of_shards", shards > 0 ? shards : 3);
        settings.put("index.number_of_replicas", replicas > 0 ? replicas : 2);
        settings.put("index.max_result_window", maxResult > 0 ? maxResult : Integer.MAX_VALUE);
        Document mapping = indexOperations.createMapping(destinationClass);
        indexOperations.create(settings, mapping);
        AliasAction.Add add = new AliasAction.Add(AliasActionParameters.builder()
            .withIndices(indexName)
            .withAliases(withAliases)
            .build());
        return indexOperations.alias(new AliasActions(add));
    }

    /**
     * 根据索引名删除索引
     *
     * @param indexName 索引名称
     * @author mar
     * @date 2021/10/28 16:29
     */
    public static boolean deleteIndexByName(String indexName) {
        return elasticsearchRestTemplate.indexOps(IndexCoordinates.of(indexName))
            .delete();
    }

    /**
     * 根据索引名判断索引是否存在
     *
     * @param indexName
     * @return boolean
     * @author mar
     * @date 2021/11/29 16:39
     */
    public static boolean isExists(String indexName) {
        IndexCoordinates of = IndexCoordinates.of(indexName);
        return elasticsearchRestTemplate.indexOps(of)
            .exists();
    }

    /**
     * 获取索引对应的别名
     */
    public static Set<String> getAlias(String index) {
        if (StrUtil.isEmpty(index))
            throw new RuntimeException("索引名不能为空");
        Map<String, Set<AliasData>> aliasesForIndex = elasticsearchRestTemplate.indexOps(IndexCoordinates.of(index))
            .getAliasesForIndex();
        Set<String> set = new HashSet<>();
        aliasesForIndex.forEach((key, value) -> {
            value.forEach(data -> set.add(data.getAlias()));
        });
        return set;
    }

    /**
     * 为索引添加别名
     *
     * @param index 真实索引
     * @param alias 别名
     */
    public static boolean addAlias(String index, String... alias) {
        Preconditions.checkNotNull(index);
        Preconditions.checkNotNull(alias);
        final IndexOperations indexOps = elasticsearchRestTemplate.indexOps(IndexCoordinates.of(index));
        AliasActions aliasActions = new AliasActions(new AliasAction.Add(AliasActionParameters.builder()
            .withIndices(index)
            .withAliases(alias)
            .build()));
        return indexOps.alias(aliasActions);
    }

    /**
     * 为索引删除别名
     *
     * @param index 真实索引
     * @param alias 别名
     */
    public static boolean delAlias(String index, String... alias) {
        Preconditions.checkNotNull(index);
        Preconditions.checkNotNull(alias);
        final IndexOperations indexOps = elasticsearchRestTemplate.indexOps(IndexCoordinates.of(index));
        AliasActions aliasActions = new AliasActions(new AliasAction.Remove(AliasActionParameters.builder()
            .withIndices(index)
            .withAliases(alias)
            .build()));
        return indexOps.alias(aliasActions);
    }

    /**
     * 为索引更换别名 旧的换为新的 不会判断旧的是否存在
     *
     * @param index 真实索引
     * @param oldAlias 要删除的别名
     * @param newAlias 要新增的别名
     */
    public static boolean replaceAlias(String index, String oldAlias, String newAlias) {
        Preconditions.checkNotNull(index);
        Preconditions.checkNotNull(oldAlias);
        Preconditions.checkNotNull(newAlias);
        final IndexOperations indexOps = elasticsearchRestTemplate.indexOps(IndexCoordinates.of(index));
        final AliasAction.Add add = new AliasAction.Add(AliasActionParameters.builder()
            .withIndices(index)
            .withAliases(newAlias)
            .build());
        final AliasAction.Remove remove = new AliasAction.Remove(AliasActionParameters.builder()
            .withIndices(index)
            .withAliases(oldAlias)
            .build());
        AliasActions aliasActions = new AliasActions(add, remove);
        return indexOps.alias(aliasActions);
    }

    /**
     * 单条数据插入
     *
     * @param t 待插入的数据实体
     * @param indexName 索引名
     * @return java.lang.String 返回文档id
     */
    public static <T> void saveByEntity(T t, String indexName) {
        //这里的操作就是指定文档id
        String id = getFieldId(t);
        IndexQuery build = new IndexQueryBuilder().withId(id)
            .withObject(t)
            .build();
        elasticsearchRestTemplate.index(build, IndexCoordinates.of(indexName));
    }

    public static <T> void saveBatchByEntities(Map<String, List<T>> map) {
        if (map != null && map.size() > 0)
            map.forEach((key, value) -> saveBatchByEntities(value, key));
    }

    /**
     * 批量插入
     *
     * @param sourceList 待插入的数据实体集合
     * @param indexName 索引名
     * @return java.util.List<java.lang.String> 返回idList
     */
    public static <T> void saveBatchByEntities(List<T> sourceList, String indexName) {
        List<IndexQuery> queryList = new ArrayList<>();
        for (T source : sourceList) {
            String id = getFieldId(source);
            IndexQuery build = new IndexQueryBuilder().withId(id)
                .withObject(source)
                .build();
            queryList.add(build);
        }
        elasticsearchRestTemplate.bulkIndex(queryList, IndexCoordinates.of(indexName));
    }

    /**
     * 单条数据更新
     *
     * @param entity 待更新的数据实体
     * @param indexName 索引名
     * @return void
     */
    public static <T> void updateByEntity(T entity, String indexName) {
        String id = getFieldId(entity);
        Map<String, String> map = null;
        try {
            map = BeanUtils.describe(entity);
        } catch (Exception e) {
            e.printStackTrace();
        }
        Document document = Document.from(map);
        document.setId(id);
        // 这里的UpdateQuery需要构造一个Document对象，但是Document对象不能用实体类转化而来
        //（可见Document的源码，位于：org.springframework.data.elasticsearch.core.document
        // 包下），因此上面才会BeanUtils.describe(entity)，将实体类转化成一个map，由map转化
        // 为Document对象。
        //不加默认false。true表示更新时不存在就插入
        UpdateQuery build = UpdateQuery.builder(id)
            .withDocAsUpsert(false)
            .withDocument(document)
            .build();
        elasticsearchRestTemplate.update(build, IndexCoordinates.of(indexName));
    }

    /**
     * 根据maps批量更新
     *
     * @param list 待更新的数据实体集合
     * @param indexName 索引名
     * @return void
     * @author Innocence
     */
    public static <T> void updateByMaps(List<T> list, String indexName) {
        List<Map<String, Object>> maps = listToMap(list);
        List<UpdateQuery> updateQueries = new ArrayList<>();
        maps.forEach(item -> {
            Document document = Document.from(item);
            document.setId(String.valueOf(item.get("id")));
            UpdateQuery build = UpdateQuery.builder(document.getId())
                .withDocument(document)
                .build();
            updateQueries.add(build);
        });
        elasticsearchRestTemplate.bulkUpdate(updateQueries, IndexCoordinates.of(indexName));
    }

    /**
     * 根据id删除数据
     *
     * @param id
     * @param indexName 索引名
     * @return java.lang.String 被删除的id
     */
    public static String deleteById(String id, String indexName) {
        return elasticsearchRestTemplate.delete(id, IndexCoordinates.of(indexName));
    }

    /**
     * 根据id批量删除数据
     *
     * @param docIdName 文档id字段名，如我们上面设置的文档id的字段名为“id”
     * @param ids 需要删除的id集合
     * @param indexName 索引名称
     * @return void
     */
    public static void deleteByIds(String docIdName, List<String> ids, String indexName) {
        StringQuery query = new StringQuery(QueryBuilders.termsQuery(docIdName, ids)
            .toString());
        elasticsearchRestTemplate.delete(query, null, IndexCoordinates.of(indexName));
    }

    /**
     * 递归统计ES聚合的数据
     */
    @SneakyThrows
    public static <T> Map<T, Long> count(Aggregations aggregations, Class<T> destinationClass)
    throws NoSuchFieldException, IllegalAccessException {
        //返回值封装
        Map<T, Long> map = new HashMap<>();
        //将t转为JSONObject防止数据值被覆盖
        Map<JSONObject, Long> jsonMap = new HashMap<>();
        for (Aggregation aggregation : aggregations) {
            Terms terms = (Terms) aggregation;
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            if (buckets.size() > 0) {
                // 如果内部还有aggregation，就继续往下走，不能统计
                for (Terms.Bucket bucket : buckets) {
                    T t = destinationClass.newInstance();
                    Field declaredField = t.getClass()
                        .getDeclaredField(aggregation.getName());
                    declaredField.setAccessible(true);
                    declaredField.set(t, bucket.getKeyAsString());
                    Aggregations aggregationsInners = bucket.getAggregations();
                    if (aggregationsInners == null || aggregationsInners.asList()
                        .size() == 0) {
                        jsonMap.put((JSONObject) JSONObject.toJSON(t), bucket.getDocCount());
                    } else {
                        countIterator(aggregationsInners, jsonMap, t);
                    }
                }
            }
        }
        for (Map.Entry<JSONObject, Long> entry : jsonMap.entrySet()) {
            //将json对象转换为java对象
            T vo = JSONObject.toJavaObject(entry.getKey(), destinationClass);
            map.put(vo, entry.getValue());
        }
        return map;
    }

    /**
     * 内部递归方法
     */
    private static <T> void countIterator(Aggregations aggregations, Map<JSONObject, Long> jsonMap, T destinationClass)
    throws NoSuchFieldException, IllegalAccessException {
        for (Aggregation aggregation : aggregations) {
            Terms terms = (Terms) aggregation;
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            if (buckets.size() > 0) {
                // 如果内部还有aggregation，就继续往下走，不能统计
                for (Terms.Bucket bucket : buckets) {
                    Field declaredField = destinationClass.getClass()
                        .getDeclaredField(aggregation.getName());
                    declaredField.setAccessible(true);
                    declaredField.set(destinationClass, bucket.getKeyAsString());
                    Aggregations aggregationsInners = bucket.getAggregations();
                    if (aggregationsInners == null || aggregationsInners.asList()
                        .size() == 0) {
                        jsonMap.put((JSONObject) JSONObject.toJSON(destinationClass), bucket.getDocCount());
                    } else {
                        countIterator(aggregationsInners, jsonMap, destinationClass);
                    }
                }
            }
        }
    }

    /**
     * 解析出id值
     *
     * @param t 泛型
     * @return java.lang.String
     * @author mar
     * @date 2021/11/1 16:25
     */
    public static <T> String getFieldId(T t) {
        String primaryKey = null;
        Field[] fields = t.getClass()
            .getDeclaredFields();
        Field field;
        for (int i = 0; i < fields.length; i++) {
            fields[i].setAccessible(true);
        }
        for (int i = 1; i < fields.length; i++) {
            try {
                field = t.getClass()
                    .getDeclaredField(fields[i].getName());
                Id id = field.getAnnotation(Id.class);
                if (id != null) {
                    //打开私有访问
                    field.setAccessible(true);
                    primaryKey = (String) field.get(t);
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return primaryKey;
    }

    /**
     * 把一个字符串的第一个字母大写、效率是最高的
     *
     * @param fieldName
     * @return java.lang.String
     * @author mar
     * @date 2021/11/2 15:49
     */
    public static String getMethodName(String fieldName) {
        byte[] items = fieldName.getBytes();
        items[0] = (byte) ((char) items[0] - 'a' + 'A');
        return new String(items);
    }

    /**
     * 用于把List<Object>转换成Map<String,Object>形式，便于更新操作
     *
     * @param list 集合
     * @return 返回对象
     * @author mar
     */
    public static <T> List<Map<String, Object>> listToMap(List<T> list) {
        List<Map<String, Object>> maps = new ArrayList<>();
        try {
            for (T t : list) {
                Map<String, Object> map = BeanUtil.beanToMap(t);
                maps.add(map);
            }
            return maps;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
