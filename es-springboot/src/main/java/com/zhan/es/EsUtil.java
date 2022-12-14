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
            throw new RuntimeException("?????????????????????");
        if (StrUtil.isEmpty(indexName))
            throw new RuntimeException("?????????????????????");
        if (withAliases == null || withAliases.length <= 0)
            throw new RuntimeException("???????????????????????????");
        for (String withAlias : withAliases) {
            if (!withAlias.endsWith("_search")) {
                throw new RuntimeException("????????????????????????_search?????????");
            }
        }
        return createIndex(destinationClass, indexName, 3, 2, Integer.MAX_VALUE, withAliases);
    }

    /**
     * ????????????
     *
     * @param destinationClass ????????????
     * @param withAliases ?????? ????????????"***_search"????????????
     * @author mar
     * @date 2021/10/28 14:54
     */
    public static <T> boolean createIndex(Class<T> destinationClass, String indexName, Integer shards, Integer replicas,
        Integer maxResult, String... withAliases) {
        if (ObjectUtil.isNull(destinationClass))
            throw new RuntimeException("?????????????????????");
        if (StrUtil.isEmpty(indexName))
            throw new RuntimeException("?????????????????????");
        if (withAliases == null || withAliases.length <= 0)
            throw new RuntimeException("???????????????????????????");
        for (String withAlias : withAliases) {
            if (!withAlias.endsWith("_search")) {
                throw new RuntimeException("????????????????????????_search?????????");
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
     * ???????????????????????????
     *
     * @param indexName ????????????
     * @author mar
     * @date 2021/10/28 16:29
     */
    public static boolean deleteIndexByName(String indexName) {
        return elasticsearchRestTemplate.indexOps(IndexCoordinates.of(indexName))
            .delete();
    }

    /**
     * ???????????????????????????????????????
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
     * ???????????????????????????
     */
    public static Set<String> getAlias(String index) {
        if (StrUtil.isEmpty(index))
            throw new RuntimeException("?????????????????????");
        Map<String, Set<AliasData>> aliasesForIndex = elasticsearchRestTemplate.indexOps(IndexCoordinates.of(index))
            .getAliasesForIndex();
        Set<String> set = new HashSet<>();
        aliasesForIndex.forEach((key, value) -> {
            value.forEach(data -> set.add(data.getAlias()));
        });
        return set;
    }

    /**
     * ?????????????????????
     *
     * @param index ????????????
     * @param alias ??????
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
     * ?????????????????????
     *
     * @param index ????????????
     * @param alias ??????
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
     * ????????????????????? ?????????????????? ??????????????????????????????
     *
     * @param index ????????????
     * @param oldAlias ??????????????????
     * @param newAlias ??????????????????
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
     * ??????????????????
     *
     * @param t ????????????????????????
     * @param indexName ?????????
     * @return java.lang.String ????????????id
     */
    public static <T> void saveByEntity(T t, String indexName) {
        //?????????????????????????????????id
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
     * ????????????
     *
     * @param sourceList ??????????????????????????????
     * @param indexName ?????????
     * @return java.util.List<java.lang.String> ??????idList
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
     * ??????????????????
     *
     * @param entity ????????????????????????
     * @param indexName ?????????
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
        // ?????????UpdateQuery??????????????????Document???????????????Document????????????????????????????????????
        //?????????Document?????????????????????org.springframework.data.elasticsearch.core.document
        // ??????????????????????????????BeanUtils.describe(entity)??????????????????????????????map??????map??????
        // ???Document?????????
        //????????????false???true?????????????????????????????????
        UpdateQuery build = UpdateQuery.builder(id)
            .withDocAsUpsert(false)
            .withDocument(document)
            .build();
        elasticsearchRestTemplate.update(build, IndexCoordinates.of(indexName));
    }

    /**
     * ??????maps????????????
     *
     * @param list ??????????????????????????????
     * @param indexName ?????????
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
     * ??????id????????????
     *
     * @param id
     * @param indexName ?????????
     * @return java.lang.String ????????????id
     */
    public static String deleteById(String id, String indexName) {
        return elasticsearchRestTemplate.delete(id, IndexCoordinates.of(indexName));
    }

    /**
     * ??????id??????????????????
     *
     * @param docIdName ??????id??????????????????????????????????????????id??????????????????id???
     * @param ids ???????????????id??????
     * @param indexName ????????????
     * @return void
     */
    public static void deleteByIds(String docIdName, List<String> ids, String indexName) {
        StringQuery query = new StringQuery(QueryBuilders.termsQuery(docIdName, ids)
            .toString());
        elasticsearchRestTemplate.delete(query, null, IndexCoordinates.of(indexName));
    }

    /**
     * ????????????ES???????????????
     */
    @SneakyThrows
    public static <T> Map<T, Long> count(Aggregations aggregations, Class<T> destinationClass)
    throws NoSuchFieldException, IllegalAccessException {
        //???????????????
        Map<T, Long> map = new HashMap<>();
        //???t??????JSONObject????????????????????????
        Map<JSONObject, Long> jsonMap = new HashMap<>();
        for (Aggregation aggregation : aggregations) {
            Terms terms = (Terms) aggregation;
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            if (buckets.size() > 0) {
                // ??????????????????aggregation????????????????????????????????????
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
            //???json???????????????java??????
            T vo = JSONObject.toJavaObject(entry.getKey(), destinationClass);
            map.put(vo, entry.getValue());
        }
        return map;
    }

    /**
     * ??????????????????
     */
    private static <T> void countIterator(Aggregations aggregations, Map<JSONObject, Long> jsonMap, T destinationClass)
    throws NoSuchFieldException, IllegalAccessException {
        for (Aggregation aggregation : aggregations) {
            Terms terms = (Terms) aggregation;
            List<? extends Terms.Bucket> buckets = terms.getBuckets();
            if (buckets.size() > 0) {
                // ??????????????????aggregation????????????????????????????????????
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
     * ?????????id???
     *
     * @param t ??????
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
                    //??????????????????
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
     * ???????????????????????????????????????????????????????????????
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
     * ?????????List<Object>?????????Map<String,Object>???????????????????????????
     *
     * @param list ??????
     * @return ????????????
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
