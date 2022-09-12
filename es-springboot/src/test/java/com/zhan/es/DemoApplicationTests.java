package com.zhan.es;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;

import com.zhan.es.dao.ProductDao;
import com.zhan.es.pojo.Product;

@SpringBootTest
class DemoApplicationTests {

    //注入 ElasticsearchRestTemplate
    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    // // 通过set 方法注入
    // @Autowired
    // public void setElasticsearchRestTemplate(ElasticsearchRestTemplate elasticsearchRestTemplate) {
    //     this.elasticsearchRestTemplate = elasticsearchRestTemplate;
    // }

    //创建索引并增加映射配置
    @Test
    public void createIndex(){
        //创建索引，系统初始化会自动创建 product 索引
        System.out.println("创建索引");
    }

    // 删除索引
    @Test
    public void deleteIndex(){
        //删除索引
        IndexOperations indexOperations = elasticsearchRestTemplate.indexOps(Product.class);
        boolean flg = indexOperations.delete();
        System.out.println("删除索引 = " + flg);
    }


    // 文档操作
    @Autowired
    private ProductDao productDao;
    /**
     * 新增
     */
    @Test
    public void save(){
        Product product = new Product();
        product.setId(2L);
        product.setTitle("华为手机");
        product.setCategory("手机");
        product.setPrice(2999.0);
        product.setImages("http://img13.360buyimg.com/n1/s450x450_jfs/t1/8539/17/18705/61758/62dfcdfdEcb6a01cb/3e3aa692a7f1f46e.jpg.avif");
        productDao.save(product);
    }
    //postman, GET http://localhost:9200/product/_doc/2

    //修改
    @Test
    public void update(){
        Product product = new Product();
        product.setId(2L);
        product.setTitle("小米 2 手机");
        product.setCategory("手机");
        product.setPrice(9999.0);
        product.setImages("http://img13.360buyimg.com/n1/s450x450_jfs/t1/8539/17/18705/61758/62dfcdfdEcb6a01cb/3e3aa692a7f1f46e.jpg.avif");
        productDao.save(product);
    }
    //postman, GET http://localhost:9200/product/_doc/2


    //根据 id 查询
    @Test
    public void findById(){
        Product product = productDao.findById(2L).get();
        System.out.println(product);
    }

    @Test
    public void findAll(){
        Iterable<Product> products = productDao.findAll();
        for (Product product : products) {
            System.out.println(product);
        }
    }

    //删除
    @Test
    public void delete(){
        Product product = new Product();
        product.setId(2L);
        productDao.delete(product);
    }
    //postman, GET http://localhost:9200/product/_doc/2

    //批量新增
    @Test
    public void saveAll(){
        List<Product> productList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Product product = new Product();
            product.setId(Long.valueOf(i));
            product.setTitle("["+i+"]小米手机");
            product.setCategory("手机");
            product.setPrice(1999.0 + i);
            product.setImages("http://img13.360buyimg.com/n1/s450x450_jfs/t1/8539/17/18705/61758/62dfcdfdEcb6a01cb/3e3aa692a7f1f46e.jpg.avif");
            productList.add(product);
        }
        productDao.saveAll(productList);
    }

    //分页查询
    @Test
    public void findByPageable() {
        //设置排序(排序方式，正序还是倒序，排序的 id)
        Sort sort = Sort.by(Sort.Direction.DESC, "id");
        int currentPage = 0;//当前页，第一页从 0 开始， 1 表示第二页
        int pageSize = 5;//每页显示多少条
        //设置查询分页
        PageRequest pageRequest = PageRequest.of(currentPage, pageSize, sort);
        //分页查询
        Page<Product> productPage = productDao.findAll(pageRequest);
        for (Product Product : productPage.getContent()) {
            System.out.println(Product);
        }
    }

    // 文档搜索

    /**
     * search 查询
     * search(termQueryBuilder) 调用搜索方法，参数查询构建器对象
     * springboot2.5.6已经移除了ElasticsearchRepository里的search()方法
     */
    @Test
    public void search(){
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "小米");
        NativeSearchQuery nativeSearchQuery = new NativeSearchQueryBuilder()
            .withQuery(termQueryBuilder)
            .withSort(SortBuilders.fieldSort("created").order(SortOrder.DESC))
            .build();

        SearchHits<Product> searchHits = elasticsearchRestTemplate.search(nativeSearchQuery, Product.class);

        if (searchHits.getTotalHits() == 0) {
            throw new ResourceNotFoundException("没有相关记录");
        }

        List<Product> products = searchHits.stream().map(SearchHit::getContent).collect(Collectors.toList());
        for (Product itemVO : products) {
            System.out.println(itemVO);
        }
    }

    /**
     * 查询加分页
     */
    @Test
    public void searchByPage(){
        int currentPage= 0 ;
        int pageSize = 5;
        //设置查询分页
        PageRequest pageRequest = PageRequest.of(currentPage, pageSize);
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("title", "小米");

        List<Product> itemVOS = new ArrayList<>();

        try {
            NativeSearchQuery query = new NativeSearchQueryBuilder()
                .withQuery(termQueryBuilder)
                .withPageable(pageRequest)
                .build();

            SearchHits<Product> search = elasticsearchRestTemplate.search(query, Product.class, IndexCoordinates.of("product"));

            search.forEach((hits)->itemVOS.add(hits.getContent()));
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (Product itemVO : itemVOS) {
            System.out.println(itemVO);
        }
    }


}
