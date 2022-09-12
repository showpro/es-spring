package com.zhan.es.dao;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import com.zhan.es.pojo.Product;

@Repository
public interface ProductDao extends ElasticsearchRepository<Product, Long>{

}
