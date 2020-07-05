package com.leyou.es;

import com.leyou.es.pojo.Item;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface ItemRepository extends ElasticsearchRepository<Item,Long> {

    public List<Item> findByPriceBetween(Double start, Double end);
}
