package com.leyou.test;

import com.leyou.es.ItemRepository;
import com.leyou.es.pojo.Item;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest
public class EsTest {

    @Autowired
    private ElasticsearchTemplate template;

    @Autowired
    private ItemRepository repository;

    @Test
    public void testCreateIndex(){
        template.createIndex(Item.class);
        template.putMapping(Item.class);
    }

    @Test
    public void insertIndex(){
        List<Item> list = new ArrayList<>();
        list.add(new Item(1L, "小米手机7", "手机", "小米", 3299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(2L, "坚果手机R1", "手机", "锤子", 3699.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(3L, "华为META10", "手机", "华为", 4499.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(4L, "小米Mix2S", "手机", "小米", 4299.00, "http://image.leyou.com/13123.jpg"));
        list.add(new Item(5L, "荣耀V10", "手机", "华为", 2799.00, "http://image.leyou.com/13123.jpg"));
        repository.saveAll(list);
    }

    @Test
    public void findIndex(){
        Iterable<Item> all = repository.findAll();
        for (Item item : all) {
            System.out.println("item = "+item);
        }
    }

    @Test
    public void findIndex2(){
        Iterable<Item> list = repository.findByPriceBetween(2000d,4000d);
        for (Item item : list) {
            System.out.println("item = "+item);
        }
    }

    @Test
    public void finfIndexNative(){
//        查询构建工具
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
//        条件搜索
        queryBuilder.withQuery(QueryBuilders.matchQuery("title","小米手机"));
//        排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
//        分页
        queryBuilder.withPageable(PageRequest.of(0,2));
//        字段控制
        queryBuilder.withSourceFilter(new FetchSourceFilter(null,new String[]{"images"}));
//        搜索
        Page<Item> items = repository.search(queryBuilder.build());
//
        List<Item> content = items.getContent();
        for (Item item : content) {
            System.out.println(item);
        }
    }

    @Test
    public void finfAgg(){
//        查询构建工具
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
//        条件搜索
        queryBuilder.withQuery(QueryBuilders.matchQuery("title","小米手机"));
//        排序
        queryBuilder.withSort(SortBuilders.fieldSort("price").order(SortOrder.DESC));
//        分页
        queryBuilder.withPageable(PageRequest.of(0,2));
//        字段控制
        queryBuilder.withSourceFilter(new FetchSourceFilter(null,new String[]{"images"}));
//        聚合
        AggregatedPage<Item> items = template.queryForPage(queryBuilder.build(), Item.class);

        List<Item> content = items.getContent();
        for (Item item : content) {
            System.out.println(item);
        }
    }

    @Test
    public void finfAgg2(){
//        查询构建工具
        NativeSearchQueryBuilder queryBuilder = new NativeSearchQueryBuilder();
//        添加聚合
        queryBuilder.addAggregation(AggregationBuilders.terms("popularBrand").field("brand"));
//        聚合
        AggregatedPage<Item> items = template.queryForPage(queryBuilder.build(), Item.class);

        Aggregations aggregations = items.getAggregations();

        StringTerms popularBrand = aggregations.get("popularBrand");

        List<StringTerms.Bucket> buckets = popularBrand.getBuckets();

        for (StringTerms.Bucket bucket : buckets) {
            System.out.println("key = "+bucket.getKeyAsString());
            System.out.println("count = "+bucket.getDocCount());
        }

    }
}
