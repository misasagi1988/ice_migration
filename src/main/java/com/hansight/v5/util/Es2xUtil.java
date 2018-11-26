package com.hansight.v5.util;

/*import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;*/
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by ck on 2017/9/21.
 */
public class Es2xUtil {
    private final static Logger LOG = LoggerFactory.getLogger(Es2xUtil.class);
    /*private TransportClient client;
    private BulkProcessor bulkProcessor;
    private TimeValue timeValue = new TimeValue(10*60*1000);

    public EsUtil(String clustertName, String nodes){
        Settings settings = Settings.settingsBuilder().put("cluster.name", clustertName)
                //.put("client.transport.ping_timeout", 60*1000)
                .build();
        client = TransportClient.builder().settings(settings).build();
        if (nodes != null) {
            String[] arr = nodes.split(",");
            for (String tmp : arr) {
                String[] tmpArr = tmp.split(":");
                if (tmpArr.length == 2) {
                    String host = tmpArr[0];
                    int port = Integer.parseInt(tmpArr[1]);
                    try {
                        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        bulkProcessor = BulkProcessor.builder(
                client,
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request,
                                          BulkResponse response) {
                        if(response.hasFailures()){
                            LOG.warn("bulk failed: {}", response.buildFailureMessage());
                        }
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                        LOG.warn("bulk failed: {}", failure);
                    }
                }).setBulkActions(4)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(4).build();
    }

    public boolean update(String index, String type, String id, Map data, boolean force){
        if(force) {
            try {
                client.prepareUpdate(index, type, id).setDoc(data).setRefresh(true).get();
                return true;
            } catch (Exception e) {
                System.out.println(e);
            }
        }
        else {
            update(index, type, id, data);
        }
        return false;
    }


    public boolean update(String index, String type, String id, Map data){
        bulkProcessor.add(new UpdateRequest(index, type, id).doc(data));
        return true;
    }

    public void insert(String index, String type, String id, Map data, boolean force){
        if(force) {
            try {
                client.prepareIndex().setIndex(index).setType(type).setId(id).setSource(data).setRefresh(true).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        else {
            insert(index, type, id, data);
        }
    }

    public void insert(String index, String type, String id, Map data){
        IndexRequest indexRequest = new IndexRequest(index, type, id).source(data);
        bulkProcessor.add(indexRequest);
    }

    *//*public void insert(String index, String type, Map data){
        if(data.get("id") == null) {
            IndexRequest indexRequest = new IndexRequest(index, type).source(data);
            bulkProcessor.add(indexRequest);
        }
        else {
            IndexRequest indexRequest = new IndexRequest(index, type, data.get("id").toString()).source(data);
            bulkProcessor.add(indexRequest);
        }
    }*//*


    public List<Map> searchThenTermAggregation(String index, String type, QueryBuilder query, TermsBuilder termsBuilder){
        try {
            SearchResponse searchResponse = client.prepareSearch(index)
                    .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                    .setTypes(type)
                    .setQuery(query)
                    .addAggregation(termsBuilder)
                    .setSize(0)
                    .execute().actionGet();

            if(searchResponse != null){
                List<Map> agg = new ArrayList<>();
                if(searchResponse.getAggregations()  == null) return agg;
                Aggregation aggregation = searchResponse.getAggregations().get(termsBuilder.getName());
                if(aggregation instanceof StringTerms){
                    ((StringTerms) aggregation).getBuckets().forEach(bucket -> {
                        Map m = new HashMap();
                        m.put("key", bucket.getKeyAsString());
                        m.put("doc_count", bucket.getDocCount());
                        agg.add(m);
                    });
                }
                return agg;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    public List<Map> search(String index, String type, QueryBuilder query){
        try {
            SearchResponse searchResponse = client.prepareSearch(index)
                    .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                    .setTypes(type)
                    .setQuery(query)
                    .execute().actionGet();

            if(searchResponse != null){
                return extractDoc(searchResponse.getHits().hits());
            }
        }catch (Exception e){
            System.out.println(e);
        }
        return new ArrayList<>();
    }

    public List<Map> searchByScan(String index, String type, QueryBuilder query){
        try {


            SearchResponse searchResponse = client.prepareSearch(index)
                    .setTypes(type)
                    .setIndicesOptions(IndicesOptions.fromOptions(true, true, true, false))
                    .setQuery(query)
                    .setSearchType(SearchType.SCAN)
                    .setScroll(timeValue)
                    .execute().actionGet();
            return searchByScrollId(searchResponse.getScrollId());
        }catch (Exception e){
            System.out.println(e);
        }
        return new ArrayList<>();
    }

    private List<Map> searchByScrollId(String scrollId){

        SearchScrollRequestBuilder searchScrollRequestBuilder;
        SearchResponse response;
        List<Map> mapList = new ArrayList<>();
        try {
            while (true) {
                searchScrollRequestBuilder = client.prepareSearchScroll(scrollId);
                searchScrollRequestBuilder.setScroll(timeValue);
                response = searchScrollRequestBuilder.get();
                if (response.getHits().getHits().length == 0) {
                    break;
                } // if
                mapList.addAll(extractDoc(response.getHits().hits()));
                scrollId = response.getScrollId();
            }
        }catch (Exception e){
            System.out.println(e);
        }
        return mapList;
    }


    private List<Map> extractDoc(SearchHit[] searchHits){
        List<Map> mapList = new ArrayList<>();
        for (SearchHit searchHit : searchHits){
            Map m = searchHit.getSource();
            if(m == null){
                m = new HashMap();
                Iterator<Map.Entry<String, SearchHitField>> iter = searchHit.getFields().entrySet().iterator();
                while (iter.hasNext()){
                    Map.Entry<String, SearchHitField> ele = iter.next();
                    m.put(ele.getKey(), ele.getValue().value());
                }
            }
            else {
                m.putAll(searchHit.getSource());
            }
            *//*m.put("_index", searchHit.getIndex());
            m.put("_type", searchHit.getType());
            m.put("_id", searchHit.getId());*//*

            mapList.add(m);
        }
        return mapList;
    }



    public void deleteIndex(String... index){
        LOG.info("delete dirty index: {}", String.join(",", index));
        client.admin().indices().delete(new DeleteIndexRequest()
                .indices(index)
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, true)));
    }

    public void closeIndex(String... index){
        LOG.info("close unused index: {}", String.join(",", index));
        client.admin().indices().close(new CloseIndexRequest()
                .indices(index)
                .indicesOptions(IndicesOptions.fromOptions(true, true, true, false)));
    }*/
}
