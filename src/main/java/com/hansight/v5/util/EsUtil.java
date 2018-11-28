package com.hansight.v5.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Consts;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.main.MainResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.histogram.ParsedDateHistogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;


/**
 * Created by ck on 2018/10/17.
 */
public class EsUtil {
    protected static final Logger logger = LoggerFactory.getLogger(EsUtil.class);

    protected static RestHighLevelClient esClient;
    protected static RestClient          restClient;
    protected static BulkProcessor bulkProcessor;
    protected static boolean is2xVersion = false;


    /**
     * Description: return es client, if not exists, create one
     *
     * @Date: 2018/2/1
     */
    public static void createClient(String clusterName, String esNodes) {
        if (clusterName == null || clusterName.isEmpty() || esNodes == null || esNodes.isEmpty())
            return ;
        Map configs = new HashMap();
        boolean sniff = configs.containsKey("sniffenabled") ? (boolean)configs.get("sniffenabled"): false;
        Integer ping = configs.containsKey("pingtimeout") ? (Integer)configs.get("pingtimeout"): 60000;
        createESClient(clusterName, esNodes, sniff, ping, configs);
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.debug("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    try {
                        logger.warn("Bulk [{}] executed with failures, msg:{}", executionId, response.buildFailureMessage());
                    } catch (Exception e) {
                        logger.warn("Bulk [{}] executed with failures, response status:{}", executionId, response.status());
                    }
                } else {
                    logger.debug("Bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Failed to execute bulk", failure);
            }
        };

        bulkProcessor =  new BulkProcessor.Builder(esClient::bulkAsync, listener, new ThreadPool(Settings.EMPTY))
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(16)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }
    /**
     * Description: return es client, if not exists, create one
     *
     * @Date: 2018/2/1
     */
    public static void createClient(String clusterName, String esNodes, Map configs) {
        if (clusterName == null || clusterName.isEmpty() || esNodes == null || esNodes.isEmpty())
            return ;
        boolean sniff = configs.containsKey("sniffenabled") ? (boolean)configs.get("sniffenabled"): false;
        Integer ping = configs.containsKey("pingtimeout") ? (Integer)configs.get("pingtimeout"): 60000;
        createESClient(clusterName, esNodes, sniff, ping, configs);

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.debug("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    logger.warn("Bulk [{}] executed with failures, msg:{}", executionId, response.buildFailureMessage());

                } else {
                    logger.debug("Bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Failed to execute bulk", failure);
            }
        };

        bulkProcessor =  new BulkProcessor.Builder(esClient::bulkAsync, listener, new ThreadPool(Settings.EMPTY))
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(16)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }

    /**
     * Description: close all es client
     *
     * @Date: 2018/2/2
     */
    public static void closeAllClient() {

    }

    /**
     * Description: create es client
     *
     * @Date: 2018/2/1
     */
    private static synchronized void createESClient(String clusterName, String esNodes, Boolean sniffEnabled, Integer pingTimeout, Map config) {
        try {
            esClient = newHighRestClient(esNodes, sniffEnabled, pingTimeout, config);
            if (esClient != null) {
                MainResponse mainResponse = esClient.info();
                String cluster = mainResponse.getClusterName().value();
                logger.info(String.format("es 集群名称 %s", cluster));
                logger.info(String.format("es 版本号 %s", mainResponse.getVersion()));
                if(mainResponse.getVersion().major < 5){
                    is2xVersion = true;
                }
                logger.info(String.format("es 节点名称 %s", mainResponse.getNodeName()));
                if (!StringUtils.equals(cluster, clusterName)) {
                    logger.warn("集群名称 %s,与配置不匹配(%s)", cluster, clusterName);
                }
            }
        } catch (Exception e) {
            logger.error("init elasticsearch connections failed! error:{}", e);
        }
    }

    /**
     * Description: create high rest client
     *
     * @Date: 2018/4/19
     */
    private static RestHighLevelClient newHighRestClient(String transportAddress, Boolean transportSniff, Integer pingTimeout,Map config) throws Exception {
        restClient = newLowRestClient(transportAddress, transportSniff, pingTimeout,config);
        return new RestHighLevelClient(restClient);
    }

    /**
     * Description: create low rest client
     *
     * @Date: 2018/4/19
     */
    private static RestClient newLowRestClient(String transportAddress, Boolean transportSniff, Integer pingTimeout, Map config) throws Exception {
        List<HttpHost> addressList = getHttpHost(transportAddress);
        int ioThreadCount;
        if(config.containsKey("IoThreadCount")){
            ioThreadCount = (int)config.get("IoThreadCount");
        }else{
            ioThreadCount =  Runtime.getRuntime().availableProcessors();
        }
        int maxConnPerRoute;
        if(config.containsKey("MaxConnPerRoute")){
            maxConnPerRoute = (int)config.get("MaxConnPerRoute");
        }else{
            maxConnPerRoute =  RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE;
        }
        int maxConnTotal;
        if(config.containsKey("MaxConnTotal")){
            maxConnTotal = (int)config.get("MaxConnTotal");
        }else{
            maxConnTotal =  RestClientBuilder.DEFAULT_MAX_CONN_TOTAL;
        }
        RestClientBuilder restClientBuilder = RestClient.builder(addressList.toArray(new HttpHost[addressList.size()]))
                .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                    @Override
                    public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                        int timeout = 600000;
                        if (pingTimeout != null) {
                            timeout = pingTimeout;
                        }
                        return requestConfigBuilder.setConnectTimeout(timeout)
                                .setSocketTimeout(timeout);
                    }
                })
                .setMaxRetryTimeoutMillis(10 * 60 * 1000) //超时时间设为5分钟
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    private IOReactorConfig ioReactorConfig = IOReactorConfig.custom().setIoThreadCount(ioThreadCount)
                            .setTcpNoDelay(true).setConnectTimeout(5 * 60 * 1000)
                            .setSoKeepAlive(true)
                            .build();
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        // HttpAsyncClientBuilder 详见
                        return httpClientBuilder
                                .setMaxConnPerRoute(maxConnPerRoute)
                                .setMaxConnTotal(maxConnTotal)
                                .setDefaultIOReactorConfig(ioReactorConfig)
                                .setThreadFactory(new ThreadFactory() {
                                    private final AtomicLong COUNT = new AtomicLong(1);
                                    @Override
                                    public Thread newThread(final Runnable r) {
                                        return new Thread(r, "ElasticsearchRestClient" + COUNT.getAndIncrement());
                                    }
                                });
                    }
                });
        RestClient restClient;
        if (transportSniff != null && transportSniff) {
            SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();
            restClient = restClientBuilder.setFailureListener(sniffOnFailureListener).build();
            Sniffer sniffer = Sniffer.builder(restClient)
                    .setSniffAfterFailureDelayMillis(30000)
                    .build();
            sniffOnFailureListener.setSniffer(sniffer);
        } else {
            restClient = restClientBuilder.build();
        }
        return restClient;
    }

    /**
     * Description: get httphost list list from string
     *
     * @Date: 2018/4/19
     */
    protected static List<HttpHost> getHttpHost(String transportAddress) throws UnknownHostException {
        List<HttpHost> result = new ArrayList<>();
        String[] addresses = transportAddress.split(",");
        for (String address : addresses) {
            String[] temp = address.split(":");
            String ip = temp[0].trim();
            String port = temp[1].trim();
            result.add(new HttpHost(InetAddress.getByName(ip), Integer.parseInt(port)));
        }
        return result;
    }



    public static List<String> getIndexName(long startTime, long endTime, String indexPrefix) {

        TimeShiftUtil tsu = TimeShiftUtil.getInstance();
        boolean bool = true;
        List<String> rsList = new ArrayList<String>();

        while (bool) {
            if (startTime > endTime) {
                bool = false;

                if (tsu.longToStringGetYMD(startTime).equals(tsu.longToStringGetYMD(endTime))) {
                    String tableName = indexPrefix + tsu.longToStringGetYMD(startTime);
                    /*if (EsIndexManagement.isIndexEnable(restClient, tableName)) {
                        rsList.add(tableName);
                    }*/
                    rsList.add(tableName);
                }
                break;
            } else {
                String tableName = indexPrefix + tsu.longToStringGetYMD(startTime);
                /*if (EsIndexManagement.isIndexEnable(restClient, tableName)) {
                    rsList.add(tableName);
                }*/
                rsList.add(tableName);
            }
            startTime = startTime + 1000 * 60 * 60 * 24L;
        }
        return rsList;
    }


    protected static BulkProcessor createBulkProcessor(){
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.debug("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request,
                                  BulkResponse response) {
                if (response.hasFailures()) {
                    logger.warn("Bulk [{}] executed with failures, msg:{}", executionId, response.buildFailureMessage());

                } else {
                    logger.debug("Bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Failed to execute bulk", failure);
            }
        };

        return new BulkProcessor.Builder(esClient::bulkAsync, listener, new ThreadPool(Settings.EMPTY))
                .setBulkActions(1000)
                .setBulkSize(new ByteSizeValue(10, ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(5))
                .setConcurrentRequests(16)
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();
    }

    static final XContentType REQUEST_BODY_CONTENT_TYPE = XContentType.JSON;
    private static String endpoint(String... parts) {
        StringJoiner joiner = new StringJoiner("/", "/", "");
        for (String part : parts) {
            if (Strings.hasLength(part)) {
                joiner.add(part);
            }
        }
        return joiner.toString();
    }
    private static String endpoint(String[] indices, String[] types, String endpoint) {
        return endpoint(String.join(",", indices), String.join(",", types), endpoint);
    }
    private static HttpEntity createEntity(ToXContent toXContent, XContentType xContentType) throws IOException {
        BytesRef source = XContentHelper.toXContent(toXContent, xContentType, false).toBytesRef();
        return new ByteArrayEntity(source.bytes, source.offset, source.length, createContentType(xContentType));
    }
    @SuppressForbidden(reason = "Only allowed place to convert a XContentType to a ContentType")
    public static ContentType createContentType(final XContentType xContentType) {
        return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
    }
    private static EsDSLHandler_2x dslHandler2x = new EsDSLHandler_2x();
    public static Map<String, List<Map<String, Object>>> search2xWithAggregation(RestClient client,  SearchRequest searchRequest) throws IOException {
        String endpoint = endpoint(searchRequest.indices(), searchRequest.types(), "_search");
        Params params = Params.builder();
        params.putParam(RestSearchAction.TYPED_KEYS_PARAM, "true");
        params.withRouting(searchRequest.routing());
        params.withPreference(searchRequest.preference());
        params.withIndicesOptions(searchRequest.indicesOptions());
        params.putParam("search_type", searchRequest.searchType().name().toLowerCase(Locale.ROOT));
        if (searchRequest.requestCache() != null) {
            params.putParam("request_cache", Boolean.toString(searchRequest.requestCache()));
        }
        params.putParam("batched_reduce_size", Integer.toString(searchRequest.getBatchedReduceSize()));
        if (searchRequest.scroll() != null) {
            params.putParam("scroll", searchRequest.scroll().keepAlive());
        }

        String json = dslHandler2x.handleJson(searchRequest.source().toString());
        HttpEntity entity = new NStringEntity(json, ContentType.APPLICATION_JSON);
        Map<String,String> parameters = new HashMap<>(params.getParams());
        parameters.put("typed_keys", "false");//false不修改返回的聚合条件的别名，如果为true，会在别名前加sterms#
        parameters.put("ignore_unavailable", "true");//忽略不可用的索引，这包括不存在的索引或关闭的索引
        Response response = client.performRequest(HttpGet.METHOD_NAME, endpoint,parameters, entity);
        String responseJson = EntityUtils.toString(response.getEntity(), Consts.UTF_8);
        JSONObject responseJSObject = JSONObject.parseObject(responseJson);
        JSONObject aggregations = responseJSObject.getJSONObject("aggregations");
        Map<String, List<Map<String, Object>>> aggMap = new HashMap<>();
        if(aggregations != null) {
            Iterator<Map.Entry<String, Object>> iterator = aggregations.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                JSONArray buckets = (JSONArray) ((JSONObject) entry.getValue()).get("buckets");
                List<Map<String, Object>> bucketList = buckets.stream().map(o -> {
                    JSONObject m = (JSONObject) o;
                    return new HashMap<String, Object>(m);
                }).collect(Collectors.toList());
                aggMap.put(entry.getKey(), bucketList);
            }
        }
        return aggMap;
    }
    static class EsDSLHandler_2x{

        public String handleJson(String json) {
            JSONObject jsonObject = JSONObject.parseObject(json);
            //用于兼容fetchSource
            parseFetchSource(jsonObject);
            //用于兼容query和agg
            parserJsonObject(jsonObject);
            return jsonObject.toJSONString();
        }

        private void parseFetchSource(JSONObject jsonObject){
            //5.x 查询保留字段默认为stored_fields， 2.x 为fields
            Object storedFields = jsonObject.get("stored_fields");
            jsonObject.remove("stored_fields");
            jsonObject.put("fields", storedFields);
        }

        private void parserJsonObject(JSONObject jsonObject){
            convertJson(jsonObject);
            for(String key :jsonObject.keySet()){
                Object value = jsonObject.get(key);
                if(value instanceof JSONObject){
                    parserJsonObject((JSONObject)value);
                } else if(value instanceof JSONArray){
                    parserJsonArray((JSONArray) value);
                }
            }
        }

        private void parserJsonArray(JSONArray jsonArray){
            for (Object value : jsonArray) {
                if (value instanceof JSONObject) {
                    parserJsonObject((JSONObject) value);
                } else if (value instanceof JSONArray) {
                    parserJsonArray((JSONArray) value);
                }
            }
        }

        private void convertJson(JSONObject jsonObject){
            //兼容2.x版本，移除exists下的权重加分，其他待补充
            if(jsonObject.containsKey("exists")){
                removeBoost(jsonObject, "exists");
            }
            //将5.x 版本的date_histogram查询翻译成 2.x的语句,可兼容2.x以上版本
            if(jsonObject.containsKey("date_histogram")){
                parseDateHistogram(jsonObject);
            }
            //当有查询语句中有脚本
            if(jsonObject.containsKey("script")){
                parseScript(jsonObject);
            }
        }

        //移除指定查询字段下的boost
        private void removeBoost(JSONObject jsonObject, String field){
            JSONObject json = jsonObject.getJSONObject(field);
            if(json.containsKey("boost")){
                jsonObject.remove("boost");
            }
        }

        private void parseDateHistogram(JSONObject jsonObject){
            JSONObject date = jsonObject.getJSONObject("date_histogram");
            if(org.apache.commons.lang3.StringUtils.isNumeric(date.getString("offset"))){
                date.put("offset",date.getLong("offset")/1000 + "s");
            }
            if(org.apache.commons.lang3.StringUtils.isNumeric(date.getString("interval"))){
                date.put("interval",date.getLong("interval")/1000 + "s");
            }
            JSONObject extend = date.getJSONObject("extended_bounds");
            if(extend.get("min") instanceof String){
                extend.put("min",Long.parseLong((String)extend.get("min")));
            }
            if(extend.get("max") instanceof String){
                extend.put("max",Long.parseLong((String)extend.get("max")));
            }
        }

        private void parseScript(JSONObject jsonObject){
            //将"source"改为"inline"
            JSONObject script = jsonObject.getJSONObject("script");
            if(script.containsKey("source")){
                //JSONObject source = script.getJSONObject("source");
                //source里包含脚本，可能很复杂，不用json解析
                Object source = script.get("source");
                script.put("inline", source);
                script.remove("source");
            }
            //将"lang": "painless"改为"lang": "groovy"
            if(script.containsKey("lang")){
                script.replace("lang", "groovy");
            }
        }

    }
    static class Params {
        private final Map<String, String> params = new HashMap<>();

        private Params() {
        }

        Params putParam(String key, String value) {
            if (Strings.hasLength(value)) {
                if (params.putIfAbsent(key, value) != null) {
                    throw new IllegalArgumentException("Request parameter [" + key + "] is already registered");
                }
            }
            return this;
        }

        Params putParam(String key, TimeValue value) {
            if (value != null) {
                return putParam(key, value.getStringRep());
            }
            return this;
        }

        Params withDocAsUpsert(boolean docAsUpsert) {
            if (docAsUpsert) {
                return putParam("doc_as_upsert", Boolean.TRUE.toString());
            }
            return this;
        }

        Params withFetchSourceContext(FetchSourceContext fetchSourceContext) {
            if (fetchSourceContext != null) {
                if (fetchSourceContext.fetchSource() == false) {
                    putParam("_source", Boolean.FALSE.toString());
                }
                if (fetchSourceContext.includes() != null && fetchSourceContext.includes().length > 0) {
                    putParam("_source_include", String.join(",", fetchSourceContext.includes()));
                }
                if (fetchSourceContext.excludes() != null && fetchSourceContext.excludes().length > 0) {
                    putParam("_source_exclude", String.join(",", fetchSourceContext.excludes()));
                }
            }
            return this;
        }

        Params withParent(String parent) {
            return putParam("parent", parent);
        }

        Params withPipeline(String pipeline) {
            return putParam("pipeline", pipeline);
        }

        Params withPreference(String preference) {
            return putParam("preference", preference);
        }

        Params withRealtime(boolean realtime) {
            if (realtime == false) {
                return putParam("realtime", Boolean.FALSE.toString());
            }
            return this;
        }

        Params withRefresh(boolean refresh) {
            if (refresh) {
                return withRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
            return this;
        }

        Params withRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
            if (refreshPolicy != WriteRequest.RefreshPolicy.NONE) {
                return putParam("refresh", refreshPolicy.getValue());
            }
            return this;
        }

        Params withRetryOnConflict(int retryOnConflict) {
            if (retryOnConflict > 0) {
                return putParam("retry_on_conflict", String.valueOf(retryOnConflict));
            }
            return this;
        }

        Params withRouting(String routing) {
            return putParam("routing", routing);
        }

        Params withStoredFields(String[] storedFields) {
            if (storedFields != null && storedFields.length > 0) {
                return putParam("stored_fields", String.join(",", storedFields));
            }
            return this;
        }

        Params withTimeout(TimeValue timeout) {
            return putParam("timeout", timeout);
        }

        Params withVersion(long version) {
            if (version != Versions.MATCH_ANY) {
                return putParam("version", Long.toString(version));
            }
            return this;
        }

        Params withVersionType(VersionType versionType) {
            if (versionType != VersionType.INTERNAL) {
                return putParam("version_type", versionType.name().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withWaitForActiveShards(ActiveShardCount activeShardCount) {
            if (activeShardCount != null && activeShardCount != ActiveShardCount.DEFAULT) {
                return putParam("wait_for_active_shards", activeShardCount.toString().toLowerCase(Locale.ROOT));
            }
            return this;
        }

        Params withIndicesOptions(IndicesOptions indicesOptions) {
            putParam("ignore_unavailable", Boolean.toString(indicesOptions.ignoreUnavailable()));
            putParam("allow_no_indices", Boolean.toString(indicesOptions.allowNoIndices()));
            String expandWildcards;
            if (indicesOptions.expandWildcardsOpen() == false && indicesOptions.expandWildcardsClosed() == false) {
                expandWildcards = "none";
            } else {
                StringJoiner joiner  = new StringJoiner(",");
                if (indicesOptions.expandWildcardsOpen()) {
                    joiner.add("open");
                }
                if (indicesOptions.expandWildcardsClosed()) {
                    joiner.add("closed");
                }
                expandWildcards = joiner.toString();
            }
            putParam("expand_wildcards", expandWildcards);
            return this;
        }

        Map<String, String> getParams() {
            return Collections.unmodifiableMap(params);
        }

        static Params builder() {
            return new Params();
        }
    }


    public static void deleteIndex(String... index){
        String method = "DELETE";
        for (String i : index) {
            try {
                if(existsIndex(i)) {
                    Response response = restClient.performRequest(method, "/" + i);
                    logger.info("delete index[{}] status: {}", EntityUtils.toString(response.getEntity()));
                }
                else {
                    logger.info("[{}] index not exists", i);
                }
            } catch (Exception e) {
                logger.error("delete index err:{}", e.getMessage());
            }
        }
    }

    public static boolean existsIndex(String index){
        String method = "HEAD";
        try {
            Response response = restClient.performRequest(method, "/" + index);
            if(response.getStatusLine().getStatusCode() == 200){
                return true;
            }
        } catch (Exception e) {
            return false;
        }
        return false;
    }

    public static void update(String index, String type, String id, Map data, boolean force){
        if(force) {
            try {
                esClient.update(new UpdateRequest(index, type, id)
                        .doc(data).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
            } catch (Exception e) {
                logger.error("update forced err:{}", e);
            }
        }
        else {
            update(index, type, id, data);
        }
    }

    public static void update(String index, String type, String id, Map data){
        bulkProcessor.add(new UpdateRequest(index, type, id).doc(data));
    }

    public static void insert(String index, String type, String id, Map data, boolean force){
        if(force) {
            try {
                esClient.index(new IndexRequest(index, type, id)
                        .source(data).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE));
            } catch (Exception e) {
                logger.error("insert forced err:{}", e);
            }
        }
        else {
            insert(index, type, id, data);
        }
    }

    public static void insert(String index, String type, String id, Map data){
        IndexRequest indexRequest = new IndexRequest(index, type, id).source(data);
        bulkProcessor.add(indexRequest);
    }


    public static List<Map> searchByScan(String index, String type, QueryBuilder query){
        List<Map> retList = new ArrayList<>();
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
            searchRequest = type != null ? searchRequest.types(type) : searchRequest;
            searchRequest.scroll(TimeValue.timeValueMinutes(30));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(query);
            searchSourceBuilder.size(1000);

            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = esClient.search(searchRequest);
            if(logger.isDebugEnabled()){
                logger.debug("[searchWithScroll] - searchRequest:{}, searchResponse:{}", searchRequest, searchResponse);
            }
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            while (searchHits != null && searchHits.length > 0) {
                for (SearchHit hit : searchHits) {
                    retList.add(hit.getSource());
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(30));
                searchResponse = esClient.searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }
            try {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                ClearScrollResponse clearScrollResponse = esClient.clearScroll(clearScrollRequest);
                logger.debug("clear scroll handle :{}", clearScrollResponse.isSucceeded());
            } catch (Exception e) {
            }

            return retList;

        } catch (Exception e) {
            logger.error("search event error can't get es client error detail: {}", e);
        }
        return retList;
    }

    public static List<Map> searchThenTermAggregation(String index, String type, QueryBuilder query, AggregationBuilder aggregation){
        List<Map> retList = new ArrayList<>();
        try {
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.indicesOptions(IndicesOptions.fromOptions(true, true, true, false));
            searchRequest = type != null ? searchRequest.types(type) : searchRequest;
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(query).aggregation(aggregation);
            searchSourceBuilder.size(0);
            searchRequest.source(searchSourceBuilder);

            if(is2xVersion){
                Map<String, List<Map<String, Object>>> aggregationMap = search2xWithAggregation(restClient, searchRequest);
                if(aggregationMap != null && !aggregationMap.isEmpty()){
                    if(aggregation instanceof TermsAggregationBuilder){
                        aggregationMap.get(aggregation.getName()).forEach(m->{
                            Map termsMap = new HashMap();
                            if(m.get("key") != null) {
                                termsMap.put("key", m.get("key").toString());
                                termsMap.put("doc_count", m.get("doc_count"));
                                retList.add(termsMap);
                            }
                        });
                    }
                    else if(aggregation instanceof DateHistogramAggregationBuilder){
                        aggregationMap.get(aggregation.getName()).forEach(m->{
                            Map dateMap = new HashMap();
                            dateMap.put("key", m.get("key"));
                            dateMap.put("keyAsString", m.get("keyAsString"));
                            dateMap.put("docCount", m.get("doc_count"));
                            retList.add(dateMap);
                        });
                    }
                }
                else {
                    logger.warn("aggregation failed in 2.x");
                }
            }
            else {
                SearchResponse searchResponse = esClient.search(searchRequest);
                if (logger.isDebugEnabled()) {
                    logger.debug("[searchWithAggregation] - searchRequest:{}, searchResponse:{}", searchRequest, searchResponse);
                }
                Map<String, Aggregation> map = searchResponse.getAggregations().asMap();
                Iterator<Map.Entry<String, Aggregation>> iter = map.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry<String, Aggregation> entry = iter.next();
                    String aggName = entry.getKey();
                    Aggregation agg = entry.getValue();
                    if (agg instanceof Terms) {

                        for (Terms.Bucket bucket : ((Terms) agg).getBuckets()) {
                            if (bucket.getKey() == null) continue;
                            Map termsMap = new HashMap();
                            termsMap.put("key", bucket.getKey());
                            termsMap.put("doc_count", bucket.getDocCount());
                            retList.add(termsMap);
                        }
                    } else if (agg instanceof ParsedDateHistogram) {
                        for (Histogram.Bucket bucket : ((ParsedDateHistogram) agg).getBuckets()) {
                            Map dateMap = new HashMap();
                            if (bucket.getKey() instanceof DateTime) {
                                dateMap.put("key", ((DateTime) bucket.getKey()).getMillis());
                            }
                            dateMap.put("keyAsString", bucket.getKeyAsString());
                            dateMap.put("docCount", bucket.getDocCount());
                            retList.add(dateMap);
                        }
                    }
                    /** TODO - 先实现Terms的聚合
                     else if(agg instanceof XXX){

                     }*/
                }
            }
            return retList;

        } catch (Exception e) {
            logger.error("search event error can't get es client error detail: {}", e);
        }
        return retList;
    }
}
