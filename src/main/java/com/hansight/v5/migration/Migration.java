package com.hansight.v5.migration;

import com.hansight.v5.util.*;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by ck on 2018/10/10.
 */
@Service
public class Migration implements Runnable {
    private final static Logger LOG = LoggerFactory.getLogger(Migration.class);

    private static final String DB_PATH = "./cache";

    private static final String DB_MIGRATION_DATE = "md";
    private static final String MIGRATED_DATE_KEY = "migrated_date";

    private static final String DB_INCIDENT = "ict";

    private static final long Hour_24_Mill = 24 * 60 * 60 * 1000;

    private static final String Alarm_3x_Index = "alarm_";
    private static final String Alarm_3x_Type = "alarm";

    private static final String Alarm_5x_Index = "incident_alarm_";
    private static final String Alarm_5x_Type = "alarm";

    private static final String Incident_5x_Index_Local = "incident_local";
    private static final String Incident_5x_Type = "incident";

    private static final String Merge_5x_Index = "incident_merge";
    private static final String Merge_5x_Type = "alarm_merge";

    private static final String Related_5x_Index = "incident_related_";
    private static final String Related_5x_Type = "related";


    private static List<String> alarm_3x_delete_fields = new ArrayList<>(
            Arrays.asList("@enrich", "@notification_ids", "@rule_meta",
                    "alarm_focus", "alarm_type",
                    "src_address_str", "dst_address_str", "occur_address_str",
                    "@alarm_source", "alarm_tag",
                    "event_ids", "ids_cnt",
                    "node_name", "node_address",
                    "dst_port_array_cnt", "dst_port_array",
                    "src_port_array_cnt", "src_port_array")
    );


    private RocksdbCache<Map> incidentCache;

    private RocksdbCache<String> mgDateCache;

    private static SimpleDateFormat showSdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    private static SimpleDateFormat yyyymmdd = new SimpleDateFormat("yyyyMMdd");
    private static SimpleDateFormat yyyymm = new SimpleDateFormat("yyyyMM");
    @Value("${migration.start_time}")
    private String startTimeDate;
    @Value("${migration.end_time}")
    private String endTimeDate;
    @Value("${elasticsearch.cluster_name}")
    private String esClusterName;
    @Value("${elasticsearch.cluster_nodes}")
    private String esClusterNodes;


    @Autowired
    private SysCfgInfo sysCfgInfo;
    /*@Autowired
    private IPService ipService;*/

    private long migrationStart;
    private long migrationEnd;

    //private EsUtil esUtil;

    @PostConstruct
    private void init() {
        LOG.info("step 1: init RocksDB cache.");
        if (!prepareCache()) {
            LOG.error("init cache failed, system exit...");
            pauseSeconds(5);
            System.exit(-1);
        }
        LOG.info("step 2: get migration time region.");
        if (mgDateCache.get(MIGRATED_DATE_KEY) == null) {
            LOG.info("first startup...");
            try {
                migrationStart = sdf.parse(startTimeDate).getTime();
                migrationEnd = sdf.parse(endTimeDate).getTime() + Hour_24_Mill -1 ;

                LOG.info("migration time region:[{} ~ {}]", showSdf.format(new Date(migrationStart)), showSdf.format(new Date(migrationEnd)));
            } catch (Exception e) {
                LOG.error("time translate failed, err:{}", e);
                pauseSeconds(5);
                System.exit(-1);
            }
        } else {
            LOG.info("re-startup...");
            try {
                migrationStart = yyyymmdd.parse(mgDateCache.get(MIGRATED_DATE_KEY)).getTime();
                LOG.info("migrated time :{}", showSdf.format(new Date(migrationStart)));
                migrationStart += Hour_24_Mill; // 缓存里面存储的日期表示已经处理完成的日期，步进一天表示将处理下一天数据
                migrationEnd = sdf.parse(endTimeDate).getTime() + Hour_24_Mill -1;
                LOG.info("migration time region:[{} ~ {}]", showSdf.format(new Date(migrationStart)), showSdf.format(new Date(migrationEnd)));

            } catch (Exception e) {
                LOG.error("time translate failed, err:{}", e);
                pauseSeconds(5);
                System.exit(-1);
            }
        }

        LOG.info("step 3: create connection to es: {}@{}", esClusterName, esClusterNodes);
        EsUtil.createClient(esClusterName, esClusterNodes);
        new Thread(this).start();
    }

    @Override
    public void run() {
        LOG.info("step 4: start migration thread.");
        String currentNodeId = sysCfgInfo.getNodeChain();
        Map<String, Integer> saeRuleType = sysCfgInfo.getSaeRuleType();
        LOG.info("current system nodeChain: {}", currentNodeId);
        LOG.debug("sae rule type info: \n {}", JsonUtil.parseToPrettyJson(saeRuleType));
        // 删除5.x的合并告警、安全事件，避免脏数据
        EsUtil.deleteIndex(Merge_5x_Index, Incident_5x_Index_Local);
        while (migrationStart < migrationEnd) {
            String indexDate = yyyymmdd.format(new Date(migrationStart));
            String yyyyMMDDDate = yyyymmdd.format(new Date(migrationStart));
            String yyyyMMDate = yyyymm.format(new Date(migrationStart));
            LOG.info("\nstart migrating [{}].............................................................\n", indexDate);
            // 删除5.x的复制原始告警、关联表，避免脏数据
            EsUtil.deleteIndex(Alarm_5x_Index + yyyyMMDate, Related_5x_Index + yyyyMMDate);

            // 聚合当前原始告警种类
            List<Map> esResponse = EsUtil.searchThenTermAggregation(Alarm_3x_Index + indexDate, Alarm_3x_Type,
                    QueryBuilders.matchAllQuery(), AggregationBuilders.terms("alarm_name").field("alarm_name").size(1000));

            if (esResponse != null) {
                for (Map m : esResponse) {
                    if (m.get("key") == null) continue;
                    String alarmName = m.get("key").toString();
                    LOG.info("process alarm: [{} - {}]", alarmName, m.get("doc_count"));

                    // 把所有的告警搜索出来
                    List<Map> alarms = EsUtil.searchByScan(Alarm_3x_Index + indexDate, Alarm_3x_Type,
                            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("alarm_name", alarmName)));

                    Map ict = incidentCache.get(alarmName);

                    Map merge = null;

                    if (ict == null) {
                        // 说明这个ict首次出现
                        ict = new HashMap();
                        ict.put("node_chain", currentNodeId);
                        ict.put("node_id", currentNodeId);
                        ict.put("@pu", "{}");
                        ict.put("advice", null);
                        ict.put("type", 10100);
                        ict.put("title", alarmName);
                        ict.put("alarm_source", alarmName);
                        ict.put("ice_rule_id", 1);
                        ict.put("handle_status", 1);
                        ict.put("ict_from", 0);
                        ict.put("confirm_status", 0);

                        ict.put("create_time", migrationStart);
                        ict.put("start_time", System.currentTimeMillis());
                        ict.put("end_time", 0L);
                        ict.put("update_time", 0L);

                        ict.put("id", TimeIdGenerator.wrapYMD2ID(
                                TimeIdGenerator.generate(migrationStart, IDSeed.INCIDENT_SEED), migrationStart));
                        ict.put("data_source_array", new String[0]);
                        ict.put("name", SequenceIdUtil.getIncrSeqId());

                        ict.put("intranet", new HashSet<>());
                        ict.put("internet", new HashSet<>());
                        ict.put("intranet_region_array", new HashSet<>());

                        LOG.info("create new incident: [title={}, name={}, id={}]", ict.get("title"), ict.get("name"), ict.get("id"));
                        // 写安全事件到5.x
                        EsUtil.insert(Incident_5x_Index_Local, Incident_5x_Type, ict.get("id").toString(), ict, true);
                    }

                    Set<String> mergeIntranet = new HashSet<>();
                    Set<String> mergeInternet = new HashSet<>();
                    Set<String> mergeIntranetRegion = new HashSet<>();
                    if (alarms != null) {
                        int number = 0;
                        for (Map alarm : alarms) {
                            if(number ++ % 500 == 0){
                                pauseSeconds(1);
                            }
                            // 删除不需要的字段
                            alarm_3x_delete_fields.forEach(f -> alarm.remove(f));
                            // 重命名指定字段
                            if(alarm.get("alarm_key") == null ||
                                    alarm.get("node_chain") == null ||
                                    alarm.get("start_time") == null ||
                                    alarm.get("end_time") == null){
                                LOG.warn("illegal alarm data: {}", alarm);
                                continue;
                            }

                            String alarmKey = alarm.remove("alarm_key").toString();
                            String nodeChain = alarm.get("node_chain").toString();
                            long startTime = (long)alarm.get("start_time");
                            long endTime = (long)alarm.get("end_time");
                            if(alarmKey.startsWith(nodeChain)){
                                if(alarmKey.indexOf('/') < 0){
                                    // 表示此告警是当前系统的告警，不是子节点的
                                    alarmKey = alarmKey.substring(nodeChain.length()+1, alarmKey.length());
                                    alarm.put("merge_key", alarmKey);
                                    alarm.put("sae_rule_id", alarm.remove("rule_id"));
                                    if(saeRuleType.containsKey(alarm.get("rule_type"))){
                                        alarm.put("sae_rule_type", saeRuleType.get(alarm.get("rule_type")));
                                    }
                                    else {
                                        alarm.put("sae_rule_type", 1);
                                    }
                                    alarm.put("id", TimeIdGenerator.wrapYMD2ID(
                                            TimeIdGenerator.generate((long)alarm.get("start_time"), IDSeed.ALERT_SEED), migrationStart));

                                    alarm.put("alarm_tag", new HashMap<>(2));
                                    alarm.put("data_source_array", new String[0]);
                                    alarm.put("alarm_advice", null);
                                    alarm.put("create_time", migrationStart);
                                    alarm.put("@sae_template_type", 1);

                                    // TODO - 处理内外网数组
                                    Set<String> ips =getStringSet(alarm, "src_address_array");
                                    ips.addAll(getStringSet(alarm, "dst_address_array"));

                                    Set<String> intranet = new HashSet<>();
                                    Set<String> internet = new HashSet<>();
                                    Set<String> intranetRegion = new HashSet<>();
                                    ips.forEach(ip->{
                                        if(IPService.isIntranet(ip)){
                                            intranet.add(ip);
                                            String regionName = IPService.getSystemIntranetName(ip);
                                            if(StringUtils.isNotBlank(regionName)) {
                                                intranetRegion.add(regionName);
                                            }
                                        }
                                        else {
                                            internet.add(ip);
                                        }
                                    });
                                    alarm.put("intranet", intranet);
                                    alarm.put("internet", internet);
                                    alarm.put("intranet_region_array", intranetRegion);

                                    mergeIntranet.addAll(intranet);
                                    mergeInternet.addAll(internet);
                                    mergeIntranetRegion.addAll(intranetRegion);
                                    // 写原始告警到5.x
                                    EsUtil.insert(Alarm_5x_Index+yyyyMMDate, Alarm_5x_Type, alarm.get("id").toString(), alarm);


                                    // 新建今天的合并告警
                                    if(merge == null){
                                        merge = new HashMap(alarm);
                                        merge.remove("src_address_array");
                                        merge.remove("dst_address_array");
                                        merge.remove("@ctime");
                                        merge.remove("@alarm_source");
                                        merge.remove("dst_address_array_cnt");
                                        merge.remove("src_address_array_cnt");
                                        merge.remove("node_name");
                                        merge.remove("alarm_tag");
                                        merge.remove("event_id");
                                        merge.remove("ids_cnt");


                                        merge.put("id", TimeIdGenerator.wrapYMD2ID(
                                                TimeIdGenerator.generate((long)merge.get("start_time"), IDSeed.MERGE_ALERT_SEED), migrationStart));
                                        merge.put("source_type", 0);
                                        merge.put("confirm_status", 0);
                                        merge.put("create_time", migrationStart);
                                        merge.put("incident_id", ict.get("id"));
                                        merge.put("intranet", new HashSet<>());
                                        merge.put("internet", new HashSet<>());
                                        merge.put("intranet_region_array", new HashSet<>());
                                        merge.put("merge_key", ict.get("id") + "-" + yyyyMMDDDate + "-" + alarmKey);


                                        switch ((int)merge.get("alarm_level")){
                                            case 0:
                                                ict.put("priority", 0);
                                                ict.put("severity", 0);
                                                break;
                                            case 1:
                                                ict.put("priority", 25);
                                                ict.put("severity", 1);
                                                break;
                                            case 2:
                                                ict.put("priority", 50);
                                                ict.put("severity", 2);
                                                break;
                                            case 3:
                                                ict.put("priority", 75);
                                                ict.put("severity", 3);
                                                break;
                                            default:
                                                LOG.warn("found unknown alarm level={}, set default", merge.get("alarm_level"));
                                                ict.put("priority", 0);
                                                ict.put("severity", 0);
                                        }
                                        ict.put("attack_phase", new Integer[]{(int)merge.get("alarm_stage")});
                                    }
                                    if(startTime < (long)merge.get("start_time")){
                                        merge.put("start_time", startTime);
                                    }
                                    if(endTime > (long)merge.get("end_time")){
                                        merge.put("end_time", endTime);
                                    }

                                    if(startTime < (long)ict.get("start_time")){
                                        ict.put("start_time", startTime);
                                    }
                                    if(endTime > (long)ict.get("update_time")){
                                        ict.put("end_time", endTime);
                                        ict.put("update_time", endTime);
                                    }

                                    // 写关联信息到5.x
                                    Map related = new HashMap(8);
                                    related.put("incident_id", ict.get("id"));
                                    related.put("start_time", startTime);
                                    related.put("create_time", migrationStart);
                                    related.put("merge_alert_id", merge.get("id"));
                                    related.put("alert_id", alarm.get("id"));
                                    related.put("concern_field", new String[0]);
                                    related.put("id", TimeIdGenerator.wrapYMD2ID(
                                            TimeIdGenerator.generate((long)alarm.get("start_time"), IDSeed.RELATION_SEED), migrationStart));
                                    EsUtil.insert(Related_5x_Index+yyyyMMDate, Related_5x_Type, related.get("id").toString(), related);
                                }
                                else {
                                    LOG.warn("found child node data={}, skip", alarm);
                                    continue;
                                }
                            }
                            else {
                                LOG.warn("nodeChain not matched: [alarm-nodechain={}, system-nodechain={]]", nodeChain, currentNodeId);
                                continue;
                            }
                        }
                        if(merge != null) {
                            merge.put("intranet", mergeIntranet);
                            merge.put("internet", mergeInternet);
                            merge.put("intranet_region_array", mergeIntranetRegion);
                            merge.put("alarm_count", alarms.size());
                            // 写合并告警到5.x
                            LOG.info("create new merge: [name={}, id={}, count={}]", merge.get("alarm_name"), merge.get("id"), merge.get("alarm_count"));
                            EsUtil.insert(Merge_5x_Index, Merge_5x_Type, merge.get("id").toString(), merge);
                        } else {
                            LOG.info("no merge data for alarm: {}.", alarmName);
                        }
                    }
                    // TODO - 处理内外网数组
                    getStringSet(ict, "intranet").addAll(mergeIntranet);
                    getStringSet(ict, "internet").addAll(mergeInternet);
                    getStringSet(ict, "intranet_region_array").addAll(mergeIntranetRegion);

                    LOG.debug("update incident:{}", JsonUtil.parseToPrettyJson(ict));
                    EsUtil.update(Incident_5x_Index_Local,
                            Incident_5x_Type, ict.get("id").toString(), ict, true);
                    incidentCache.put(alarmName, ict);

                    if((long)ict.get("update_time") - (long)ict.get("start_time") >= 30 * Hour_24_Mill){
                        incidentCache.expire(alarmName);
                    }
                }
            }
            LOG.info("\nmigrating [{}].............................................................OK\n", indexDate);
            mgDateCache.put(MIGRATED_DATE_KEY, indexDate);
            migrationStart += Hour_24_Mill;
        }

        LOG.info("ice migration finish, system exit...");
        pauseSeconds(30);
        System.exit(0);
    }


    private boolean prepareCache() {
        try {
            mgDateCache = new RocksdbCache(DB_PATH, DB_MIGRATION_DATE, String.class);
            incidentCache = new RocksdbCache(DB_PATH, DB_INCIDENT, Map.class);
        } catch (Exception e) {
            LOG.error("prepare cache failed, err:{}", e);
            closeCache();
            return false;
        }
        return true;
    }


    private void closeCache() {
        if (mgDateCache != null) {
            mgDateCache.close();
        }
        if (incidentCache != null) {
            incidentCache.close();
        }
    }


    private static Set<String> getStringSet(Map data, String key){
        if(data.get(key) == null) {
            Set s = new HashSet<>();
            data.put(key, s);
            return s;
        }
        Object o = data.get(key);

        if(o instanceof String){
            return new HashSet(Arrays.asList(((String) o).split(",")));
        }
        if(o instanceof Set){
            return (Set)o;
        }
        if(o instanceof Collection){
            Set<String> list = new HashSet<>();
             ((Collection) o).forEach(m->{
                 if(m != null)
                    list.add(m.toString());
             });
            data.put(key, list);
             return list;
        }
        return new HashSet<>();
    }



    private void pauseSeconds(int seconds){
        try {
            Thread.sleep(seconds * 1000);
        } catch (Exception e) {
        }
    }
}
