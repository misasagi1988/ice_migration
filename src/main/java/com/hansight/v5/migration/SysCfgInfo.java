package com.hansight.v5.migration;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hansight.v5.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by ck on 2018/10/10.
 */
@Service
public class SysCfgInfo {
    private final static Logger LOG = LoggerFactory.getLogger(SysCfgInfo.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private String nodeChain;
    private Map<String, Integer> saeRuleType;
    private Map<String, List<String>> intranetSettings = new HashMap<>();

    // 获取当前系统节点ID
    public String getNodeChain(){
        if(nodeChain != null) return nodeChain;
        nodeChain =  jdbcTemplate.queryForObject("select id from system_node where type = 'CURRENT_NODE'", String.class);
        if(nodeChain == null || nodeChain.isEmpty()){
            LOG.error("query system node info failed, system exit...");
            System.exit(-1);
        }
        return nodeChain;
    }

    // 获取当前sae rule type info, key is name, value is id
    public Map<String, Integer> getSaeRuleType(){
        if(saeRuleType != null) return saeRuleType;
        List<Map<String, Integer>> ruleTypes =  jdbcTemplate.query("select * from sae_rule_type", new MapRowMapper());
        if(ruleTypes == null || ruleTypes.isEmpty()){
            LOG.error("query sae rule type info failed, system exit...");
            System.exit(-1);
        }
        saeRuleType = new HashMap<>(ruleTypes.size());
        ruleTypes.forEach(rt->saeRuleType.putAll(rt));
        return saeRuleType;
    }


    public Map<String, List<String>> getSystemIntranet(){
        intranetSettings.clear();
        List<Map<String, List<String>>> ret = jdbcTemplate.query("select config from system_config where type='INTRANET'", new ListRowMapper());
        if(ret == null || ret.isEmpty()){
            LOG.error("query system intranet info failed, system exit...");
            System.exit(-1);
        }
        ret.forEach(r->intranetSettings.putAll(r));
        return intranetSettings;
    }

    class MapRowMapper implements RowMapper<Map<String, Integer>>{

        @Override
        public Map<String, Integer> mapRow(ResultSet rs, int rowNum) throws SQLException {
            Map map = new HashMap(2);
            map.put(rs.getString("name"), rs.getInt("id"));
            return map;
        }
    }

    class ListRowMapper implements RowMapper<Map<String, List<String>>>{

        @Override
        public Map<String, List<String>> mapRow(ResultSet rs, int rowNum) throws SQLException {
            Map<String, List<String>> res = new HashMap<>();
            List<String> list = new ArrayList<>();
            Map m = JsonUtil.parseObject(rs.getString("config"), Map.class);
            String name = m.get("name").toString();
            if(m.get("intranet") instanceof JSONArray){
                Object[] oo = ((JSONArray) m.get("intranet")).toArray();
                for (Object o : oo){
                    if(o instanceof JSONObject){
                        list.add(((JSONObject) o).get("content").toString());
                    }
                }
            }
            res.put(name, list);
            return res;
        }
    }
}
