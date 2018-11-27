package com.hansight.v5.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by ck on 2018/1/16.
 */
@Service
public class IPService {
    private final static Logger LOG = LoggerFactory.getLogger(IPService.class);
    private static Map<IPRange, String> systemIntranets = new HashMap<>();//非线程安全，所以需要搭配锁使用

    @Autowired
    private SysCfgInfo sysCfgInfo;

    @PostConstruct
    private void init(){
        Map<String, List<String>> ipMap = sysCfgInfo.getSystemIntranet();
        if(ipMap != null) {
            ipMap.entrySet().stream().forEach(m-> {
                m.getValue().forEach(
                        va ->systemIntranets.put(new IPRange(va), m.getKey())
                );
            });
            LOG.info("system intranet settings: {}", ipMap.keySet());
        } else {
            LOG.warn("can not found system intranet infos");
        }
    }

    /**
     * 私有IP：A类  10.0.0.0-10.255.255.255
     * B类  172.16.0.0-172.31.255.255
     * C类  192.168.0.0-192.168.255.255
     * 当然，还有127这个网段是环回地址
     **/
    private static long aBegin, aEnd, bBegin, bEnd, cBegin, cEnd, localhost;

    static {
        aBegin = ip2Long("10.0.0.0");
        aEnd = ip2Long("10.255.255.255");
        bBegin = ip2Long("172.16.0.0");
        bEnd = ip2Long("172.31.255.255");
        cBegin = ip2Long("192.168.0.0");
        cEnd = ip2Long("192.168.255.255");
        localhost = ip2Long("127.0.0.1");
    }

    public static boolean isIntranet(String ip) {
        if(isSystemIntranet(ip)) return true;
        if (isTraditinalIntrenat(ip)) {
            return true;
        }
        return false;
    }

    public static boolean isSystemIntranet(long ip) {
        for (IPRange ir : systemIntranets.keySet()) {
            if (ir.inRange(ip)) {
                return true;
            }
        }
        return false;
    }

    public static String getSystemIntranetName(String ip) {
        long ipValue = ip2Long(ip);
        Iterator<Map.Entry<IPRange, String>> it = systemIntranets.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<IPRange, String> item = it.next();
            if (item.getKey().inRange(ipValue)) {
                return item.getValue();
            }
        }
        return "";
    }

    public static boolean isSystemIntranet(String ip) {
        if (ip == null || ip.isEmpty()) return false;
        return isSystemIntranet(ip2Long(ip));
    }

    public static boolean isTraditinalIntranet(long ip) {
        return (aBegin <= ip && ip <= aEnd) ||
                (bBegin <= ip && ip <= bEnd) ||
                (cBegin <= ip && ip <= cEnd) ||
                ip == localhost;
    }

    public static boolean isTraditinalIntrenat(String ip) {
        if (ip == null || ip.isEmpty()) return false;
        return isTraditinalIntranet(ip2Long(ip));
    }

    public static long ip2Long(String ip) {
        long ip2long = 0L;
        try {
            ip = ip.trim();
            StringTokenizer stringTokenizer = new StringTokenizer(ip, ".");
            String[] ips = new String[4];
            int k = 0;
            while (stringTokenizer.hasMoreElements()){
                ips[k++] = stringTokenizer.nextToken();
                if(k >=4) break;
            }
            //非IP数据判断
            if(k < 4){
                return -1;
            }
            for (int i = 0; i < 4; ++i) {
                ip2long = ip2long << 8 | Integer.parseInt(ips[i]);
            }
        } catch (Exception e) {
            return -1;
        }
        return ip2long;
    }

    /**
     * 校验提供的IP地址的合法性
     *
     * @param ip 提供的IP地址
     * @return
     * @createTime: 2016年11月25日 下午10:24:23
     * @author: fengqc
     */
    public static boolean isIp(String ip) {
        return Pattern.matches("((?:(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(?:25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d))))", ip);
    }

    static class IPRange {
        long from;
        long to;

        public IPRange(String ip) {
            if (ip.contains("-")) {
                String[] ips = ip.split("-");
                if (ips.length == 2) {
                    long p1 = ip2Long(ips[0]);
                    long p2 = ip2Long(ips[1]);
                    if (p1 > p2) {
                        from = p2;
                        to = p1;
                    } else {
                        from = p1;
                        to = p2;
                    }
                }
            } else if (ip.contains("/")) {
                String[] ips = ip.split("/");
                int mask = Integer.parseInt(ips[1]);
                from = ip2Long(ips[0]) & (0xFFFFFFFF << (32 - mask));//???
                to = from + (0xFFFFFFFF >>> mask);
            } else {
                from = to = ip2Long(ip);
            }
        }

        public boolean inRange(long ip) {
            return ip >= from && ip <= to;
        }
    }
}
