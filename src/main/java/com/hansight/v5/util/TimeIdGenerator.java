package com.hansight.v5.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TimeIdGenerator
 */

public class TimeIdGenerator {

    private static final long seedTimeStamp = 1503417600000L;

    private static final int workerIdBits = 6;

    private static final int sequenceBits = 5;

    private static final int workerIdShift = sequenceBits;

    private static final int timestampLeftShift = sequenceBits + workerIdBits;

    private static final int sequenceMask = -1 ^ (-1 << sequenceBits);

    private static volatile AtomicInteger sequence = new AtomicInteger(0);

    private static volatile AtomicLong lastTimestamp = new AtomicLong(-1);

    private long workerId;

    private static TimeIdGenerator instance;

    private static TimeIdGenerator getInstance() {
        if (instance == null) {
            synchronized (TimeIdGenerator.class) {
                if (instance == null) {
                    instance = new TimeIdGenerator();
                    BufferedReader br = null;
                    try {
                        br = new BufferedReader(new FileReader("workerid"));
                        instance.workerId = Long.valueOf(br.readLine());
                    } catch (Exception e) {
                        instance.workerId = 0L;
                    } finally {
                        try {
                            if (br != null)
                                br.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        return instance;
    }

    /**
     * 生成带时间序列的ID
     *
     * @param time
     * @return
     */
    public synchronized static Long generate(Long time) {
        long timestamp = timeGen();

        long savedTimestamp = lastTimestamp.get();
        if (timestamp < savedTimestamp) {
            // 时钟回退，加1毫秒
            timestamp = savedTimestamp + 1;
        }

        // 如果是同一时间生成的，则进行毫秒内序列
        int seq = 0;
        if (savedTimestamp == timestamp) {
            seq = sequence.incrementAndGet() & sequenceMask;

            // 毫秒内序列溢出
            if (seq == 0) {
                timestamp = tilNextMillis(savedTimestamp);
            }
        } else {
            seq = 0;
        }

        sequence.set(seq);
        lastTimestamp.set(timestamp);

        // 用时间戳在前能得到更大的区间
        return ((timestamp - seedTimeStamp) << timestampLeftShift)
                | (getInstance().workerId << workerIdShift)
                | seq;
    }

    public synchronized static Long generate(Long time, int workerOrder) {
        long timestamp = timeGen();

        long savedTimestamp = lastTimestamp.get();
        if (timestamp < savedTimestamp) {
            // 时钟回退，加1毫秒
            timestamp = savedTimestamp + 1;
        }

        // 如果是同一时间生成的，则进行毫秒内序列
        int seq = 0;
        if (savedTimestamp == timestamp) {
            seq = sequence.incrementAndGet() & sequenceMask;

            // 毫秒内序列溢出
            if (seq == 0) {
                timestamp = tilNextMillis(savedTimestamp);
            }
        } else {
            seq = 0;
        }

        sequence.set(seq);
        lastTimestamp.set(timestamp);

        // 用时间戳在前能得到更大的区间
        return ((timestamp - seedTimeStamp) << timestampLeftShift)
                | (workerOrder << workerIdShift)
                | seq;
    }

    private static long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    private static long timeGen() {
        return System.currentTimeMillis();
    }

    private final static int  shiftYMD     = 47;
    private final static int  shiftDay     = 0;
    private final static int  shiftMonth   = 5;
    private final static int  shiftYear    = 9;
    private final static long maxDefaultId = 0x7FFFFFFFFFFFL;

    public static String wrapYMD2ID(long ID, long time) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(time);
        long id = ((0x001FL & cal.get(Calendar.DAY_OF_MONTH)) << shiftDay |
                (0x000FL & (cal.get(Calendar.MONTH) + 1)) << shiftMonth |
                (0x007FL & (cal.get(Calendar.YEAR) - 2000)) << shiftYear) << shiftYMD | ID;
        return id + "";
    }

    public static String wrapYMD2ID(long ID, String ymd) {
        Calendar cal = Calendar.getInstance();
        cal.set(Integer.parseInt(ymd.substring(0, 4)), Integer.parseInt(ymd.substring(4,6)), Integer.parseInt(ymd.substring(6, ymd.length())));
        long id = ((0x001FL & cal.get(Calendar.DAY_OF_MONTH)) << shiftDay |
                (0x000FL & (cal.get(Calendar.MONTH) + 1)) << shiftMonth |
                (0x007FL & (cal.get(Calendar.YEAR) - 2000)) << shiftYear) << shiftYMD | ID;
        return id + "";
    }

    public static String getYMDFromID(long id) {
        // 兼容未包装的id，策略是从id取出生成时间
        if (id <= maxDefaultId) {
            return TimeShiftUtil.getInstance().longToStringGetYMD((id >> timestampLeftShift) + seedTimeStamp);
        }
        return String.format("20%02d%02d%02d",
                id >> (shiftYMD + shiftYear) & 0x007FL,
                id >> (shiftYMD + shiftMonth) & 0x000FL,
                id >> shiftYMD & 0x001FL);
    }

    public static String getYMDFromID(String id) {
        return getYMDFromID(Long.parseLong(id));
    }
}
