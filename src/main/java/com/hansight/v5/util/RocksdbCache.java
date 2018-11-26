package com.hansight.v5.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.*;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by ck on 2018/7/30.
 */
public class RocksdbCache<T> {
    protected static final Charset UTF_8 = StandardCharsets.UTF_8;

    private static final Map<String, RocksdbCache>                       OPENED_CACHE = new ConcurrentHashMap<>();
    private              com.google.common.cache.LoadingCache<String, T> memoryCache  = CacheBuilder.newBuilder()
            .concurrencyLevel(4)
            .initialCapacity(10000)
            .maximumSize(10000)
            //.expireAfterAccess(1, TimeUnit.HOURS)
            .build(new CacheLoader<String, T>() {
                @Override
                public T load(String key) {
                    return getFromDb(key);
                }
            });
    private              Class<T>                                        entityClass;

    static {
        RocksDB.loadLibrary();
    }

    protected static final ColumnFamilyOptions      cfOptions              =
            new ColumnFamilyOptions().optimizeUniversalStyleCompaction()
                    .setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
    protected static final DBOptions                dbOptions              = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
    protected final        List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

    protected RocksDB            db = null;
    protected ColumnFamilyHandle currentCfh;
    protected String             absPath;

    public RocksdbCache(String path, String database, Class<T> tClass) throws RocksDBException {
        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                new ColumnFamilyDescriptor(database.getBytes(UTF_8), cfOptions)
                                                                        );
        File dir = new File(path + "/" + database);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        absPath = dir.getAbsolutePath();
        if (OPENED_CACHE.containsKey(absPath)) {
            db = OPENED_CACHE.get(absPath).getDb();
            currentCfh = OPENED_CACHE.get(absPath).getCurrentCfh();
        } else {
            db = RocksDB.open(dbOptions, path + "/" + database, cfDescriptors, columnFamilyHandleList);
            currentCfh = columnFamilyHandleList.get(1);
            OPENED_CACHE.put(absPath, this);
        }
        entityClass = tClass;
    }

    private RocksDB getDb() {
        return db;
    }

    private ColumnFamilyHandle getCurrentCfh() {
        return currentCfh;
    }

    public void close() {
        for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandleList) {
            columnFamilyHandle.close();
        }
        columnFamilyHandleList.clear();

        if (db != null) {
            db.close();
            db = null;
        }
        dbOptions.close();
        cfOptions.close();
        OPENED_CACHE.remove(absPath);
        memoryCache.invalidateAll();
    }

    /**
     * 性能差，不推荐使用
     *
     * @param key
     * @return
     */
    public boolean containsKey(String key) {
        return get(key) == null;
    }

    public void put(String key, T o) {
        if (StringUtils.isEmpty(key) || o == null) {
            return;
        }
        try {
            if (o instanceof String) {
                db.put(currentCfh, key.getBytes(), o.toString().getBytes());
            } else {
                db.put(currentCfh, key.getBytes(), JsonUtil.toJsonStr(o).getBytes());
            }
            memoryCache.put(key, o);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void expire(String key) {
        if (StringUtils.isEmpty(key)) {
            return;
        }
        try {
            db.delete(currentCfh, key.getBytes());
            memoryCache.invalidate(key);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public T get(String key) {
        try {
            return memoryCache.getUnchecked(key);
        } catch (Exception e) {
            // TODO - Guava Cache不允许存储null值，如果db查询结果为null时就会抛异常
        }
        return null;
    }

    public List<T> getAll() {
        RocksIterator iterator = null;
        try {
            List<T> tList = new ArrayList<T>();
            if (entityClass == String.class) {
                iterator = db.newIterator(currentCfh);
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    tList.add((T) new String(iterator.value()));
                    iterator.next();
                }
                iterator.close();
                return tList;
            } else if (entityClass == Object.class) {
                iterator = db.newIterator(currentCfh);
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    tList.add((T) iterator.value());
                    iterator.next();
                }
                iterator.close();
                return tList;
            } else {
                iterator = db.newIterator(currentCfh);
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    tList.add(JsonUtil.parseObject(new String(iterator.value()), entityClass));
                    iterator.next();
                }
                iterator.close();
                return tList;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        return new ArrayList<T>();
    }

    public List<T> getAll(int from, int to) {
        if (to < from) return new ArrayList<T>();
        RocksIterator iterator = null;
        try {
            List<T> tList = new ArrayList<T>();
            if (entityClass == String.class) {
                iterator = db.newIterator(currentCfh);
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    if (from-- <= 0 && tList.size() <= (to - from)) {
                        tList.add((T) new String(iterator.value()));
                    }
                    iterator.next();
                }
                iterator.close();
                return tList;
            } else if (entityClass == Object.class) {
                iterator = db.newIterator(currentCfh);
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    if (from-- <= 0 && tList.size() <= (to - from)) {
                        tList.add((T) iterator.value());
                    }
                    iterator.next();
                }
                iterator.close();
                return tList;
            } else {
                iterator = db.newIterator(currentCfh);
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    if (from-- <= 0 && tList.size() <= (to - from)) {
                        byte[] v = iterator.value();
                        tList.add(JsonUtil.parseObject(new String(v), entityClass));
                    }
                    iterator.next();
                }
                iterator.close();
                return tList;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }
        return new ArrayList<T>();
    }

    public List<String> keys() {
        List<String> keys = new ArrayList<>();
        RocksIterator iterator = null;
        try {
            iterator = db.newIterator(currentCfh);
            iterator.seekToFirst();
            while (iterator.isValid()) {
                keys.add(new String(iterator.key()));
                iterator.next();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }

        return keys;
    }

    public List<String> keys(int from, int to) {
        List<String> keys = new ArrayList<>();
        RocksIterator iterator = null;
        try {
            iterator = db.newIterator(currentCfh);
            iterator.seekToFirst();
            while (iterator.isValid()) {
                if (from-- <= 0 && keys.size() <= (to - from)) {
                    keys.add(new String(iterator.key()));
                }
                iterator.next();
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (iterator != null) {
                iterator.close();
            }
        }

        return keys;
    }

    private T getFromDb(String key) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        try {
            byte[] data = db.get(currentCfh, key.getBytes());
            if (data == null) {
                return null;
            }
            if (entityClass == String.class) {
                return (T) new String(data);
            }
            if (entityClass == Object.class) {
                return (T) data;
            }
            return JsonUtil.parseObject(new String(data), entityClass);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return null;
    }
}
