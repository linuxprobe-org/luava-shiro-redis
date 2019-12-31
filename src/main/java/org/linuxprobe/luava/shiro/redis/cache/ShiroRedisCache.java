package org.linuxprobe.luava.shiro.redis.cache;

import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.linuxprobe.luava.cache.impl.redis.RedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ShiroRedisCache implements Cache<Serializable, Serializable> {
    private static final Logger logger = LoggerFactory.getLogger(ShiroRedisCache.class);
    /**
     * key前缀
     */
    private String keyPrefix = "shiro:cache:";
    /**
     * redis操作类
     */
    private RedisCache redisCache;
    private JdkSerializationRedisSerializer serializer = new JdkSerializationRedisSerializer();
    /**
     * 缓存超时时间, 单位毫秒
     */
    private long timeout;

    private ShiroGuavaCache<Serializable, Serializable> guavaCache;

    /**
     * @param redisCache redis缓存
     * @param name       缓存名称
     * @param timeout    缓存超时时间, 单位毫秒
     */
    public ShiroRedisCache(RedisCache redisCache, String name, long timeout) {
        if (redisCache == null) {
            throw new NullPointerException("redisCache can't be null");
        }
        this.redisCache = redisCache;
        this.keyPrefix = this.keyPrefix + name + ":";
        if (ShiroRedisCache.logger.isTraceEnabled()) {
            ShiroRedisCache.logger.trace("create shiro redis cache, keyPrefix is '{}'", this.keyPrefix);
        }
        this.timeout = timeout;
        this.guavaCache = new ShiroGuavaCache<>(timeout);
    }

    /**
     * 根据Key获取缓存中的值
     */
    @Override
    public Serializable get(Serializable key) throws CacheException {
        if (key == null) {
            return null;
        }
        Serializable value = this.guavaCache.get(key);
        if (value == null) {
            Serializable oldKey = key;
            key = this.keyAddPrefix(key);
            try {
                value = this.redisCache.get(key);
                if (value != null) {
                    this.guavaCache.put(oldKey, value);
                }
                return value;
            } catch (Throwable t) {
                throw new CacheException(t);
            }
        }
        return value;
    }

    /**
     * 往缓存中放入key-value，返回缓存中之前的值
     */
    @Override
    public Serializable put(Serializable key, Serializable value) throws CacheException {
        this.guavaCache.put(key, value);
        // 每8秒同步一次到redis
        if (System.currentTimeMillis() % 8000 == 0) {
            key = this.keyAddPrefix(key);
            this.redisCache.set(key, value, this.timeout, TimeUnit.MILLISECONDS);
        }
        return value;
    }

    /**
     * 移除缓存中key对应的值，返回该值
     */
    @Override
    public Serializable remove(Serializable key) throws CacheException {
        this.guavaCache.remove(key);
        key = this.keyAddPrefix(key);
        try {
            Serializable value = this.redisCache.get(key);
            this.redisCache.delete(key);
            return value;
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    /**
     * 清空整个缓存
     */
    @Override
    public void clear() throws CacheException {
        this.guavaCache.clear();
        try {
            Set<String> keys = this.redisCache.scan(this.keyPrefix + "*");
            this.redisCache.delete(keys);
        } catch (Throwable t) {
            throw new CacheException(t);
        }
    }

    /**
     * 返回缓存大小
     */
    @Override
    public int size() {
        int size = this.guavaCache.size();
        if (size == 0) {
            try {
                return this.redisCache.scan(this.keyPrefix + "*").size();
            } catch (Throwable t) {
                throw new CacheException(t);
            }
        }
        return 0;
    }

    /**
     * 获取缓存中所有的key
     */
    @Override
    public Set<Serializable> keys() {
        Set<Serializable> keys = this.guavaCache.keys();
        if (keys == null || keys.isEmpty()) {
            try {
                Set<String> allKeys = this.redisCache.scan(this.keyPrefix + "*");
                for (Serializable key : allKeys) {
                    key.toString().substring(this.keyPrefix.length());
                }
                return (Set<Serializable>) (Set<?>) allKeys;
            } catch (Throwable t) {
                throw new CacheException(t);
            }
        }
        return keys;
    }

    /**
     * 获取缓存中所有的value
     */
    @Override
    public Collection<Serializable> values() {
        Collection<Serializable> values = this.guavaCache.values();
        if (values == null || values.isEmpty()) {
            try {
                Collection<Serializable> allvalues = new HashSet<>();
                Set<Serializable> keys = this.keys();
                for (Serializable key : keys) {
                    allvalues.add(this.get(key));
                }
                return allvalues;
            } catch (Throwable t) {
                throw new CacheException(t);
            }
        }
        return values;
    }

    private String keyAddPrefix(Serializable key) {
        String newKey = null;
        try {
            if (key instanceof String || key instanceof Number || key instanceof Character || key instanceof Boolean) {
                newKey = this.keyPrefix + key;
            } else {
                newKey = this.keyPrefix + new String(this.serializer.serialize(key), "UTF-8");
            }
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
        return newKey;
    }
}