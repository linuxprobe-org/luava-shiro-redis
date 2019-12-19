package org.linuxprobe.luava.shiro.redis.cache;

import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.CacheManager;
import org.linuxprobe.luava.cache.impl.redis.RedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ShiroRedisCacheManager implements CacheManager {

    private static final Logger logger = LoggerFactory.getLogger(ShiroRedisCacheManager.class);

    private final ConcurrentMap<String, Cache<Serializable, Serializable>> caches = new ConcurrentHashMap<>();
    /**
     * 缓存超时时间, 单位秒
     */
    private long cacheTimeout;
    /**
     * redis操作类
     */
    private RedisCache redisCache;

    public ShiroRedisCacheManager(RedisCache redisCache) {
        this(redisCache, 0);
    }

    public ShiroRedisCacheManager(RedisCache redisCache, long cacheTimeout) {
        this.redisCache = redisCache;
        this.cacheTimeout = cacheTimeout;
    }

    /**
     * 根据缓存名字获取一个Cache
     */
    @SuppressWarnings("unchecked")
    @Override
    public Cache<Serializable, Serializable> getCache(String name) throws CacheException {
        ShiroRedisCacheManager.logger.debug("获取名称为: " + name + " 的RedisCache实例");
        Cache<Serializable, Serializable> c = this.caches.get(name);
        if (c == null) {
            c = new ShiroRedisCache(this.redisCache, name, this.cacheTimeout);
            this.caches.put(name, c);
        }
        return c;
    }
}