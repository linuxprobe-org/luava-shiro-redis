package org.linuxprobe.luava.shiro.redis.cache;

import com.google.common.cache.CacheBuilder;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ShiroGuavaCache<K, V> implements Cache<K, V> {
    private com.google.common.cache.Cache<K, V> cache;

    public ShiroGuavaCache(long timeout) {
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(100000000) // 设置缓存的最大容量
                .expireAfterWrite(timeout, TimeUnit.MILLISECONDS) // 设置缓存在写入一分钟后失效
                .concurrencyLevel(30000) // 设置并发级别为3000
                .recordStats() // 开启缓存统计
                .build();
    }

    /**
     * 缓存超时时间, 单位毫秒
     */
    private long timeout;

    @Override
    public V get(K k) throws CacheException {
        return this.cache.getIfPresent(k);
    }

    @Override
    public V put(K k, V v) throws CacheException {
        this.cache.put(k, v);
        return v;
    }

    @Override
    public V remove(K k) throws CacheException {
        V v = this.cache.getIfPresent(k);
        this.cache.invalidate(k);
        return v;
    }

    @Override
    public void clear() throws CacheException {
        this.cache.invalidateAll();
    }

    @Override
    public int size() {
        return (int) this.cache.size();
    }

    @Override
    public Set<K> keys() {
        return this.cache.asMap().keySet();
    }

    @Override
    public Collection<V> values() {
        return this.cache.asMap().values();
    }
}
