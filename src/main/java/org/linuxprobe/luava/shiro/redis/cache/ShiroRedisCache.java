package org.linuxprobe.luava.shiro.redis.cache;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.linuxprobe.luava.cache.impl.RedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.serializer.JdkSerializationRedisSerializer;

public class ShiroRedisCache implements Cache<Serializable, Serializable> {
	private Logger logger = LoggerFactory.getLogger(this.getClass());
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
	 * @param redisCache redis缓存
	 * @param name       缓存名称
	 */
	public ShiroRedisCache(RedisCache redisCache, String name) {
		if (redisCache == null) {
			throw new NullPointerException("redisCache can't be null");
		}
		this.redisCache = redisCache;
		this.keyPrefix = this.keyPrefix + name + ":";
		if (logger.isTraceEnabled()) {
			logger.trace("create shiro redis cache, keyPrefix is '{}'", this.keyPrefix);
		}
	}

	/**
	 * 根据Key获取缓存中的值
	 */
	@Override
	public Serializable get(Serializable key) throws CacheException {
		key = this.keyAddPrefix(key);
		try {
			if (key == null) {
				return null;
			} else {
				return this.redisCache.get(key);
			}
		} catch (Throwable t) {
			throw new CacheException(t);
		}
	}

	/**
	 * 往缓存中放入key-value，返回缓存中之前的值
	 */
	@Override
	public Serializable put(Serializable key, Serializable value) throws CacheException {
		key = this.keyAddPrefix(key);
		try {
			this.redisCache.set(key, value);
			return value;
		} catch (Throwable t) {
			throw new CacheException(t);
		}
	}

	/**
	 * 移除缓存中key对应的值，返回该值
	 */
	@Override
	public Serializable remove(Serializable key) throws CacheException {
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
		try {
			Set<Serializable> keys = this.redisCache.keys(this.keyPrefix + "*");
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
		try {
			return this.redisCache.keys(this.keyPrefix + "*").size();
		} catch (Throwable t) {
			throw new CacheException(t);
		}
	}

	/**
	 * 获取缓存中所有的key
	 */
	@Override
	public Set<Serializable> keys() {
		try {
			Set<Serializable> allKeys = this.redisCache.keys(this.keyPrefix + "*");
			for (Serializable key : allKeys) {
				key.toString().substring(this.keyPrefix.length());
			}
			return allKeys;
		} catch (Throwable t) {
			throw new CacheException(t);
		}
	}

	/**
	 * 获取缓存中所有的value
	 */
	@Override
	public Collection<Serializable> values() {
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