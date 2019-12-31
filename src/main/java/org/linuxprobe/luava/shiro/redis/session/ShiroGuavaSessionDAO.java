package org.linuxprobe.luava.shiro.redis.session;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.eis.AbstractSessionDAO;

import java.io.Serializable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class ShiroGuavaSessionDAO extends AbstractSessionDAO {
    private Cache<Serializable, Session> cache;

    public ShiroGuavaSessionDAO(long timeout) {
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(100000000) // 设置缓存的最大容量
                .expireAfterWrite(timeout, TimeUnit.MILLISECONDS) // 设置缓存在写入一分钟后失效
                .concurrencyLevel(30000) // 设置并发级别为3000
                .recordStats() // 开启缓存统计
                .build();
    }

    public void saveSession(Session session) throws UnknownSessionException {
        if (session == null || session.getId() == null) {
            throw new UnknownSessionException("session or session id is null");
        }
        this.cache.put(session.getId(), session);
    }

    @Override
    protected Serializable doCreate(Session session) {
        Serializable sessionId = this.generateSessionId(session);
        this.assignSessionId(session, sessionId);
        this.saveSession(session);
        return sessionId;
    }

    @Override
    protected Session doReadSession(Serializable sessionId) {
        if (sessionId == null) {
            return null;
        }
        return this.cache.getIfPresent(sessionId);
    }

    @Override
    public void update(Session session) throws UnknownSessionException {
        this.saveSession(session);
    }

    @Override
    public void delete(Session session) {
        this.cache.invalidate(session.getId());
    }

    @Override
    public Collection<Session> getActiveSessions() {
        return this.cache.asMap().values();
    }
}
