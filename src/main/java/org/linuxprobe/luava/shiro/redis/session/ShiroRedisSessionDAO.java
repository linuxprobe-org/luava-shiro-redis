package org.linuxprobe.luava.shiro.redis.session;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.shiro.session.Session;
import org.apache.shiro.session.UnknownSessionException;
import org.apache.shiro.session.mgt.eis.AbstractSessionDAO;
import org.linuxprobe.luava.cache.impl.RedisCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShiroRedisSessionDAO extends AbstractSessionDAO {
	private static Logger logger = LoggerFactory.getLogger(ShiroRedisSessionDAO.class);

	private static final String sessionKeyPrefix = "shiro:session:";

	private RedisCache redisCache;

	public ShiroRedisSessionDAO(RedisCache redisCache) {
		this.redisCache = redisCache;
	}

	@Override
	public void update(Session session) throws UnknownSessionException {
		this.saveSession(session);
	}

	/**
	 * save session
	 * 
	 * @param session
	 * @throws UnknownSessionException
	 */
	private void saveSession(Session session) throws UnknownSessionException {
		if (session == null || session.getId() == null) {
			throw new UnknownSessionException("session or session id is null");
		}
		long timeOut = session.getTimeout();
		if (timeOut < 0) {
			this.redisCache.set(this.getKey(session.getId()), (Serializable) session);
		} else {
			this.redisCache.set(this.getKey(session.getId()), (Serializable) session, timeOut / 1000);
		}
	}

	@Override
	public void delete(Session session) {
		if (session == null || session.getId() == null) {
			logger.error("session or session id is null");
			return;
		}
		redisCache.delete(this.getKey(session.getId()));
	}

	@Override
	public Collection<Session> getActiveSessions() {
		Set<Session> sessions = new HashSet<Session>();
		Set<Serializable> keys = this.redisCache.keys(sessionKeyPrefix + "*");
		if (keys != null && keys.size() > 0) {
			for (Serializable key : keys) {
				Session s = (Session) redisCache.get(key);
				sessions.add(s);
			}
		}
		return sessions;
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
		Session session = redisCache.get(getKey(sessionId));
		return session;
	}

	/**
	 * 把key加上前缀
	 * 
	 * @param key
	 * @return
	 */
	private String getKey(Serializable sessionId) {
		String preKey = sessionKeyPrefix + sessionId;
		return preKey;
	}
}