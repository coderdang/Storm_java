package Util_Redis;

import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Utils_Time.LogInfo;
import Utils_Time.PrintException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisException;

/**
 * 
 * @author dangwei
 *
 */
public class RedisDao {

	private static Logger logger = LoggerFactory.getLogger(RedisDao.class);
	private JedisSentinelPool jedisSentinelPool;

	private static RedisDao instance;

	public static synchronized RedisDao getInstance() {
		if (instance == null) {
			instance = new RedisDao();
		}
		return instance;
	}

	public JedisSentinelPool getJedisSentinelPool() {
		return jedisSentinelPool;
	}

	public void setJedisSentinelPool(JedisSentinelPool jedisSentinelPool) {
		this.jedisSentinelPool = jedisSentinelPool;
	}

	private RedisDao() {
		super();
		Properties p = new Properties();
		try {
			p.load(this.getClass().getResourceAsStream("/Config/redis.properties"));
			JedisPoolConfig config = new JedisPoolConfig();
			Integer minIdle=Integer.valueOf(p.getProperty("redis.minIdle"));
			config.setMinIdle(minIdle);			
			Integer maxWaitMillis=Integer.valueOf(p.getProperty("redis.maxWaitMillis"));
			config.setMaxWaitMillis(maxWaitMillis);
			Integer maxIdle=Integer.valueOf(p.getProperty("redis.maxIdle"));
			config.setMaxWaitMillis(maxIdle);
			Boolean testOnBorrow=Boolean.valueOf(p.getProperty("redis.testOnBorrow"));
			config.setTestOnBorrow(testOnBorrow);
			Boolean testOnReturn=Boolean.valueOf(p.getProperty("redis.testOnReturn"));
			config.setTestOnBorrow(testOnReturn);
			Boolean testWhileIdle=Boolean.valueOf(p.getProperty("redis.testWhileIdle"));
			config.setTestOnBorrow(testWhileIdle);			
			
			Set<String> set = new HashSet<String>();
			set.add(p.getProperty("redis.ip"));
			String masterName=p.getProperty("redis.masterName");
			Integer timeout=Integer.valueOf(p.getProperty("redis.timeout"));
			String pwd=p.getProperty("redis.password");
			Integer database=Integer.valueOf(p.getProperty("redis.database"));
			this.jedisSentinelPool = new JedisSentinelPool(masterName, set,config,timeout,pwd ,database);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogInfo.appendLog("Redis.err", PrintException.getStackTrace(e));			
		}
	}

	/**
	 * 判断key是否存在
	 * 
	 * @param key
	 * @return true or false
	 */
	public Boolean existsKey(String key) {

		Jedis jedis = null;
		try {
			jedis = jedisSentinelPool.getResource();
			return jedis.exists(key);
		} catch (Exception ex) {
			// logger.error("get error.", ex);
			returnBrokenResource(jedis);
			throw new JedisException("Could not get a resource from the pool", ex);
		} finally {
			returnResource(jedis);
		}
	}

	public boolean set(String key, String value) {
		Jedis jedis = null;
		try {
			jedis = jedisSentinelPool.getResource();
			jedis.set(key, value);
			return true;
		} catch (Exception ex) {
			// logger.error("set error.", ex);
			returnBrokenResource(jedis);
			throw new JedisException("Could not get a resource from the pool", ex);
		} finally {
			returnResource(jedis);
		}
	}

	public String get(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisSentinelPool.getResource();
			return jedis.get(key);
		} catch (Exception ex) {
			// logger.error("get error.", ex);
			returnBrokenResource(jedis);
			throw new JedisException("Could not get a resource from the pool", ex);
		} finally {
			returnResource(jedis);
		}
	}

	public boolean delkey(String key) {
		Jedis jedis = null;
		try {
			jedis = jedisSentinelPool.getResource();
			jedis.del(key);
			return true;
		} catch (Exception ex) {
			// logger.error("del error.", ex);
			returnBrokenResource(jedis);
			throw new JedisException("Could not get a resource from the pool", ex);
		} finally {
			returnResource(jedis);
		}
	}

	private void returnBrokenResource(Jedis jedis) {
		try {
			jedis.close();
			// jedisSentinelPool.returnBrokenResource(jedis);
		} catch (Exception e) {
			logger.error("returnBrokenResource error.", e);
		}
	}

	private void returnResource(Jedis jedis) {
		try {
			jedis.close();
			// jedisSentinelPool.returnResource(jedis);
		} catch (Exception e) {
			logger.error("returnResource error.", e);
		}
	}

}
