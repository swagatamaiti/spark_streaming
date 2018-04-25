package com.mutliOrder.ETL.helper;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

public class RedisHelper {
	
	// Redis服务器IP
		private static String ADDR = "192.168.1.210";

		// Redis的端口号
		private static int PORT = 6379;

		// 可用连接实例的最大数目，默认值为8；
		// 如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
		private static int MAX_ACTIVE = 2;

		// 控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
		private static int MAX_IDLE = 2;

		// 等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException；
		private static int MAX_WAIT = 100000;

		private static int TIMEOUT = 100000;

		// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
		private static boolean TEST_ON_BORROW = true;

		private JedisPool jedisPool = null;
		
		private static Map<Integer,RedisHelper> instance = new HashMap<Integer, RedisHelper>();
		public static RedisHelper getInstance(int database) throws Exception {
			if(instance.get(database) == null) {
				synchronized(RedisHelper.class) {
					if(instance.get(database) == null) {
						instance.put(database, new RedisHelper(database));
					}
				}
			}
			return instance.get(database);
		} 
		
		private RedisHelper(int database){
			try {
				JedisPoolConfig config = new JedisPoolConfig();
				config.setMaxTotal(MAX_ACTIVE);
				config.setMaxIdle(MAX_IDLE);
				config.setMaxWaitMillis(MAX_WAIT);
				config.setTestOnBorrow(TEST_ON_BORROW);
//				jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT);
				this.jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT,null,database);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		/**
		 * 获取Jedis实例
		 * 
		 * @return
		 */
		private synchronized Jedis getJedis() {
			try {
				if (jedisPool != null) {
					Jedis resource = jedisPool.getResource();
					return resource;
				} else {
					return null;
				}
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}

		/**
		 * 释放jedis资源
		 * 
		 * @param jedis
		 */
		@SuppressWarnings("deprecation")
		public void returnResource(final Jedis jedis) {
			if (jedis != null) {
				jedisPool.returnResource(jedis);
			}
		}
	    
	
	    public String getValueByKey(String key){
	    	Jedis jedis = getJedis();
	    	String value = jedis.get(key);
	    	returnResource(jedis);
	    	jedis=null;
	    	return value;
	    }
		public void insertKV(String key,String value){
			Jedis jedis = getJedis();
	    	 jedis.set(key, value);
	    	returnResource(jedis);
	    	jedis=null;
		}
		
		public Integer getIntByString(String key) {
			Jedis jedis = getJedis();
			byte[] value = jedis.get(key.getBytes());
	    	returnResource(jedis);
	    	if(value==null) {
	    		return null;
	    	}else {
	    		return Bytes.toInt(value);
	    	}
	    	
		}
		
		public byte[] hget(byte[] key,byte[] field) {
			Jedis jedis = getJedis();
			byte[] value = jedis.hget(key, field);
	    	returnResource(jedis);
			return value;
		}
		
		  public Set<String> keys(String pattern){
		    	Jedis jedis = getJedis();
		    	Set<String> list = jedis.keys(pattern);
		    	returnResource(jedis);
		    	jedis=null;
				return list;
		    }
		  
		  public String type(String key){
		    	Jedis jedis = getJedis();
		    	String type = jedis.type(key);
		    	returnResource(jedis);
				return type;
		    }
		
	    public List<String> getListAll(String key){
	    	Jedis jedis = getJedis();
	    	List<String> list = jedis.lrange(key,0,-1);
	    	returnResource(jedis);
	    	jedis=null;
			return list;
	    }
	    
	    public void removeListByKV(String key,String value){
	    	Jedis jedis = getJedis();
	    	jedis.lrem(key, 1, value);
	    	returnResource(jedis);
	    	jedis=null;
	    }
	    
	    public void insertList(String key,String value){
	    	Jedis jedis = getJedis();
	    	jedis.rpush(key, value);
	    	returnResource(jedis);
	    	jedis=null;
	    }
	    
	    public void insertKV(byte[] key,byte[] value){
	    	Jedis jedis = getJedis();
	    	jedis.set(key, value);
	    	returnResource(jedis);
	    }
	    
	    public Map<String,String> getMapAll(String key){
	    	Jedis jedis = getJedis();
	    	Map<String,String> map = jedis.hgetAll(key);
	    	returnResource(jedis);
	    	jedis=null;
			return map;	
	    }
	    public void putMapAll(String key,Map<String,String>map){
	    	Jedis jedis = getJedis();
	    	jedis.hmset(key, map);
			returnResource(jedis);
	    	jedis=null;
	    }

	    
	    public void saddPipeline(Map<String,Set<String>> map) {
	    	Jedis jedis = getJedis();
	    	Pipeline pipeline = jedis.pipelined();
	    	Iterator<Entry<String, Set<String>>> it = map.entrySet().iterator();
	    	while(it.hasNext()) {
	    		Entry<String, Set<String>> entry = it.next();
	    		pipeline.sadd(entry.getKey(), entry.getValue().toArray(new String[0]));
	    	}
	    	pipeline.sync();
	    	returnResource(jedis);
	    }
	    
	    public void sadd(String key,String... menber) {
	    	Jedis jedis = getJedis();
	    	jedis.sadd(key, menber);
	    	returnResource(jedis);
	    }
	    
	    public void rpush(String key,String... menber) {
	    	Jedis jedis = getJedis();
	    	jedis.rpush(key, menber);
	    	returnResource(jedis);
	    }
	    
	    public void ltrim(String key,long start,long end) {
	    	Jedis jedis = getJedis();
	    	jedis.ltrim(key, start, end);
	    	returnResource(jedis);
	    }
	    
	    public void sadd(byte[] key,byte[]... menber) {
	    	Jedis jedis = getJedis();
	    	jedis.sadd(key, menber);
	    	returnResource(jedis);
	    }
	    
	    public boolean sismember(byte[] key,byte[] menber) {
	    	boolean flag = false;
	    	Jedis jedis = getJedis();
	    	flag = jedis.sismember(key, menber);
	    	returnResource(jedis);
	    	return flag;
	    }
	    
	    public void srem(String key,String menber) {
	    	Jedis jedis = getJedis();
	    	jedis.srem(key, menber);
	    	returnResource(jedis);
	    }
	    
	    public List<String> lrange(String key,long start,long end) {
	    	Jedis jedis = getJedis();
	    	List<String> list = jedis.lrange(key, start, end);
	    	returnResource(jedis);
			return list;
	    }
	    
	    public Long llen(String key) {
	    	Jedis jedis = getJedis();
	    	Long len = jedis.llen(key);
	    	returnResource(jedis);
			return len;
	    }
	    
	    public void lrem(String key,String value,int count) {
	    	Jedis jedis = getJedis();
	    	jedis.lrem(key, 1, value);
	    	returnResource(jedis);
	    }
	    
	    public Set<String> smembers(String key) {
	    	Jedis jedis = getJedis();
	    	Set<String> set = jedis.smembers(key);
	    	returnResource(jedis);
			return set;
	    }
	    
	    public Set<String> pipelineSmembers(String key) {
	    	Jedis jedis = getJedis();
	    	Pipeline pipeline = jedis.pipelined();
	    	Set<String> set = pipeline.smembers(key).get();
	    	returnResource(jedis);
			return set;
	    }
	    
		public void hset(byte[] key, byte[] field, byte[] value) {
			// TODO Auto-generated method stub
			Jedis jedis = getJedis();
			jedis.hset(key, field, value);
			returnResource(jedis);
		}
		
		public void expire(String key,int second) {
			Jedis jedis = getJedis();
			jedis.expire(key, second);
			returnResource(jedis);
		}

		public Map<byte[], byte[]> hgetAll(byte[] key) {
			Jedis jedis = getJedis();
			Map<byte[], byte[]> map = jedis.hgetAll(key);
			returnResource(jedis);
			return map;
		}
		public static void main(String[] args) throws Exception {
//			RedisHelper redis= RedisHelper.getInstance(2);
//			String[] keys = {"App_WordRelatio2"};
//			Jedis jedis = redis.getJedis();
//			for(String key:keys) {
//				while(true) {
//					String uuid = jedis.spop(key);
//					if(uuid!=null) {
//						jedis.sadd("App_WordRelation"+uuid.substring(0, 2), uuid);
//					}else {
//						break;
//					}
//				}
//				
//			}
			RedisHelper redis= RedisHelper.getInstance(2);
			String redisKey = "App_KeywordRanking";
			Jedis  jedis = redis.getJedis();
			String[] strss = {"0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"};
			String[] strs = {"0","1","2","3","4","5","6","7","8","9","a","b","c","d","e","f"};
			for(String a:strss) {
				for(String b:strs) {
					jedis.del(redisKey+a+b);
				}
			}
//			while(value!=null) {
//				value = jedis.spop(redisKey);
//				redis.sadd("App_Comment"+value.substring(0,2), value);
//			}
			//			Set<String> set = new HashSet<String>();
//			for(Integer i =0;i<200000;i++) {
//				set.add(GenUuid.GetUuid());
//			}
//			
//			redis.sadd("123", set.toArray(new String[0]));
		
		}
}
