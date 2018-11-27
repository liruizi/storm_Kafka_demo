package com.lrz.storm.util;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;

import java.util.LinkedHashSet;
import java.util.Set;

public class RedisConfigUtil {
	private static JedisCluster jedisCluster = null;

	private static Set<HostAndPort> nodes = null;

	private static JedisPoolConfig poolConfig = new JedisPoolConfig();

	static {
		nodes = new LinkedHashSet<HostAndPort>();
		nodes.add(new HostAndPort("192.168.164.133", 7000));
		nodes.add(new HostAndPort("192.168.164.133", 7001));
		nodes.add(new HostAndPort("192.168.164.134", 7002));
		nodes.add(new HostAndPort("192.168.164.134", 7003));
		nodes.add(new HostAndPort("192.168.164.135", 7004));
		nodes.add(new HostAndPort("192.168.164.135", 7005));
		// 最大连接数
		poolConfig.setMaxTotal(1);
		// 最大空闲数
		poolConfig.setMaxIdle(1);
		// 最大允许等待时间，如果超过这个时间还未获取到连接，则会报JedisException异常：
		poolConfig.setMaxWaitMillis(60000);
	}

	public static JedisCluster getJedisCluster() {
		if (jedisCluster == null) {
			jedisCluster = new JedisCluster(nodes, poolConfig);
		}
		return jedisCluster;
	}

}
