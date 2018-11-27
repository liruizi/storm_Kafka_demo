package com.lrz.storm.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.type.TypeReference;
import org.apache.log4j.Logger;
public class JedisUtil {

	private static Logger log = Logger.getLogger(JedisUtil.class);

	/**
	 * 存放String类型的值
	 *
	 * @param key
	 *            key值
	 * @param value
	 *            value值
	 * @throws Exception
	 */
	public void setString(String key, String value) throws Exception {
		try {
			RedisConfigUtil.getJedisCluster().set(key, value);
		} catch (Exception e) {
			log.error(e);
			throw new Exception("存储String对象的值出错");
		}
	}

	/**
	 * 获取String类型的值
	 *
	 * @param key
	 *            key值
	 * @return 取得对应值
	 * @throws Exception
	 */
	public String getString(String key) throws Exception {
		String value = null;
		try {
			value = RedisConfigUtil.getJedisCluster().get(key);
		} catch (Exception e) {
			log.error(e);
			throw new Exception("获取String对象的值出错");
		}
		return value;
	}

	/**
	 * 设置对象
	 *
	 * @param key
	 *            key值
	 * @param object
	 *            对象
	 * @throws Exception
	 */
	public void setObject(String key, Object object) throws Exception {
		try {
			String json = JsonConvertUtil.objectConvertJSON(object);
			RedisConfigUtil.getJedisCluster().set(key, json);
		} catch (Exception e) {
			log.error(e);
			throw new Exception("存储Object对象的值出错");
		}
	}

	/**
	 * 获取对象
	 *
	 * @param key
	 *            key值
	 * @param type
	 *            获取对象的类型
	 * @return 对象类型
	 * @throws Exception
	 */
	public Object getObject(String key, TypeReference<?> type) throws Exception {
		Object obj = null;
		try {
			if (RedisConfigUtil.getJedisCluster().exists(key)) {
				String json = RedisConfigUtil.getJedisCluster().get(key);
				obj = JsonConvertUtil.jsonConvertObject(json, type);
			}
		} catch (Exception e) {
			log.error(e);
			throw new Exception("获取Object对象的值出错");
		}
		return obj;
	}

	/**
	 * 根据key、field取hash值，转换为对象
	 *
	 * @param key
	 * @param field
	 * @return
	 * @throws Exception
	 */
	public Object hGet(String key, String field, TypeReference<?> type) throws Exception {
		try {
			if (RedisConfigUtil.getJedisCluster().exists(key)) {
				String json = RedisConfigUtil.getJedisCluster().hget(key, field);
				return JsonConvertUtil.jsonConvertObject(json, type);
			}
			return null;
		} catch (Exception e) {
			log.error(e);
			throw new Exception("获取Hash时出错");
		}
	}

	/**
	 * 取key对应的所有hash，返回hashmap
	 *
	 * @param key
	 * @return
	 * @throws Exception
	 */
	public Map hGetAll(String key) throws Exception {
		try {
			if (RedisConfigUtil.getJedisCluster().exists(key)) {
				return RedisConfigUtil.getJedisCluster().hgetAll(key);
			}
			return null;
		} catch (Exception e) {
			log.error(e);
			throw new Exception("获取HashAll时出错");
		}
	}

	/**
	 * 存放到缓存中，并设置失效时间
	 *
	 * @param key
	 *            key值
	 * @param object
	 *            实体
	 * @param min
	 *            失效时间
	 * @throws Exception
	 */
	public void setObjectByExpire(String key, Object object, int min) throws Exception {
		int seconds = min * 60;
		try {
			String json = JsonConvertUtil.objectConvertJSON(object);
			RedisConfigUtil.getJedisCluster().set(key, json);
			RedisConfigUtil.getJedisCluster().expire(key, seconds);
		} catch (Exception e) {
			log.error(e);
			throw new Exception("存储Object对象的值并设置失效时间操作出错");
		}
	}

	/**
	 * 获取缓存信息
	 *
	 * @param key
	 *            key值
	 * @param type
	 *            实体类型
	 * @return 获取的对象
	 * @throws Exception
	 */
	public Object getExistObject(String key, TypeReference<?> type) throws Exception {
		try {
			if (RedisConfigUtil.getJedisCluster().exists(key)) {
				String json = RedisConfigUtil.getJedisCluster().get(key);
				return JsonConvertUtil.jsonConvertObject(json, type);
			}
			return null;
		} catch (Exception e) {
			log.error(e);
			throw new Exception("获取Object对象的值并设置失效时间操作出错");
		}
	}

	/**
	 * 判断缓存中是否存在此key
	 *
	 * @param key
	 *            key值
	 * @return true存在，false不存在
	 * @throws Exception
	 */
	public boolean isExists(String key) throws Exception {
		boolean flag = false;
		try {
			flag = RedisConfigUtil.getJedisCluster().exists(key);
		} catch (Exception e) {
			log.error(e);
			throw new Exception("判断缓存中是否存在此key时出错");
		}
		return flag;
	}

	/**
	 * 根据key值删除缓存内容
	 *
	 * @param key
	 *            key值
	 * @throws Exception
	 */
	public void deleteByKey(String key) throws Exception {
		try {
			if (RedisConfigUtil.getJedisCluster().exists(key)) {
				RedisConfigUtil.getJedisCluster().del(key);
			}
		} catch (Exception e) {
			log.error(e);
			throw new Exception("删除此key时出错");
		}
	}

	/**
	 * 将相同key值放入到链表中
	 *
	 * @param key
	 *            key值
	 * @param object
	 *            对象
	 * @throws Exception
	 */
	public void setList(String key, Object object) throws Exception {
		try {
			String json = JsonConvertUtil.objectConvertJSON(object);
			RedisConfigUtil.getJedisCluster().rpush(key, json);
		} catch (Exception e) {
			log.error(e);
			throw new Exception("存储列表对象的值出错");
		}
	}

	/**
	 * 获取列表对象
	 *
	 * @param key
	 *            key值
	 * @param type
	 *            获取对象的类型
	 * @return 对象类型
	 * @throws Exception
	 */
	public List<Object> getList(String key, TypeReference<?> type) throws Exception {
		try {
			List<Object> list = new ArrayList<Object>();
			Long size = RedisConfigUtil.getJedisCluster().llen(key);
			List<String> bis = RedisConfigUtil.getJedisCluster().lrange(key, 0, size.intValue() - 1);
			for (String bi : bis) {
				Object obj = JsonConvertUtil.jsonConvertObject(bi, type);
				list.add(obj);
			}
			return list;
		} catch (Exception e) {
			log.error(e);
			throw new Exception("获取列表对象出错");
		}
	}

	/**
	 * 保存对象到set集合中，去掉重复数据
	 *
	 * @param key
	 *            key值
	 * @param object
	 *            对象
	 * @throws Exception
	 */
	public void setSadd(String key, Object object) throws Exception {
		try {
			String json = JsonConvertUtil.objectConvertJSON(object);
			RedisConfigUtil.getJedisCluster().sadd(key, json);
		} catch (Exception e) {
			log.error(e);
			throw new Exception("保存对象到set集合中出错");
		}
	}

	/**
	 * 获取所有set集合中的对象
	 *
	 * @param key
	 *            key值
	 * @param type
	 *            获取对象的类型
	 * @return 对象类型
	 * @throws Exception
	 */
	public List<Object> getSmembers(String key, TypeReference<?> type) throws Exception {
		try {
			List<Object> list = new ArrayList<Object>();
			java.util.Set<String> bis = RedisConfigUtil.getJedisCluster().smembers(key);
			for (String bi : bis) {
				Object obj = JsonConvertUtil.jsonConvertObject(bi, type);
				list.add(obj);
			}
			return list;
		} catch (Exception e) {
			log.error(e);
			throw new Exception("获取所有set集合中的对象出错");
		}
	}

}
