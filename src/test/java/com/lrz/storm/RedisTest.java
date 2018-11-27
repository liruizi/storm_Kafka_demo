package com.lrz.storm;

import com.lrz.storm.util.JedisUtil;

/**
 * 
 * @Description: Redis 测试连接
 * @author:DARUI LI
 * @version:1.0.0
 * @Data:2018年11月27日 下午8:07:02
 */
public class RedisTest {

	public static void main(String[] args) {
		JedisUtil redisUtil = new JedisUtil();
		try {
			redisUtil.setString("name", "test");
			String string = redisUtil.getString("name");
			System.out.println(string);
			
			redisUtil.deleteByKey("name");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
