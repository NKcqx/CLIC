package fdu.daslab.redis;

import fdu.daslab.util.Configuration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.FileNotFoundException;

/**
 * redis客户端相关的使用方法
 *
 * @author 唐志伟
 * @version 1.0
 * @since 2020/11/13 2:11 PM
 */
public class JedisUtil {

    private static JedisPool jedisPool = null;

    static {
        initJedis();
    }

    private static void initJedis() {
        try {
            Configuration configuration = new Configuration();
            String redisUrl = configuration.getProperty("redis-url");
            int redisPort = Integer.parseInt(configuration.getProperty("redis-port"));
            String redisPassword = configuration.getProperty("redis-password");
            final JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPool = new JedisPool(jedisPoolConfig, redisUrl, redisPort, 2000, redisPassword);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        // 关闭连接池
        if (jedisPool != null) {
            jedisPool.close();
        }
    }

    /**
     * 获取一个操作redis的客户端
     *
     * @return Jedis客户端
     */
    public static Jedis getClient() {
        if (jedisPool == null) {
            initJedis();
        }
        return jedisPool.getResource();
    }

}
