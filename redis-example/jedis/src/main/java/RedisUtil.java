import java.util.Map;

import redis.clients.jedis.Jedis;

/**
 * Redis 测试类
 *
 */

public class RedisUtil {

    private static Jedis jedis;

    static {
        jedis = new Jedis("127.0.0.1:6379");

        //权限认证
        jedis.auth("123456");
    }

    /**
     * 设置字符串
     * @param key  全局key
     * @param value  字符串值
     */
    public static void setStringValue(String key, String value) {
        jedis.set(key, value);
    }

    /**
     * 获取字符串
     * @param key 全局key
     * @return String 获取的字符串值
     */
    public static String getStringValue(String key) {
        return jedis.get(key);
    }


    /**
     * 添加Hash数据
     * @param hashName  全局key
     * @param hashContent 一次性设置整个哈希内容
     */
    public static void setHashAll(String hashName, Map<String, String> hashContent) {
        jedis.hmset(hashName, hashContent);
    }

    /**
     * 根据键值获取整个hashmap
     * @param hashName  全局key
     * @return 整个哈希结果
     */
    public static Map<String, String> getHashAll(String hashName) {
        return jedis.hgetAll(hashName);
    }


    /**
     * 设置单个哈希key值
     * @param hashName  全局key
     * @param hashKey  哈希的key
     * @param hashValue  对应的value
     */
    public static void setHashValue(String hashName, String hashKey, String hashValue) {
        jedis.hset(hashName, hashKey, hashValue);
    }

    
    /**
     * 获取hashmap中的指定key
     * @param hashName  全局key
     * @param key  哈希的key
     * @return 指定的哈希值
     */
    public static String getHashByKey(String hashName, String key) {
        return jedis.hget(hashName, key);
    }

    /**
     * 关闭连接
     */
    public static void close() {
        jedis.close();
    }
}
