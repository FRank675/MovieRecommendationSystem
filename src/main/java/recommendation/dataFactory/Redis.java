package recommendation.dataFactory;

import redis.clients.jedis.Jedis;

public class Redis {
    //singleton Jedis
    private static volatile Jedis redisClient;
    final static String REDIS_END_POINT = "localhost";
    final static int REDIS_PORT = 6379;

    private Redis(){
        redisClient = new Jedis(REDIS_END_POINT, REDIS_PORT);
    }

    public static Jedis getInstance(){
        if (null == redisClient){
            synchronized (Redis.class){
                if (null == redisClient){
                    redisClient = new Jedis(REDIS_END_POINT, REDIS_PORT);
                }
            }
        }
        return redisClient;
    }
}
