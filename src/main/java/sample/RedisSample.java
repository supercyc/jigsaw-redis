/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2014
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package sample;

import java.util.List;

import com.ericsson.jigsaw.redis.JedisScriptExecutor;
import com.ericsson.jigsaw.redis.JedisShardedTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisActionNoResult;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisException;

public class RedisSample {

    private static JedisPool jedisPool;

    public static void main(String[] args) {
        initPoolWhenStartUp();

        getAndSetMethod();

        getAndSetAction();

        destroyPoolWhileShutdown();
    }

    private static void initPoolWhenStartUp() {
        //Use URL to init pool
        //direct pool
        jedisPool = new JedisPoolBuilder().setUrl("direct://localhost:6379?poolSize=5").buildPool();
        //sentinel pool
        jedisPool = new JedisPoolBuilder().setUrl("sentinel://sc-1:26379,sc-2:26379?masterNames=default&poolSize=100&poolName=sentinelExample").buildPool();
        //sharding pool
        List<JedisPool> shardedPools = new JedisPoolBuilder().setUrl("sentinel://sc-1:26379,sc-2:26379?masterNames=shard1,shard2&" +
                "poolSize=100&database=0&password=&timeout=2000&poolName=sentinelExample2").buildShardedPools();

        //sentinel pool,minimal configure
        jedisPool = new JedisPoolBuilder().setHosts("rdsmon-1,rdsmon-2").setMasterName("default").setPoolSize(100).setPoolName("jedisPoolName")
                .buildPool();

        //direct pool, minimal configure, hosts would be skip.
        jedisPool = new JedisPoolBuilder().setHosts("rdsmon-1,rdsmon-2").setMasterName("direct:127.0.0.1:6379")
                .setPoolSize(100).buildPool();

        //sentinel pool, fully configure
        jedisPool = new JedisPoolBuilder().setHosts("rdsmon-1,rdsmon-2").setPort(26379).setMasterName("default")
                .setPoolSize(100).setDatabase(0).setPassword("").setTimeout(2000).buildPool();

        //direct pool, fully configure, hosts would be skip.
        jedisPool = new JedisPoolBuilder().setHosts("rdsmon-1,rdsmon-2").setMasterName("direct:127.0.0.1:6379")
                .setPoolSize(100).setDatabase(0).setPassword("").setTimeout(2000).buildPool();
    }

    private static void destroyPoolWhileShutdown() {
        jedisPool.destroy();
    }

    /**
     * Use the predefined methods to handle redis.
     * <p/>
     * JedisTemplate will get and return the connection from jedisPool correctly, like Spring JdbcTemplate.
     * <p/>
     * If the method you want didn't predefined, extend JedisTemplate yourself, or ask component owner for help, or see
     * the #getAndSetAction().
     */
    private static void getAndSetMethod() {
        String key = "mysample:method:mykey";
        String value = "myvalue";

        JedisTemplate jedisTemplate = new JedisTemplate(jedisPool);

        try {
            jedisTemplate.set(key, value);
            String result = jedisTemplate.get(key);
        } catch (JedisException e) {
            // exception handling
        }
    }

    /**
     * Write callback action to handle redis in below cases:
     * <p/>
     * 1. Execute more than one methods in one transaction or pipeline.
     * 2. The method you want didn't predefined and you are lazy to extend JedisTemplate.
     */
    private static void getAndSetAction() {
        final String counterKey = "mysample:counter";

        final String key = "mysample:action:mykey";
        final String value = "myvalue";

        JedisTemplate jedisTemplate = new JedisTemplate(jedisPool);

        try {
            //action without result
            jedisTemplate.execute(new JedisActionNoResult() {
                @Override
                public void action(Jedis jedis) {
                    jedis.set(key, value);
                }
            });

            //action with result
            String result = jedisTemplate.execute(new JedisAction<String>() {
                @Override
                public String action(Jedis jedis) {
                    return jedis.get(key);
                }
            });

            //transaction
            jedisTemplate.execute(new JedisActionNoResult() {
                @Override
                public void action(Jedis jedis) {
                    Transaction tx = jedis.multi();
                    tx.incr(counterKey);
                    tx.expire(counterKey, 60);
                    tx.exec();
                }
            });

            //pipeline
            jedisTemplate.execute(new JedisActionNoResult() {
                @Override
                public void action(Jedis jedis) {
                    Pipeline pl = jedis.pipelined();
                    pl.incr(counterKey);
                    pl.expire(counterKey, 60);
                    pl.sync();
                }
            });

        } catch (JedisException e) {
            // exception handling
        }
    }

    /**
     * Sample for lua script feature.
     */
    private static void luaScriptFeature() {

        //init once
        String script = "i am a lua script";
        JedisScriptExecutor jedisScriptExecutor = new JedisScriptExecutor(jedisPool);
        String scriptId = jedisScriptExecutor.load(script);

        //execute 
        String[] keys = new String[] { "key1" };
        String[] args = new String[] { "value1" };
        //List<String> keys = ...
        //List<String> args = ...;

        jedisScriptExecutor.execute(scriptId, keys, args);
    }

    /**
     * Sample for sharding feature.
     * <p/>
     * Pass more than one jedisPool to the JedisShardedTemplate, it will calculate which jedisPool will handle the key.
     * If only one jedisPool passed, it won't do the calculation, so please use JedisShardedTemplate by default.
     */
    private static void shardingFeature() {

        //init pool once
        //sentinel pool
        List<JedisPool> jedisPools = new JedisPoolBuilder().setHosts("rdsmon-1,rdsmon-2")
          .setShardedMasterNames("default,cluster2,cluster3").setPoolSize(100).buildShardedPools();
        JedisShardedTemplate jedisShardedTemplate = new JedisShardedTemplate(jedisPools);

        // method, same as JedisTemplate.
        final String key = "mysample:action:mykey";
        final String value = "myvalue";
        try {
            jedisShardedTemplate.set(key, value);
            String result = jedisShardedTemplate.get(key);
        } catch (JedisException e) {
            // exception handling
        }

        //action, must provided the which key you wish to change in the Action.
        jedisShardedTemplate.execute(key, new JedisActionNoResult() {
            @Override
            public void action(Jedis jedis) {
                jedis.set(key, value);
            }
        });

        //drop pool once
        for (JedisPool pool : jedisPools) {
            pool.destroy();
        }
    }

}
