package com.ericsson.jigsaw.redis.ct.replication;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.PipelineActionNoResult;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.replication.JedisReplicationConstants;
import com.ericsson.jigsaw.redis.replication.JedisReplicationShardedTemplate;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;
import com.ericsson.jigsaw.redis.shard.NormHashShardingProvider;

public class JedisReplicationMultiShardsCT {

    private static final int Q_REDIS_PORT = 16383;

    private static final int DATA_REDIS_PORT = 16379;

    private static final String defaultShardKey = "shard1";

    private static final String DATA_URL = "direct://localhost:16379?poolSize=3&poolName=traffic";
    private static final String QUEUE_URL = "direct://localhost:16383?poolSize=3&poolName=dwbuffer";

    private static RedisServer tServer1;
    private static RedisServer tServer2;
    private static Jedis tJedis1;
    private static Jedis tJedis2;

    private static RedisServer dwServer1;
    private static RedisServer dwServer2;
    private static Jedis dwJedis1;
    private static Jedis dwJedis2;

    private static JedisExceptionHandler exceptionHandler;
    private static Logger logger;

    private static JedisReplicationShardedTemplate jedisGeoRedTemplate;

    @BeforeClass
    public static void beforeClass() throws Exception {
        exceptionHandler = Mockito.mock(JedisExceptionHandler.class);
        logger = Mockito.mock(Logger.class);

        // traffic data
        tServer1 = new RedisServer(DATA_REDIS_PORT);
        tServer1.start();
        tServer2 = new RedisServer(DATA_REDIS_PORT + 1);
        tServer2.start();

        tJedis1 = new Jedis("127.0.0.1", DATA_REDIS_PORT);
        tJedis2 = new Jedis("127.0.0.1", DATA_REDIS_PORT + 1);

        // doublewrite
        dwServer1 = new RedisServer(Q_REDIS_PORT);
        dwServer1.start();
        dwServer2 = new RedisServer(Q_REDIS_PORT + 1);
        dwServer2.start();

        dwJedis1 = new Jedis("127.0.0.1", Q_REDIS_PORT);
        dwJedis2 = new Jedis("127.0.0.1", Q_REDIS_PORT + 1);

        // jedisPool
        List<JedisPool> tJedisPool = new JedisPoolBuilder().setUrl(DATA_URL).buildShardedPools();
        List<JedisPool> qJedisPool = new JedisPoolBuilder().setUrl(QUEUE_URL).buildShardedPools();

        jedisGeoRedTemplate = new JedisReplicationShardedTemplate(new NormHashShardingProvider<JedisTemplate>(true),
                tJedisPool, qJedisPool, exceptionHandler);

    }

    @After
    public void tearDown() throws InterruptedException {
        tJedis1.flushAll();
        tJedis2.flushAll();
        dwJedis1.flushAll();
        dwJedis2.flushAll();

        Mockito.reset(logger);
    }

    @AfterClass
    public static void afterClass() {
        tJedis1.close();
        tServer1.stop();
        tJedis2.close();
        tServer2.stop();

        dwJedis1.close();
        dwServer1.stop();
        dwJedis2.close();
        dwServer2.stop();
    }


    @Test
    public void testTemplateSetMethod() throws InterruptedException {

        // prepare
        final String key = "set";
        final String value = "set-method";

        createDwBuffer();

        // replay
        jedisGeoRedTemplate.set(defaultShardKey, key, value);

        // verify
        // a) traffic data
        String rtnValue = jedisGeoRedTemplate.get(key);
        assertEquals(value, rtnValue);

        // b) operation log queue
        Thread.sleep(500);

        // the first element is used to create queue, pop it before verify
        dwJedis1.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        long queueLen = dwJedis1.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"set\",\"args\":[\"set\",\"set-method\"],\"paramTypes\":[\"java.lang.String\",\"java.lang.String\"]}";
        String rtnOpLog = dwJedis1.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        assertEquals(expectedOpLog, rtnOpLog);
    }

    @Test
    public void testPipelineSetex() throws InterruptedException {

        // prepare
        final String key = "pipeline";
        final String value = "setex";
        final int expire = 1;

        createDwBuffer();

        // replay
        jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
            @Override
            public void action(Pipeline pipeline) {
                pipeline.setex(key, expire, value);
            }
        });

        // verify
        // a) traffic data
        String rtnValue = jedisGeoRedTemplate.get(defaultShardKey, key);
        assertEquals(value, rtnValue);

        // b) operation log queue
        Thread.sleep(500);

        // the first element is used to create queue, pop it before verify
        dwJedis1.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        long queueLen = dwJedis1.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"setex\",\"args\":[\"pipeline\",1,\"setex\"],\"paramTypes\":[\"java.lang.String\",\"int\",\"java.lang.String\"]}";
        String rtnOpLog = dwJedis1.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(expectedOpLog, rtnOpLog);
    }

    private void createDwBuffer() {

        dwJedis1.del(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        dwJedis1.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, "DoubleWriteBuffer");
        long len1 = dwJedis1.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, len1);

        dwJedis2.del(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        dwJedis2.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, "DoubleWriteBuffer");

        long len2 = dwJedis2.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, len2);
    }
}
