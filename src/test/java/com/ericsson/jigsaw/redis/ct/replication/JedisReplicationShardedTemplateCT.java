package com.ericsson.jigsaw.redis.ct.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.PipelineActionNoResult;
import com.ericsson.jigsaw.redis.JigsawJedisException;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.replication.JedisReplicationConstants;
import com.ericsson.jigsaw.redis.replication.JedisReplicationShardedTemplate;
import com.ericsson.jigsaw.redis.replication.RedisOp;
import com.ericsson.jigsaw.redis.replication.exception.JedisReplicationException;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;
import com.ericsson.jigsaw.redis.shard.NormHashShardingProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

public class JedisReplicationShardedTemplateCT {

    private static final int Q_SENTINEL_PORT = 36380;

    private static final int DATA_SENTINEL_PORT = 36379;

    private static final int Q_REDIS_PORT = 16380;

    private static final int DATA_REDIS_PORT = 16379;

    private static final String defaultShardKey = "shard1";

    private static RedisServer trafficRedisServer;
    private static Jedis trafficJedis;

    private static RedisServer dwRedisServer;
    private static Jedis dwJedis;

    private static JedisExceptionHandler exceptionHandler;
    private static Logger logger;

    private static JedisReplicationShardedTemplate jedisGeoRedTemplate;

    @BeforeClass
    public static void beforeClass() throws IOException {
        exceptionHandler = Mockito.mock(JedisExceptionHandler.class);
        logger = Mockito.mock(Logger.class);

        // traffic data
        trafficRedisServer = new RedisServer(DATA_REDIS_PORT);
        trafficRedisServer.start();
        JedisPool trafficJedisPool = new JedisPoolBuilder().setHosts("traffic").setPort(DATA_SENTINEL_PORT)
                .setMasterName("direct:localhost:" + DATA_REDIS_PORT).setPoolSize(2).setPoolName("traffic").buildPool();

        trafficJedis = new Jedis("127.0.0.1", DATA_REDIS_PORT);

        // doublewrite
        dwRedisServer = new RedisServer(Q_REDIS_PORT);
        dwRedisServer.start();
        JedisPool dwJedisPool = new JedisPoolBuilder().setHosts("doublewrite").setPort(Q_SENTINEL_PORT)
                .setMasterName("direct:localhost:" + Q_REDIS_PORT).setPoolSize(2).setPoolName("doublewrite")
                .buildPool();

        dwJedis = new Jedis("127.0.0.1", Q_REDIS_PORT);

        jedisGeoRedTemplate = new JedisReplicationShardedTemplate(new NormHashShardingProvider<JedisTemplate>(true),
                Lists.newArrayList(trafficJedisPool), Lists.newArrayList(dwJedisPool), exceptionHandler);
    }

    @After
    public void tearDown() {
        trafficJedis.flushAll();
        dwJedis.flushAll();

        Mockito.reset(logger);
    }

    @AfterClass
    public static void afterClass() {
        trafficJedis.close();
        trafficRedisServer.stop();

        dwJedis.close();
        dwRedisServer.stop();
    }

    @Test
    public void testTemplateSetMethod() throws InterruptedException {

        // prepare
        final String key = "set";
        final String value = "set-method";

        // replay
        jedisGeoRedTemplate.set(defaultShardKey, key, value);

        // verify
        // a) traffic data
        String rtnValue = jedisGeoRedTemplate.get(key);
        assertEquals(value, rtnValue);

        // b) operation log queue
        Thread.sleep(500);

        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"set\",\"args\":[\"set\",\"set-method\"],\"paramTypes\":[\"java.lang.String\",\"java.lang.String\"]}";
        String rtnOpLog = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        assertEquals(expectedOpLog, rtnOpLog);
    }

    @Test
    public void testPipelineSadd() throws InterruptedException {
        // prepare
        final String key = "pipeline";
        final String value = "sadd";

        createDwBuffer();

        // replay
        jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
            @Override
            public void action(Pipeline pipeline) {
                pipeline.sadd(key, value);
            }
        });

        // verify
        // a) traffic data
        Set<String> rtnSet = jedisGeoRedTemplate.smembers(key);
        assertTrue(rtnSet.contains(value));

        // b) operation log queue
        Thread.sleep(500);

        // the first element is used to create queue, pop it before verify
        dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"sadd\",\"args\":[\"pipeline\",[\"sadd\"]],\"paramTypes\":[\"java.lang.String\",\"[Ljava.lang.String;\"]}";
        String rtnOpLog = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        assertEquals(expectedOpLog, rtnOpLog);
    }

    @Test
    public void testPipelineSet() throws InterruptedException {
        // prepare
        final String key = "pipeline";
        final String value = "set";

        createDwBuffer();

        // replay
        jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
            @Override
            public void action(Pipeline pipeline) {
                pipeline.set(key, value);
            }
        });

        // verify
        // a) traffic data
        String rtnValue = jedisGeoRedTemplate.get(key);
        assertEquals(value, rtnValue);

        // b) operation log queue
        Thread.sleep(500);

        // the first element is used to create queue, pop it before verify
        dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"set\",\"args\":[\"pipeline\",\"set\"],\"paramTypes\":[\"java.lang.String\",\"java.lang.String\"]}";
        String rtnOpLog = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
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
        String rtnValue = jedisGeoRedTemplate.get(key);
        assertEquals(value, rtnValue);

        // b) operation log queue
        Thread.sleep(500);

        // the first element is used to create queue, pop it before verify
        dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"setex\",\"args\":[\"pipeline\",1,\"setex\"],\"paramTypes\":[\"java.lang.String\",\"int\",\"java.lang.String\"]}";
        String rtnOpLog = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(expectedOpLog, rtnOpLog);
    }

    @Test
    public void testPipelineDel() throws InterruptedException {
        // prepare
        final String key = "pipeline-del";
        trafficJedis.set(key, "pipeline-del");

        createDwBuffer();

        // replay
        jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
            @Override
            public void action(Pipeline pipeline) {
                pipeline.del(key);
            }
        });

        // verify
        // a) traffic data
        String rtnValue = jedisGeoRedTemplate.get(key);
        assertNull(rtnValue);

        // b) operation log queue
        Thread.sleep(500);

        // the first element is used to create queue, pop it before verify
        dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"del\",\"args\":[\"pipeline-del\"],\"paramTypes\":[\"java.lang.String\"]}";
        String rtnOpLog = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(expectedOpLog, rtnOpLog);
    }

    @Test
    public void testPipelineSrem() throws InterruptedException {
        // prepare
        final String key = "pipeline";
        final String member = "srem";

        trafficJedis.sadd(key, member);

        createDwBuffer();

        // replay
        jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
            @Override
            public void action(Pipeline pipeline) {
                pipeline.srem(key, member);
            }
        });

        // verify
        // a) traffic data
        Set<String> rtnValue = jedisGeoRedTemplate.smembers(key);
        assertTrue(!rtnValue.contains(member));

        // b) operation log queue
        Thread.sleep(500);

        // the first element is used to create queue, pop it before verify
        dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"srem\",\"args\":[\"pipeline\",[\"srem\"]],\"paramTypes\":[\"java.lang.String\",\"[Ljava.lang.String;\"]}";
        String rtnOpLog = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(expectedOpLog, rtnOpLog);
    }

    @Test
    public void testJedisNonIdemponentOperation() throws InterruptedException {
        // prepare
        final String key = "PipelineActionNoResult";
        final String value = "get";

        createDwBuffer();
        trafficJedis.set(key, value);

        // replay
        jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
            @Override
            public void action(Pipeline pipeline) {
                pipeline.get(key);
            }
        });

        // verify
        // a) no record in operation log queue
        Thread.sleep(500);
        // the first element is used to create queue, pop it before verify

        // only queue create log
        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, queueLen);

        // after pop, should no record
        dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(0, queueLen);
    }

    @Test
    public void testInvalidParameter() {
        // prepare
        JedisPool jedisPool1 = new JedisPoolBuilder().setHosts("traffic").setPort(DATA_SENTINEL_PORT)
                .setMasterName("direct:localhost:6398").setPoolSize(2).setPoolName("invalidParamter").buildPool();

        JedisPool jedisPool2 = new JedisPoolBuilder().setHosts("invalidParamter").setPort(26397)
                .setMasterName("direct:localhost:6397").setPoolSize(100).setPoolName("invalidParamter").buildPool();
        try {
            // replay
            @SuppressWarnings("unused")
            JedisReplicationShardedTemplate template = new JedisReplicationShardedTemplate(
                    new NormHashShardingProvider<JedisTemplate>(true), Lists.newArrayList(jedisPool1, jedisPool2),
                    Lists.newArrayList(jedisPool1), exceptionHandler);

            fail("The size of traffic and doublewrite queue should be equal!");

        } catch (Exception e) {
            assertTrue(e instanceof JedisReplicationException);
            assertEquals("description: Pool size not equal, data pool size: 2, backlog queue pool size: 1",
                    e.getMessage());
        }
    }

    @Test
    public void testEnqueueFIFO() throws IOException, InterruptedException {
        // prepare
        final String key = "pipeline";

        // replay
        jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
            @Override
            public void action(Pipeline pipeline) {
                for (int i = 1; i < 1001; i++) {
                    pipeline.set(key, String.valueOf(i));
                }
                pipeline.del(key);
            }
        });

        // verify
        // a) traffic data
        String rtnValue = jedisGeoRedTemplate.get(key);
        assertNull(rtnValue);

        // b) operation log queue
        Thread.sleep(500);

        // verify queue length
        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1001, queueLen);

        // verify set command sequence
        for (int i = 1; i < queueLen; i++) {
            String keyInQueue = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
            ObjectMapper mapper = new ObjectMapper();
            RedisOp operation = mapper.readValue(keyInQueue, RedisOp.class);
            String command = operation.getCmd();
            assertEquals("set", command);
            assertTrue(operation.getArgs().contains(String.valueOf(i)));
        }

        // verify del command
        String keyInQueue = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        ObjectMapper mapper = new ObjectMapper();
        RedisOp operation = mapper.readValue(keyInQueue, RedisOp.class);
        String command = operation.getCmd();
        assertEquals("del", command);

    }

    //write traffic will fail now, skip this test
    @Ignore
    @Test
    public void testWriteTrafficDataSucessWithOpQueueDown() {
        // prepare
        final String key = "pipeline";

        // stop operation queue
        dwJedis.flushAll();
        dwJedis.close();
        dwRedisServer.stop();

        // replay
        jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
            @Override
            public void action(Pipeline pipeline) {
                for (int i = 1; i < 100; i++) {
                    pipeline.set(key, String.valueOf(i));
                }
            }
        });

        // verify
        // a) traffic data
        String rtnValue = jedisGeoRedTemplate.get(key);
        assertEquals("99", rtnValue);

        // restart for teardown
        dwRedisServer.start();
        dwJedis = new Jedis("127.0.0.1", Q_REDIS_PORT);
    }

    @Test
    public void testTrafficDataServerDown() {
        // prepare
        JedisPool trafficJedisPool = new JedisPoolBuilder().setHosts("traffic").setPort(DATA_SENTINEL_PORT + 1)
                .setMasterName("direct:localhost:" + DATA_REDIS_PORT + 1).setPoolSize(2).setPoolName("traffic")
                .buildPool();

        JedisPool dwJedisPool = new JedisPoolBuilder().setHosts("doublewrite").setPort(Q_SENTINEL_PORT + 1)
                .setMasterName("direct:localhost:" + Q_REDIS_PORT + 1).setPoolSize(2).setPoolName("doublewrite")
                .buildPool();

        JedisReplicationShardedTemplate jedisGeoRedTemplate = new JedisReplicationShardedTemplate(
                new NormHashShardingProvider<JedisTemplate>(true), Lists.newArrayList(trafficJedisPool),
                Lists.newArrayList(dwJedisPool), exceptionHandler);

        final String key = "pipeline";

        // stop operation queue
        dwJedis.del(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        // replay
        try {

            jedisGeoRedTemplate.execute(defaultShardKey, new PipelineActionNoResult() {
                @Override
                public void action(Pipeline pipeline) {
                    for (int i = 1; i < 100; i++) {
                        pipeline.set(key, String.valueOf(i));
                    }
                }
            });

            fail("should fail. Traffic redis server is down!");
        } catch (Exception e) {
            assertTrue(e instanceof JigsawJedisException);
        }

        // verify
        // a) traffic data
        long qLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(0, qLen);
    }

    private void createDwBuffer() {

        dwJedis.del(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        dwJedis.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, "DoubleWriteBuffer");

        long len = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(1, len);
    }
}
