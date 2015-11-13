package com.ericsson.jigsaw.redis.replication.backlog;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.JedisShardedTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.backlog.RedisOpUtilKryo;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.replication.JedisReplicationConstants;
import com.ericsson.jigsaw.redis.replication.RedisOp;
import com.ericsson.jigsaw.redis.replication.RedisOpUtil;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;
import com.ericsson.jigsaw.redis.shard.NormHashShardingProvider;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.collect.Lists;

public class RedisBacklogManagerTest {
    private static int REDIS_PORT = 16397;
    private static int SENTINEL_REDIS_PORT = 36397;

    private static RedisServer server;

    private static Jedis dwJedis;

    private static JedisExceptionHandler exceptionHandler;

    @BeforeClass
    public static void beforeClass() throws IOException {
        exceptionHandler = Mockito.mock(JedisExceptionHandler.class);

        server = new RedisServer(REDIS_PORT);
        server.start();

        dwJedis = new Jedis("127.0.0.1", REDIS_PORT);
        dwJedis.del(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
    }

    @After
    public void tearDown() {
        dwJedis.flushAll();
        Mockito.reset(exceptionHandler);
    }

    @AfterClass
    public static void afterClass() {
        dwJedis.close();
        server.stop();
    }

    @Test
    public void testEnqueue() throws Exception {
        // prepare
        final String shardKey = "shard1";
        final String command = "set";
        final String key = "manager";
        final String value = "enqueue";
        Object[] args = { key, value };
        List<String> parameterTypes = Lists.newArrayList();
        parameterTypes.add("java.lang.String");
        parameterTypes.add("java.lang.String");

        String op = RedisOpUtil.buildOperationLogString(shardKey, command, args, parameterTypes);

        JedisPool jedisPool = new JedisPoolBuilder().setHosts("manager").setPort(SENTINEL_REDIS_PORT)
                .setMasterName("direct:localhost:" + REDIS_PORT).setPoolSize(2).setPoolName("manager").buildPool();

        JedisShardedTemplate jedisTemplate = new JedisShardedTemplate(
                new NormHashShardingProvider<JedisTemplate>(true), null, Lists.newArrayList(jedisPool));

        RedisBacklogManager doubleWriteManager = new RedisBacklogManager(jedisTemplate);

        List<Serializable> ops = new ArrayList<Serializable>();
        // replay
        for (int i = 0; i < 230; i++) {
            ops.add(op);
        }
        doubleWriteManager.feed(shardKey, ops);
        // verify

        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(230, queueLen);

        String expectedOpLog = "{\"shardKey\":\"shard1\",\"cmd\":\"set\",\"args\":[\"manager\",\"enqueue\"],\"paramTypes\":[\"java.lang.String\",\"java.lang.String\"]}";
        String rtnOpLog = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);

        assertEquals(expectedOpLog, rtnOpLog);
        dwJedis.flushAll();

    }

    @Ignore
    @Test
    public void testEnqueueKryo() {
        // prepare
        final String shardKey = "shard1";
        final String command = "set";
        final String key = "manager";
        final String value = "enqueue";
        String[] args = { key, value };
        List<String> parameterTypes = Lists.newArrayList();
        parameterTypes.add("java.lang.String");
        parameterTypes.add("java.lang.String");

        byte[] opByte = RedisOpUtilKryo.buildOperationLogByte(shardKey, command, args, parameterTypes);

        JedisPool jedisPool = new JedisPoolBuilder().setHosts("manager").setPort(SENTINEL_REDIS_PORT)
                .setMasterName("direct:localhost:" + REDIS_PORT).setPoolSize(2).setPoolName("manager").buildPool();

        JedisShardedTemplate jedisTemplate = new JedisShardedTemplate(
                new NormHashShardingProvider<JedisTemplate>(true), null, Lists.newArrayList(jedisPool));

        RedisBacklogManager doubleWriteManager = new RedisBacklogManager(jedisTemplate);

        // replay
        List<Serializable> ops = new ArrayList<Serializable>();
        for (int i = 0; i < 230; i++) {
            ops.add(opByte);
        }
        doubleWriteManager.enqueueKryo(shardKey, ops);
        // verify

        long queueLen = dwJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals(230, queueLen);

        byte[] rtnOpLogByte = dwJedis.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE.getBytes());

        Kryo kryo = new Kryo();
        kryo.register(RedisOp.class);
        Input input = new Input(rtnOpLogByte);
        RedisOp resultObj = kryo.readObject(input, RedisOp.class);

        assertEquals(command, resultObj.getCmd());
        dwJedis.flushAll();

    }

}
