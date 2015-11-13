/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication.backlog;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.cluster.ClusterRuntime;
import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.replication.JedisReplicationConstants;
import com.ericsson.jigsaw.redis.replication.RedisOp;
import com.ericsson.jigsaw.redis.replication.RedisOpTestUtil;
import com.ericsson.jigsaw.redis.replication.ReplicaContext;
import com.ericsson.jigsaw.redis.replication.backlog.RedisReplicaBacklog;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RedisReplicaBacklogTest {

    private static RedisServer redisServer;
    private static Jedis client;
    private static RedisReplicaBacklog opQueue;

    private static final String defaultShardKey = "shard1";
    private static final String defaultCmd = "set";
    private static ObjectMapper mapper;
    private static JedisPool jedisPool;
    private static ClusterRuntime clusterRuntimeMock;

    @BeforeClass
    public static void beforeClass() throws Exception {
        redisServer = new RedisServer(18379);
        redisServer.start();
        client = new Jedis("127.0.0.1", 18379);
        jedisPool = new JedisPoolBuilder().setUrl("direct://localhost:18379?poolSize=3&poolName=oplog").buildPool();
        clusterRuntimeMock = Mockito.mock(ClusterRuntime.class);
        ReplicaContext.setReplicaClusterRuntime(clusterRuntimeMock);
        mapper = new ObjectMapper();
    }

    @Before
    public void setup() {
        opQueue = new RedisReplicaBacklog(jedisPool, null);
    }

    @After
    public void tearDown() {
        Mockito.reset(clusterRuntimeMock);
        client.flushAll();
        opQueue.clear();
    }

    @AfterClass
    public static void afterClass() {
        jedisPool.destroy();
        client.close();
        redisServer.stop();
    }

    @Test
    public void testPush() throws Exception {
        Mockito.when(clusterRuntimeMock.isMaster()).thenReturn(true);
        opQueue.start();
        try {
            //prepare
            String opLog1 = generateOpLog("key1", "test1");
            opQueue.push(opLog1);
            String opLog2 = generateOpLog("key2", "test2");
            opQueue.push(opLog2);
            RedisOpTestUtil.waitQueueConsume(client);
            //verify
            String result = opQueue.pop();
            assertEquals("log1 not correct", opLog1, result);
            result = opQueue.pop();
            assertEquals("log2 not correct", opLog2, result);

        } finally {
            opQueue.stop();
        }

    }

    @Test
    public void testPop() throws Exception {
        Mockito.when(clusterRuntimeMock.isMaster()).thenReturn(true);
        opQueue.start();
        try {
            //prepare
            String opLog1 = generateOpLog("key1", "test1");
            String opLog2 = generateOpLog("key2", "test2");
            client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog1);
            client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog2);
            RedisOpTestUtil.waitQueueConsume(client);
            String result = opQueue.pop();
            assertEquals("log1 not correct", opLog1.toString(), result.toString());
            result = opQueue.pop();
            assertEquals("log2 not correct", opLog2.toString(), result.toString());
        } finally {
            opQueue.stop();
        }
    }

    @Test
    public void testBatchPop() throws Exception {
        Mockito.when(clusterRuntimeMock.isMaster()).thenReturn(true);
        opQueue.start();
        try {
            //prepare
            String opLog1 = generateOpLog("key1", "test1");
            String opLog2 = generateOpLog("key2", "test2");
            client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog1);
            client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog2);
            RedisOpTestUtil.waitQueueConsume(client);
            List<String> results = opQueue.batchPop(3);
            assertEquals("log size not correct", 2, results.size());
            assertEquals("log1 not correct", opLog1.toString(), results.get(0).toString());
            assertEquals("log2 not correct", opLog2.toString(), results.get(1).toString());
        } finally {
            opQueue.stop();
        }
    }

    @Test
    public void testMixOperation() throws JsonGenerationException, JsonMappingException, IOException {
        Mockito.when(clusterRuntimeMock.isMaster()).thenReturn(true);
        opQueue.start();
        try {
            //prepare
            String opLog1 = generateOpLog("key1", "test1");
            opQueue.push(opLog1);
            RedisOpTestUtil.waitQueueConsume(client);
            List<String> results = opQueue.batchPop(3);
            assertEquals("log size not correct", 1, results.size());
            assertEquals("log1 not correct", opLog1.toString(), results.get(0).toString());
        } finally {
            opQueue.stop();
        }
    }

    @Test
    public void testDequeueIsNotMaster() throws Exception {
        Mockito.when(clusterRuntimeMock.isMaster()).thenReturn(false);
        String opLog1 = generateOpLog("key1", "test1");
        client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog1);
        opQueue.start();
        try {
            Thread.sleep(1000);
            assertEquals("queue should not be consume", Long.valueOf(1),
                    client.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE));
        } finally {
            opQueue.stop();
        }

    }

    @Test
    public void testStartStop() throws Exception {
        Mockito.when(clusterRuntimeMock.isMaster()).thenReturn(true);
        opQueue.start();
        opQueue.stop();
        String opLog1 = generateOpLog("key1", "test1");
        client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog1);
        Thread.sleep(1000);
        Long queueLen = client.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals("queue should not be pop", Long.valueOf(1), queueLen);
        opQueue.start();
        try {
            RedisOpTestUtil.waitQueueConsume(client);
            String result = opQueue.pop();
            assertEquals("log not correct", opLog1, result);
        } finally {
            opQueue.stop();
        }
    }

    @Test
    public void testReturnOpToQueue() throws Exception {
        String opLog1 = generateOpLog("key1", "test1");
        String opLog2 = generateOpLog("key2", "test2");

        opQueue.returnOperationToQueue(Arrays.asList(opLog1, opLog2));
        String firstOp = client.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals("op log error", opLog1, firstOp);
        String secOp = client.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals("op log error", opLog2, secOp);
    }

    @Test
    public void testBufferReturnQueueWhenStop() throws Exception {
        Mockito.when(clusterRuntimeMock.isMaster()).thenReturn(true);
        opQueue.start();
        String opLog1 = generateOpLog("key1", "test1");
        String opLog2 = generateOpLog("key2", "test2");
        client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog1);
        client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog2);
        RedisOpTestUtil.waitQueueConsume(client);
        opQueue.stop();
        String firstOp = client.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals("op log error", opLog1, firstOp);
        String secondOp = client.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals("op log error", opLog2, secondOp);

    }

    @Test
    public void testBufferReturnQueueWhenStopWithLogsRemain() throws Exception {
        Mockito.when(clusterRuntimeMock.isMaster()).thenReturn(true);
        opQueue.setBuffer(new ArrayBlockingQueue<String>(2));
        String opLog1 = generateOpLog("key1", "test1");
        String opLog2 = generateOpLog("key2", "test2");
        //buffer should be block when insert the 3th log
        String opLog3 = generateOpLog("key3", "test3");
        client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog1);
        client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog2);
        client.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, opLog3);
        opQueue.start();
        RedisOpTestUtil.waitQueueConsume(client);
        opQueue.stop();
        String firstOp = client.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals("op log error", opLog1, firstOp);
        String secondOp = client.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals("op log error", opLog2, secondOp);
        String thirdOp = client.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        assertEquals("op log error", opLog3, thirdOp);

    }

    protected String generateOpLog(String key, String value) throws JsonGenerationException, JsonMappingException,
            IOException {
        List<Object> arguments = new ArrayList<Object>();
        arguments.add(key);
        arguments.add(value);
        List<String> parameterTypes = new ArrayList<String>();
        parameterTypes.add("java.lang.String");
        parameterTypes.add("java.lang.String");
        final RedisOp operation = new RedisOp(defaultShardKey, defaultCmd, arguments, parameterTypes);
        return mapper.writeValueAsString(operation);
    }
}
