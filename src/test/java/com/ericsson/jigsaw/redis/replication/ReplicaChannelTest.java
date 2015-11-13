/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication;

import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import com.ericsson.jigsaw.cluster.ClusterRuntime;
import com.ericsson.jigsaw.embedded.redis.RedisSentinel;
import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.replication.backlog.RedisReplicaBacklog;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;
import com.ericsson.jigsaw.redis.replication.impl.ReplicaChannelImpl;
import com.ericsson.jigsaw.redis.replication.impl.ReplicaDataSourceFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReplicaChannelTest {

    private static RedisSentinel localSentinel;
    private static RedisServer localRedis;
    private static RedisServer remoteRedis;

    private static RedisServer opLogRedis;

    private static JedisPool opJedisPool;
    private static JedisPool localSentinelPool;
    //client
    private static Jedis localRedisClient;
    private static Jedis remoteRedisClient;
    private static Jedis opLogRedisClient;

    private static final String MASTER_NAME = "default";
    private static final String defaultShardKey = "shard1";
    private static final String defaultCmd = "set";
    private static final int INDEX = 0;
    private static final String APP = "testApp";
    @Mock
    private ReplicaService remoteReplicaService;

    @Mock
    private JedisExceptionHandler mockExceptionHandler;

    @Mock
    private ClusterRuntime clusterRuntime;

    private ReplicaChannelImpl replicaChannel;

    private ObjectMapper mapper = new ObjectMapper();

    private static HostAndPort localAddr;

    @BeforeClass
    public static void beforeClass() throws Exception {
        //local redis
        localRedis = new RedisServer(38379);
        localRedis.start();
        remoteRedis = new RedisServer(38380);
        remoteRedis.start();
        localSentinel = RedisSentinel.builder().port(28379).masterName(MASTER_NAME).masterPort(38379).build();
        localSentinel.start();
        localSentinelPool = new JedisPoolBuilder().setUrl("sentinel://localhost:28379?masterNames=default&poolSize=3")
                .buildPool();
        localAddr = new HostAndPort("127.0.0.1", 38379);

        //opLog redis
        opLogRedis = new RedisServer(38381);
        opLogRedis.start();
        opJedisPool = new JedisPoolBuilder().setUrl("direct://localhost:38381?poolSize=3&poolName=oplog").buildPool();

        //client
        localRedisClient = new Jedis("127.0.0.1", 38379);
        remoteRedisClient = new Jedis("127.0.0.1", 38380);
        opLogRedisClient = new Jedis("127.0.0.1", 38381);

    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        ReplicaContext.setReplicaClusterRuntime(clusterRuntime);
        Mockito.when(clusterRuntime.isMaster()).thenReturn(true);
        HostAndPort remoteAddr = new HostAndPort("127.0.0.1", 38380);
        Mockito.when(remoteReplicaService.getConnectInfo(APP, INDEX)).thenReturn(remoteAddr.toString() + ",0");
        Mockito.doAnswer(new Answer() {

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                remoteRedisClient.slaveofNoOne();
                Mockito.when(remoteReplicaService.isSyncedUp(APP, INDEX)).thenReturn(true);
                return null;
            }

        }).when(remoteReplicaService).stopSyncUp(APP, INDEX);
        ReplicaDataSource masterDataSource = ReplicaDataSourceFactory.createLocalReplicaDataSource(localSentinelPool,
                mockExceptionHandler);

        ReplicaDataSource slaveDataSource = ReplicaDataSourceFactory.createRemoteReplicaDataSource(APP,
                remoteReplicaService, INDEX, mockExceptionHandler);
        ReplicaBacklog operationQueue = new RedisReplicaBacklog(opJedisPool, mockExceptionHandler);
        replicaChannel = new ReplicaChannelImpl(masterDataSource, slaveDataSource, operationQueue);
        replicaChannel.start();
    }

    @After
    public void tearDown() throws Exception {
        Mockito.reset(remoteReplicaService);
        localRedisClient.flushAll();
        opLogRedisClient.flushAll();
        remoteRedisClient.slaveofNoOne();
        remoteRedisClient.flushAll();
        replicaChannel.stop();
    }

    @AfterClass
    public static void afterClass() {
        opJedisPool.destroy();
        localSentinelPool.destroy();
        localRedisClient.close();
        remoteRedisClient.close();
        opLogRedisClient.close();
        localSentinel.stop();
        localRedis.stop();
        remoteRedis.stop();

        opLogRedis.stop();
    }

    @Test
    public void testReplicaSuccessfully() throws Exception {
        //prepare
        RedisOp opLogObj = generateOpLog("key1", "value1");
        String oplog1 = generateJsonString(opLogObj);
        opLogRedisClient.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog1);

        validateValue("key1", "value1");

        //push again
        RedisOp opLogObj2 = generateOpLog("key2", "value2");
        String oplog2 = generateJsonString(opLogObj2);
        opLogRedisClient.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog2);

        validateValue("key2", "value2");
    }

    //this case do not need to run , because it's a kryo implement
    @Ignore
    @Test
    public void testReplicaSuccessfullyKryo() throws Exception {
        //prepare
        RedisOp opLogObj = generateOpLog("key1", "value1");
        Kryo kryo = new Kryo();
        kryo.register(RedisOp.class, 0);
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeObject(output, opLogObj);
        Pipeline pipelined = opLogRedisClient.pipelined();

        pipelined.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE.getBytes(), output.toBytes());
        pipelined.sync();
        output.close();

        validateValue("key1", "value1");
    }

    private void validateValue(String key, String value) throws InterruptedException {
        String remoteValue = null;
        for (int i = 0; i < 20; i++) {
            remoteValue = remoteRedisClient.get(key);
            if (remoteValue == null) {
                Thread.sleep(200);
            } else {
                break;
            }
        }
        Assert.assertNotNull("no value found for key: " + key, remoteValue);
        Assert.assertEquals("replay fail", value, remoteValue);
    }

    @Test
    public void testFullSyncUpSuccessfully() throws Exception {
        RedisOp opLogObj = generateOpLog("key", "value1");
        String oplog = generateJsonString(opLogObj);
        replicaChannel.stop();
        localRedisClient.set("key", "value1");
        opLogRedisClient.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog);
        opLogRedisClient.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog);
        //set max queue size to 1
        replicaChannel.setDwQueueMaxLen(1L);
        replicaChannel.setMonitorQueueInterval(500);
        replicaChannel.start();

        waitSyncingUp();
        Assert.assertEquals("data is not synced up", "value1", remoteRedisClient.get("key"));
        Mockito.verify(remoteReplicaService).stopSyncUp(APP, INDEX);
    }

    @Test
    public void testIsSyncedUpReturnTrue() {
        Mockito.when(remoteReplicaService.isSyncedUp(APP, INDEX)).thenReturn(true);
        Assert.assertTrue("syncup should be true", replicaChannel.isSyncedUp());
        Mockito.verify(remoteReplicaService).isSyncedUp(APP, INDEX);
    }

    @Test
    public void testFullSyncUpSuccessfullyWithBackupRedisDown() throws Exception {
        RedisOp opLogObj = generateOpLog("key", "value1");
        String oplog = generateJsonString(opLogObj);
        replicaChannel.stop();
        //set max queue size to 1
        replicaChannel.setDwQueueMaxLen(1L);
        replicaChannel.setMonitorQueueInterval(500);
        replicaChannel.start();
        //stop backup redis
        remoteRedis.stop();
        remoteRedisClient.close();
        //insert 2 oplog to queue and reach max queue size
        localRedisClient.set("key", "value1");
        for (int i = 0; i < 120; i++) {
            opLogRedisClient.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog);
        }
        //remote redis recover and syncup
        remoteRedis.start();
        remoteRedisClient = new Jedis("127.0.0.1", 38380);
        waitSyncingUp();
        Assert.assertEquals("data is not synced up", "value1", remoteRedisClient.get("key"));
        Mockito.verify(remoteReplicaService, Mockito.atLeast(1)).stopSyncUp(APP, INDEX);
    }

    private void waitSyncingUp() throws InterruptedException {
        boolean syncup = false;
        for (int i = 0; i < 50; i++) {
            if (this.replicaChannel.getLastSyncUpTime() == 0) {
                Thread.sleep(500);
            } else {
                syncup = true;
                break;
            }
        }
        Assert.assertTrue("wait synup take too long", syncup);
    }

    @Test
    public void testReplayFailAndStop() throws Exception {
        remoteRedis.stop();
        RedisOp opLogObj = generateOpLog("key", "value1");
        String oplog = generateJsonString(opLogObj);
        opLogRedisClient.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog);
        RedisOpTestUtil.waitQueueConsume(opLogRedisClient);
        //wait 1s to let channel replay
        Thread.sleep(1000);
        replicaChannel.stop();
        String result = opLogRedisClient.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        Assert.assertEquals("oplog should be return", oplog, result);
        remoteRedis.start();
        remoteRedisClient = new Jedis("127.0.0.1", 38380);
        replicaChannel.start();
    }

    @Test
    public void testStopStart() throws Exception {
        replicaChannel.stop();

        //prepare
        RedisOp opLogObj = generateOpLog("key1", "value1");
        String oplog1 = generateJsonString(opLogObj);
        opLogRedisClient.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog1);
        Thread.sleep(1000);
        Long queueLen = opLogRedisClient.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        Assert.assertEquals("channel stop fail", Long.valueOf(1), queueLen);

        replicaChannel.start();

        validateValue("key1", "value1");

    }

    @Test
    public void testNotMaster() throws Exception {
        Mockito.when(clusterRuntime.isMaster()).thenReturn(false);
        replicaChannel.stop();
        RedisOp opLogObj = generateOpLog("key1", "value1");
        String oplog1 = generateJsonString(opLogObj);
        opLogRedisClient.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog1);
        replicaChannel.start();
        Thread.sleep(1000);
        Long queueLen = opLogRedisClient.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        Assert.assertEquals("cluster is not master,should not replica", Long.valueOf(1), queueLen);
    }

    @Test
    public void testReSyncup() throws Exception {
        localRedisClient.set("key", "value1");
        replicaChannel.reSyncUp();
        waitSyncupFinish();
        Assert.assertEquals("data is not synced up", "value1", remoteRedisClient.get("key"));
        Mockito.verify(remoteReplicaService).stopSyncUp(APP, INDEX);
    }

    private void waitSyncupFinish() {
        for (int i = 0; i < 50; i++) {
            if (!replicaChannel.isSyncedUp()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    //do nothing
                }
            } else {
                return;
            }
        }
        fail("wait sync up finish too long");
    }

    protected RedisOp generateOpLog(String key, String value) throws Exception {
        List<Object> arguments = new ArrayList<Object>();
        arguments.add(key);
        arguments.add(value);
        List<String> parameterTypes = new ArrayList<String>();
        parameterTypes.add("java.lang.String");
        parameterTypes.add("java.lang.String");
        return new RedisOp(defaultShardKey, defaultCmd, arguments, parameterTypes);

    }

    private String generateJsonString(RedisOp oplog) throws JsonGenerationException, JsonMappingException,
            IOException {
        return mapper.writeValueAsString(oplog);
    }

}
