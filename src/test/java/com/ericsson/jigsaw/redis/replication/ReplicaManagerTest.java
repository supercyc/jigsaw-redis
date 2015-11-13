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
import java.util.Arrays;
import java.util.List;

import org.apache.cxf.feature.AbstractFeature;
import org.apache.cxf.feature.LoggingFeature;
import org.apache.cxf.jaxrs.JAXRSServerFactoryBean;
import org.apache.cxf.jaxrs.client.JAXRSClientFactory;
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

import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import com.ericsson.jigsaw.cluster.ClusterRuntime;
import com.ericsson.jigsaw.embedded.redis.RedisSentinel;
import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.pool.JedisSentinelPool;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

public class ReplicaManagerTest {

    private static RedisServer redis1;
    private static RedisServer redis2;

    private static RedisSentinel sentinel;

    private static RedisServer redis3;
    private static RedisServer redis4;

    private static Jedis sentinelClient;

    private static Jedis client3;
    private static Jedis client4;

    private static Jedis client1;
    private static Jedis client2;

    //app1
    private static JedisPool masterPool1;
    private static JedisPool masterPool2;
    private static JedisPool operationPool1;
    private static JedisPool operationPool2;

    //app2
    private static JedisPool masterPool3;
    private static JedisPool masterPool4;
    private static JedisPool operationPool3;
    private static JedisPool operationPool4;

    private static ReplicaInfo replicaInfo4App1;
    private static ReplicaInfo replicaInfo4App2;

    private ReplicaManager replicaManager;

    private static ReplicaService replicaService;

    @Mock
    private static ClusterRuntime clusterRuntime;

    private static final String remoteReplicaServiceUrl = "http://127.0.0.1:18071/ReplicaService";

    private static final String defaultShardKey = "shard1";
    private static final String defaultCmd = "set";
    private static final String APP_1 = "testApp1";
    private static final String APP_2 = "testApp2";
    private static final String LOCAL_HOST = "127.0.0.1";

    private static final String REDIS_CONINFO_1 = "127.0.0.1:6311,0";
    private static final String REDIS_CONINFO_2 = "127.0.0.1:6312,0";
    private static final String REDIS_CONINFO_3 = "127.0.0.1:6411,0";
    private static final String REDIS_CONINFO_4 = "127.0.0.1:6412,0";
    private static final String REDIS_CONINFO_1_1 = "127.0.0.1:6311,1";

    private ObjectMapper mapper = new ObjectMapper();

    @BeforeClass
    public static void beforeClass() throws Exception {
        prepareRedis();
        prepareReplicaInfo();

        replicaService = Mockito.mock(ReplicaService.class);
        startReplicaService();
    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        expectReplicaService();
        expectClusterRuntime();
    }

    private void expectClusterRuntime() {
        ReplicaContext.setReplicaClusterRuntime(clusterRuntime);
        Mockito.when(clusterRuntime.isMaster()).thenReturn(true);
    }

    private void expectReplicaService() {
        Mockito.when(replicaService.getConnectInfo(APP_1, 0)).thenReturn(REDIS_CONINFO_1);
        Mockito.when(replicaService.getConnectInfo(APP_1, 1)).thenReturn(REDIS_CONINFO_2);
        Mockito.when(replicaService.getConnectInfo(APP_2, 0)).thenReturn(REDIS_CONINFO_3);
        Mockito.when(replicaService.getConnectInfo(APP_2, 1)).thenReturn(REDIS_CONINFO_4);
    }

    private static void prepareRedis() throws IOException {
        redis1 = new RedisServer(6311);
        redis2 = new RedisServer(6312);
        redis3 = new RedisServer(6411);
        redis4 = new RedisServer(6412);
        redis1.start();
        redis2.start();
        redis3.start();
        redis4.start();

        client1 = new Jedis(LOCAL_HOST, 6311);
        client2 = new Jedis(LOCAL_HOST, 6312);
        client3 = new Jedis(LOCAL_HOST, 6411);
        client4 = new Jedis(LOCAL_HOST, 6412);

        sentinel = RedisSentinel.builder().port(26411).masterName("master1_app1").masterPort(6311).build();
        sentinel.start();
        sentinelClient = new Jedis(LOCAL_HOST, 26411);
        sentinelClient.sentinelMonitor("master2_app1", LOCAL_HOST, 6312, 1);
        sentinelClient.sentinelMonitor("master1_app2", LOCAL_HOST, 6411, 1);
        sentinelClient.sentinelMonitor("master2_app2", LOCAL_HOST, 6412, 1);
    }

    private static void prepareReplicaInfo() {
        //master pools for app1
        masterPool1 = new JedisPoolBuilder().setUrl(
                "sentinel://localhost:26411?poolName=master1_app1&masterNames=master1_app1&poolSize=3&database=1")
                .buildPool();
        masterPool2 = new JedisPoolBuilder().setUrl(
                "sentinel://localhost:26411?poolName=master2_app1&masterNames=master2_app1&poolSize=3").buildPool();
        //operation log pools for app1
        operationPool1 = new JedisPoolBuilder().setUrl("direct://localhost:6411?poolName=op1_app1&poolSize=3")
                .buildPool();
        operationPool2 = new JedisPoolBuilder().setUrl("direct://localhost:6412?poolName=op2_app1&poolSize=3")
                .buildPool();

        replicaInfo4App1 = new ReplicaInfo(APP_1, Arrays.asList(masterPool1, masterPool2), Arrays.asList(
                operationPool1, operationPool2));

        //master pools for app2
        masterPool3 = new JedisPoolBuilder().setUrl(
                "sentinel://localhost:26411?poolName=master1_app2&masterNames=master1_app2&poolSize=3").buildPool();
        masterPool4 = new JedisPoolBuilder().setUrl(
                "sentinel://localhost:26411?poolName=master2_app2&masterNames=master2_app2&poolSize=3").buildPool();
        //operation log pools for app2
        operationPool3 = new JedisPoolBuilder().setUrl("direct://localhost:6311?poolName=op1_app2&poolSize=3")
                .buildPool();
        operationPool4 = new JedisPoolBuilder().setUrl("direct://localhost:6312?poolName=op2_app2&poolSize=3")
                .buildPool();

        replicaInfo4App2 = new ReplicaInfo(APP_2, Arrays.asList(masterPool3, masterPool4), Arrays.asList(
                operationPool3, operationPool4));
    }

    private static void startReplicaService() {
        JAXRSServerFactoryBean sf = new JAXRSServerFactoryBean();
        sf.setServiceBean(new ReplicaServiceDelegate(replicaService));
        List<Object> providers = new ArrayList<Object>();
        providers.add(new JacksonJaxbJsonProvider());
        sf.setProviders(providers);
        List<AbstractFeature> features = new ArrayList<AbstractFeature>();
        features.add(new LoggingFeature());
        sf.setFeatures(features);
        sf.setAddress(remoteReplicaServiceUrl);
        sf.create();
    }

    @After
    public void tearDown() {
        Mockito.reset(replicaService);
        client1.flushAll();
        client2.flushAll();
        client3.flushAll();
        client4.flushAll();
    }

    @AfterClass
    public static void afterClass() {
        masterPool1.destroy();
        masterPool2.destroy();
        masterPool3.destroy();
        masterPool4.destroy();
        operationPool1.destroy();
        operationPool2.destroy();
        operationPool3.destroy();
        operationPool4.destroy();
        client1.close();
        client2.close();
        sentinel.stop();
        redis3.stop();
        redis4.stop();
        redis1.stop();
        redis2.stop();
    }

    private ReplicaService gerReplicaServiceClient(String replicaServiceBaseUrl) {
        List<Object> providers = new ArrayList<Object>();
        providers.add(new JacksonJaxbJsonProvider());
        return JAXRSClientFactory.create(replicaServiceBaseUrl, ReplicaService.class, providers);
    }

    @Test
    public void testStartStop() throws Exception {
        replicaManager = new ReplicaManager(Arrays.asList(replicaInfo4App1, replicaInfo4App2),
                gerReplicaServiceClient(remoteReplicaServiceUrl), null);
        //prepare
        String oplog1 = generateOpLog("key1_app2", "value1_app2");
        client1.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog1);

        String oplog2 = generateOpLog("key2_app2", "value2_app2");
        client2.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog2);

        String oplog3 = generateOpLog("key1_app1", "value1_app1");
        client3.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog3);

        String oplog4 = generateOpLog("key2_app1", "value2_app1");
        client4.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog4);

        //log flow
        //client1 <=> client3
        //client2 <=> client4

        try {
            replicaManager.start();

            //verify
            validateValue(client1, "key1_app1", "value1_app1");
            validateValue(client2, "key2_app1", "value2_app1");
            validateValue(client3, "key1_app2", "value1_app2");
            validateValue(client4, "key2_app2", "value2_app2");
        } finally {
            replicaManager.stop();
        }
    }

    @Test
    public void testNotMaster() throws Exception {
        Mockito.when(clusterRuntime.isMaster()).thenReturn(false);

        replicaManager = new ReplicaManager(Arrays.asList(replicaInfo4App1, replicaInfo4App2),
                gerReplicaServiceClient(remoteReplicaServiceUrl), null);
        //prepare
        String oplog1 = generateOpLog("key1_app2", "value1_app2");
        client1.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog1);

        String oplog2 = generateOpLog("key2_app2", "value2_app2");
        client2.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog2);

        String oplog3 = generateOpLog("key1_app1", "value1_app1");
        client3.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog3);

        String oplog4 = generateOpLog("key2_app1", "value2_app1");
        client4.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog4);

        //log flow
        //client1 <=> client3
        //client2 <=> client4

        try {
            replicaManager.start();

            //verify
            Thread.sleep(1000);
            Assert.assertEquals(Long.valueOf(1), client1.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE));
            Assert.assertEquals(Long.valueOf(1), client2.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE));
            Assert.assertEquals(Long.valueOf(1), client3.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE));
            Assert.assertEquals(Long.valueOf(1), client4.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE));
        } finally {
            replicaManager.stop();
        }
    }

    //skip this test because it's a kryo implement
    @Ignore
    @Test
    public void testStartStopKryo() throws Exception {
        replicaManager = new ReplicaManager(Arrays.asList(replicaInfo4App1, replicaInfo4App2),
                gerReplicaServiceClient(remoteReplicaServiceUrl), null);
        //prepare
        RedisOp oplog1 = RedisOpTestUtil.generateOpLog("key1_app2", "value1_app2");
        insertOPLog(oplog1, client1);

        RedisOp oplog2 = RedisOpTestUtil.generateOpLog("key2_app2", "value2_app2");
        insertOPLog(oplog2, client2);

        RedisOp oplog3 = RedisOpTestUtil.generateOpLog("key1_app1", "value1_app1");
        insertOPLog(oplog3, client3);

        RedisOp oplog4 = RedisOpTestUtil.generateOpLog("key2_app1", "value2_app1");
        insertOPLog(oplog4, client4);

        //log flow
        //client1 <=> client3
        //client2 <=> client4

        try {
            replicaManager.start();

            //verify
            validateValue(client1, "key1_app1", "value1_app1");
            validateValue(client2, "key2_app1", "value2_app1");
            validateValue(client3, "key1_app2", "value1_app2");
            validateValue(client4, "key2_app2", "value2_app2");
        } finally {
            replicaManager.stop();
        }
    }

    private void insertOPLog(RedisOp opLogObj, Jedis opLogRedisClient) {
        Kryo kryo = new Kryo();
        kryo.register(RedisOp.class, 0);
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeObject(output, opLogObj);
        Pipeline pipelined = opLogRedisClient.pipelined();

        pipelined.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE.getBytes(), output.toBytes());
        pipelined.sync();
        output.close();
    }

    private void validateValue(Jedis client, String key, String value) throws InterruptedException {
        String remoteValue = null;
        for (int i = 0; i < 80; i++) {
            remoteValue = client.get(key);
            if (remoteValue == null) {
                Thread.sleep(100);
            } else {
                Assert.assertEquals("replay fail", value, remoteValue);
                break;
            }
        }
        Assert.assertNotNull("no value found for key: " + key, remoteValue);
        Assert.assertEquals("replay fail", value, remoteValue);
    }

    @Test
    public void testPauseResume() throws Exception {
        try {
            replicaManager = new ReplicaManager(Arrays.asList(replicaInfo4App1, replicaInfo4App2),
                    gerReplicaServiceClient(remoteReplicaServiceUrl), null);
            replicaManager.start();
            replicaManager.stop(APP_1);
            //APP1 pause replica , but APP2 should be continue
            String oplog3 = generateOpLog("key1_app1", "value1_app1");
            client3.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog3);

            String oplog1 = generateOpLog("key1_app2", "value1_app2");
            client1.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, oplog1);
            validateValue(client3, "key1_app2", "value1_app2");
            Assert.assertEquals("APP1 pause fail", Long.valueOf(1),
                    client3.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE));

            //APP1 resume replica
            replicaManager.start(APP_1);
            validateValue(client1, "key1_app1", "value1_app1");
        } finally {
            replicaManager.stop();
        }
    }

    @Test
    public void testIsAllSyncedUp() throws InterruptedException {
        final List<JedisPool> dataPools = new ArrayList<JedisPool>();
        final JedisSentinelPool mockDataPool = Mockito.mock(JedisSentinelPool.class);
        dataPools.add(mockDataPool);
        final List<JedisPool> queuePools = new ArrayList<JedisPool>();
        final JedisPool mockOpQueuePool = Mockito.mock(JedisPool.class);
        queuePools.add(mockOpQueuePool);

        final Jedis mockQueueJedis = Mockito.mock(Jedis.class);
        Mockito.when(mockOpQueuePool.getResource()).thenReturn(mockQueueJedis);

        Mockito.when(mockQueueJedis.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE)).thenReturn(
                JedisReplicationConstants.DW_QUEUE_MAX_LEN + 1);
        final Pipeline pipeline = new Pipeline();
        pipeline.setClient(Mockito.mock(Client.class));
        Mockito.when(mockQueueJedis.pipelined()).thenReturn(pipeline);

        ReplicaInfo replicaInfo = new ReplicaInfo(APP_1, dataPools, queuePools);
        final ReplicaManager replicaMgr = new ReplicaManager(Arrays.asList(replicaInfo),
                gerReplicaServiceClient(remoteReplicaServiceUrl), null);
        replicaMgr.start();

        Thread.sleep(100);

        try {
            boolean result = replicaMgr.isAllSyncedUp(APP_1);
            Assert.assertFalse("one channel not synced up", result);
        } finally {
            replicaMgr.stop();
        }
    }

    @Test
    public void testStartStopSyncUp() throws Exception {
        replicaManager = new ReplicaManager(Arrays.asList(replicaInfo4App1, replicaInfo4App2),
                gerReplicaServiceClient(remoteReplicaServiceUrl), null);
        client4.set("key1", "value1");
        replicaManager.startSyncUp(APP_1, 0);
        //wait 500ms to make sentinel take action
        Thread.sleep(500);
        List<String> address = sentinelClient.sentinelGetMasterAddrByName("master1_app1");
        Assert.assertNull("remove sentinel monitor fail", address);
        replicaManager.stopSyncUp(APP_1, 0);
        //wait 500ms to make sentinel take action
        Thread.sleep(500);
        address = sentinelClient.sentinelGetMasterAddrByName("master1_app1");
        Assert.assertEquals("sentinel did not monitor master1", "6311", address.get(1));

    }

    @Test
    public void testGetExteralAddr() {
        replicaManager = new ReplicaManager(Arrays.asList(replicaInfo4App1, replicaInfo4App2),
                gerReplicaServiceClient(remoteReplicaServiceUrl), null);
        //test different database
        Assert.assertEquals("get exteral addr fail", REDIS_CONINFO_1_1, replicaManager.getConnectInfo(APP_1, 0));
        Assert.assertEquals("get exteral addr fail", REDIS_CONINFO_2, replicaManager.getConnectInfo(APP_1, 1));

        Assert.assertEquals("get exteral addr fail", REDIS_CONINFO_3, replicaManager.getConnectInfo(APP_2, 0));
        Assert.assertEquals("get exteral addr fail", REDIS_CONINFO_4, replicaManager.getConnectInfo(APP_2, 1));
    }

    @Test
    public void testResyncUp() throws Exception {
        //operation pool is the same with master pool, because operation pool is unless in this case
        ReplicaInfo replicaInfo = new ReplicaInfo(APP_1, Arrays.asList(masterPool1, masterPool2), Arrays.asList(
                masterPool1, masterPool2));
        Mockito.when(replicaService.getConnectInfo(APP_1, 0)).thenReturn(REDIS_CONINFO_3);
        Mockito.when(replicaService.getConnectInfo(APP_1, 1)).thenReturn(REDIS_CONINFO_4);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                client3.slaveofNoOne();
                sentinelClient.sentinelMonitor("master1_app2", LOCAL_HOST, 6411, 1);
                Mockito.when(replicaService.isSyncedUp(APP_1, 0)).thenReturn(true);
                return null;
            }
        }).when(replicaService).stopSyncUp(APP_1, 0);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                client4.slaveofNoOne();
                sentinelClient.sentinelMonitor("master2_app2", LOCAL_HOST, 6412, 1);
                Mockito.when(replicaService.isSyncedUp(APP_1, 1)).thenReturn(true);
                return null;
            }
        }).when(replicaService).stopSyncUp(APP_1, 1);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                sentinelClient.sentinelRemove("master1_app2");
                Mockito.when(replicaService.isSyncedUp(APP_1, 0)).thenReturn(false);
                return null;
            }
        }).when(replicaService).startSyncUp(APP_1, 0);
        Mockito.doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Mockito.when(replicaService.isSyncedUp(APP_1, 1)).thenReturn(false);
                sentinelClient.sentinelRemove("master2_app2");
                return null;
            }
        }).when(replicaService).startSyncUp(APP_1, 1);
        replicaManager = new ReplicaManager(Arrays.asList(replicaInfo, replicaInfo4App2),
                gerReplicaServiceClient(remoteReplicaServiceUrl), null);
        replicaManager.start();

        client1.set("key1", "value1");
        client2.set("key2", "value2");

        try {
            replicaManager.reSyncUp(APP_1);
            waitSyncupFinish(APP_1);
            //verify
            Assert.assertEquals("synup fail", "value1", client3.get("key1"));
            Assert.assertEquals("synup fail", "value2", client4.get("key2"));
            //sentinel back to normal

            Assert.assertEquals("6311", sentinelClient.sentinelGetMasterAddrByName("master1_app1").get(1));
            Assert.assertEquals("6312", sentinelClient.sentinelGetMasterAddrByName("master2_app1").get(1));
            Assert.assertEquals("6411", sentinelClient.sentinelGetMasterAddrByName("master1_app2").get(1));
            Assert.assertEquals("6412", sentinelClient.sentinelGetMasterAddrByName("master2_app2").get(1));
        } finally {
            replicaManager.stop();
        }
    }

    private void waitSyncupFinish(String appName) {
        for (int i = 0; i < 50; i++) {
            if (!replicaManager.isAllSyncedUp(appName)) {
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

    protected String generateOpLog(String key, String value) throws Exception {
        List<Object> arguments = new ArrayList<Object>();
        arguments.add(key);
        arguments.add(value);
        List<String> parameterTypes = new ArrayList<String>();
        parameterTypes.add("java.lang.String");
        parameterTypes.add("java.lang.String");
        RedisOp opLog = new RedisOp(defaultShardKey, defaultCmd, arguments, parameterTypes);
        return mapper.writeValueAsString(opLog);
    }
}
