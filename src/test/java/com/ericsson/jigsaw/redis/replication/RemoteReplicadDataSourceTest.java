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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.replication.impl.RemoteReplicaDataSourceImpl;

public class RemoteReplicadDataSourceTest {

    private static RedisServer redisData1;
    private static RedisServer redisData2;

    //jedis
    private static Jedis redisClient1;
    private static Jedis redisClient2;
    private static Jedis redisClient3;

    private static final int REDIS_PORT_1 = 3162;
    private static final int REDIS_PORT_2 = 3163;
    private static final String REDIS_CONINFO_1 = "127.0.0.1:3162,0";
    private static final String REDIS_CONINFO_2 = "127.0.0.1:3163,0";
    private static final String REDIS_CONINFO_3 = "127.0.0.1:3162,1";

    private static final HostAndPort REDIS_ADDR_1 = new HostAndPort("127.0.0.1", REDIS_PORT_1);
    private static final String defaultShardKey = "shard1";
    private static final String defaultCmd = "set";
    private static final String APP = "testApp";
    private static final String password = "123456";

    @Mock
    private ReplicaService replicaServiceMock;

    private RemoteReplicaDataSourceImpl remoteReplicaDataSource;

    @BeforeClass
    public static void beforeClass() throws Exception {
        redisData1 = new RedisServer(REDIS_PORT_1);
        redisData1.start();
        redisData2 = new RedisServer(REDIS_PORT_2);
        redisData2.start();

        redisClient1 = new Jedis("127.0.0.1", REDIS_PORT_1);
        redisClient2 = new Jedis("127.0.0.1", REDIS_PORT_2);
        //same redis instance ,database index 1
        redisClient3 = new Jedis("127.0.0.1", REDIS_PORT_1);

    }

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        remoteReplicaDataSource = new RemoteReplicaDataSourceImpl(APP, replicaServiceMock, 0, null);
    }

    @After
    public void tearDown() {
        redisClient1.flushAll();
        redisClient2.flushAll();
    }

    @AfterClass
    public static void afterClass() {
        ReplicaContext.resetPassword();
        redisClient1.close();
        redisClient2.close();
        redisClient3.close();
        redisData2.stop();
        redisData1.stop();
    }

    @Test
    public void testSyncupWithPassword() {
        redisClient1.set("key", "value");
        redisClient1.configSet("requirepass", password);
        redisClient1.auth(password);
        redisClient2.configSet("requirepass", password);
        redisClient2.auth(password);
        Mockito.when(replicaServiceMock.getConnectInfo(APP, 0)).thenReturn(REDIS_CONINFO_2);
        try {
            ReplicaContext.setRedisPassword(password);
            remoteReplicaDataSource.syncUpWith(REDIS_ADDR_1);
            waitSyncUpFinish();
            Assert.assertEquals("synup with password fail", "value", redisClient2.get("key"));
        } finally {
            ReplicaContext.resetPassword();
            redisClient1.configSet("requirepass", "");
            redisClient2.configSet("requirepass", "");
            redisClient2.slaveofNoOne();
        }

    }

    private void waitSyncUpFinish() {
        boolean finish = false;
        for (int i = 0; i < 20; i++) {
            if (!remoteReplicaDataSource.isSyncedUp(REDIS_ADDR_1)) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    // do nothing
                }
            } else {
                finish = true;
                break;
            }
        }
        Assert.assertTrue("wait sync up finish too long", finish);
    }

    @Test
    public void testReplaySuccessfully() throws Exception {
        Mockito.when(replicaServiceMock.getConnectInfo(APP, 0)).thenReturn(REDIS_CONINFO_1);
        RedisOp op = generateOpLog("key", "value");
        List<RedisOp> opLogs = Arrays.asList(op);
        remoteReplicaDataSource.replay(opLogs);
        //verify
        Assert.assertEquals("repay fail", "value", redisClient1.get("key"));
        //stop redis
        redisData1.stop();

        try {
            remoteReplicaDataSource.replay(opLogs);
            fail("shoud throw jedisConnectionException");
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof JedisConnectionException);
            Mockito.when(replicaServiceMock.getConnectInfo(APP, 0)).thenReturn(REDIS_CONINFO_2);
            //replay again
            remoteReplicaDataSource.replay(opLogs);
            Assert.assertEquals("repay fail", "value", redisClient2.get("key"));
        } finally {
            redisData1.start();
            redisClient1 = new Jedis("127.0.0.1", REDIS_PORT_1);
        }
    }

    @Test
    public void testReplayWithDatabase() throws Exception {
        redisClient3.select(1);
        Mockito.when(replicaServiceMock.getConnectInfo(APP, 0)).thenReturn(REDIS_CONINFO_3);
        RedisOp op = generateOpLog("key", "value");
        List<RedisOp> opLogs = Arrays.asList(op);
        remoteReplicaDataSource.replay(opLogs);
        //verify
        Assert.assertEquals("repay fail", "value", redisClient3.get("key"));
    }

    @Test
    public void testReplayWithPassword() throws Exception {
        redisClient1.configSet("requirepass", "123456");
        Mockito.when(replicaServiceMock.getConnectInfo(APP, 0)).thenReturn(REDIS_CONINFO_1);
        ReplicaContext.setRedisPassword("123456");
        RedisOp op = generateOpLog("key", "value");
        List<RedisOp> opLogs = Arrays.asList(op);
        try {
            redisClient1.auth("123456");
            remoteReplicaDataSource.replay(opLogs);
        } finally {
            //verify
            ReplicaContext.resetPassword();
            Assert.assertEquals("repay fail", "value", redisClient1.get("key"));
            redisClient1.configSet("requirepass", "");
        }
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

}
