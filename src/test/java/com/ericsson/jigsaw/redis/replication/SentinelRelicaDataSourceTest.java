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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.embedded.redis.RedisSentinel;
import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.pool.JedisSentinelPool;
import com.ericsson.jigsaw.redis.replication.impl.SentinelReplicaDataSourceImpl;

public class SentinelRelicaDataSourceTest {

    private static RedisSentinel backupRedisSentinel;

    private static RedisServer originRedis;
    private static RedisServer backupRedis;

    private static Jedis originJedis;
    private static Jedis backupJedis;
    private static Jedis sentinelJedis;

    private static ReplicaDataSource replicaDataSource;

    private static JedisPool backupSentinelPool;

    private static HostAndPort originRedisAddr;
    private static HostAndPort backupRedisAddr;

    private static final String MASTER_NAME = "mymaster";

    @BeforeClass
    public static void beforeClass() throws IOException {
        originRedis = new RedisServer(37379);
        backupRedis = new RedisServer(37380);
        originRedis.start();
        backupRedis.start();
        backupRedisSentinel = RedisSentinel.builder().port(47380).masterName(MASTER_NAME).masterPort(37380).build();
        backupRedisSentinel.start();
        originRedisAddr = new HostAndPort("127.0.0.1", 37379);
        backupRedisAddr = new HostAndPort("127.0.0.1", 37380);

        backupSentinelPool = new JedisPoolBuilder().setUrl(
                "sentinel://localhost:47380?masterNames=mymaster&poolSize=3&poolName=sentinelTest").buildPool();

        originJedis = new Jedis("127.0.0.1", 37379);
        backupJedis = new Jedis("127.0.0.1", 37380);
        sentinelJedis = new Jedis("127.0.0.1", 47380);

        replicaDataSource = new SentinelReplicaDataSourceImpl((JedisSentinelPool) backupSentinelPool, null);
    }

    @After
    public void tearDown() {
        backupJedis.slaveofNoOne();
        originJedis.flushAll();
        backupJedis.flushAll();
    }

    @AfterClass
    public static void afterClass() {
        backupSentinelPool.destroy();
        originJedis.close();
        backupJedis.close();
        sentinelJedis.close();
        originRedis.stop();
        backupRedis.stop();
        backupRedisSentinel.stop();
    }

    @Test
    public void testStartStopSyncup() {
        replicaDataSource.startSyncUp();
        List<String> masterAddr = sentinelJedis.sentinelGetMasterAddrByName(MASTER_NAME);
        assertNull("master should be removed", masterAddr);
        assertFalse("should be syncing up", replicaDataSource.isSyncedUp());
        //call again
        //replicaDataSource.startSyncUp();
        replicaDataSource.stopSyncUp();
        masterAddr = sentinelJedis.sentinelGetMasterAddrByName(MASTER_NAME);
        assertEquals("master not correct", "37380", masterAddr.get(1));
        assertTrue("should be synced up", replicaDataSource.isSyncedUp());
    }

    @Test
    public void testStartStopSyncUpWith() throws InterruptedException {
        originJedis.set("key", "origin");
        replicaDataSource.startSyncUp();
        replicaDataSource.syncUpWith(originRedisAddr);
        waitSyncedUp();
        List<String> masterAddr = sentinelJedis.sentinelGetMasterAddrByName(MASTER_NAME);
        assertNull("master should be removed", masterAddr);
        assertEquals("syncup fail", "origin", backupJedis.get("key"));

        replicaDataSource.stopSyncUp();
        masterAddr = sentinelJedis.sentinelGetMasterAddrByName(MASTER_NAME);
        assertEquals("master not correct", "37380", masterAddr.get(1));

        originJedis.set("key2", "origin2");
        assertNull("backup redis should not receive key2", backupJedis.get("key2"));
    }

    private void waitSyncedUp() throws InterruptedException {
        while (!replicaDataSource.isSyncedUp(originRedisAddr)) {
            Thread.sleep(100);
        }
    }
}
