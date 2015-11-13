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
import static org.junit.Assert.assertNull;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.embedded.redis.RedisServer;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.pool.JedisPoolBuilder;
import com.ericsson.jigsaw.redis.replication.impl.LocalReplicaDataSourceImpl;

public class ReplicaDataSourceTest {

    private static RedisServer originRedis;
    private static RedisServer backupRedis;

    private static Jedis originJedis;
    private static Jedis backupJedis;

    private static ReplicaDataSource replicaDataSource;

    private static JedisPool backupJedisPool;

    private static HostAndPort originRedisAddr;
    private static HostAndPort backupRedisAddr;

    @BeforeClass
    public static void beforeClass() throws IOException {
        originRedis = new RedisServer(31379);
        backupRedis = new RedisServer(31380);
        originRedis.start();
        backupRedis.start();

        originRedisAddr = new HostAndPort("127.0.0.1", 31379);
        backupRedisAddr = new HostAndPort("127.0.0.1", 31380);

        backupJedisPool = new JedisPoolBuilder().setUrl("direct://localhost:31380?poolSize=3&poolName=backup")
                .buildPool();

        originJedis = new Jedis("127.0.0.1", 31379);
        backupJedis = new Jedis("127.0.0.1", 31380);

        replicaDataSource = new LocalReplicaDataSourceImpl(backupJedisPool, null);
    }

    @After
    public void tearDown() {
        originJedis.slaveofNoOne();
        backupJedis.slaveofNoOne();
        originJedis.flushAll();
        backupJedis.flushAll();
    }

    @AfterClass
    public static void afterClass() {
        backupJedisPool.destroy();
        originJedis.close();
        backupJedis.close();
        originRedis.stop();
        backupRedis.stop();
    }

    @Test
    public void testStartStopSyncUp() throws InterruptedException {
        //prepare
        originJedis.set("key1", "origin");
        backupJedis.set("key1", "backup");
        //verify
        replicaDataSource.syncUpWith(originRedisAddr);
        waitSyncedUp();
        assertEquals("syncup fail", "origin", backupJedis.get("key1"));
        replicaDataSource.stopSyncUp();
        originJedis.set("key2", "origin2");
        assertNull("backup redis should not receive key2", backupJedis.get("key2"));

        //syncup again
        replicaDataSource.syncUpWith(originRedisAddr);
        waitSyncedUp();
        assertEquals("syncup fail", "origin2", backupJedis.get("key2"));
    }

    private void waitSyncedUp() throws InterruptedException {
        while (!replicaDataSource.isSyncedUp(originRedisAddr)) {
            Thread.sleep(100);
        }
    }
}
