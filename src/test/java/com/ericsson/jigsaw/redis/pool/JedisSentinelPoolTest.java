/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.pool;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisSentinelPoolTest {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNonSentinelRun() {

        HostAndPort[] sentinelInfos = new HostAndPort[2];
        sentinelInfos[0] = new HostAndPort("localhost", 6079);
        sentinelInfos[1] = new HostAndPort("localhost", 6179);

        JedisPoolConfig config = new JedisPoolConfig();
        JedisSentinelPool sentinelPool = new JedisSentinelPool(sentinelInfos, "default", new ConnectionInfo("testsen"), config);

        try {
            sentinelPool.getResource();
            fail("should not be here.");
        } catch (JedisConnectionException e) {
            ;
        }

        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        assertEquals(true, sentinelPool.getMasterSwitchListener().isAlive());
        sentinelPool.destroy();

        assertEquals(false, sentinelPool.getMasterSwitchListener().isAlive());
    }

}
