/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.service;

import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.redis.pool.JedisPool;

public class MasterElectorServiceTest {

    MasterElectorService masterElector;

    @Mock
    JedisPool mockJedisPool;

    @Mock
    Jedis jedis;

    String key = "masterKey";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(mockJedisPool.getResource()).thenReturn(jedis);
        masterElector = new MasterElectorService(mockJedisPool, key);
    }

    @After
    public void tearDown() throws Exception {
        masterElector = null;
    }

    @Test
    public void testInit() {
        assertTrue(masterElector.init());
    }

    @Test
    public void testIsMasterGetValid() {
        String hostId = "host";
        masterElector.setHostId(hostId);

        when(jedis.get(key)).thenReturn(hostId);
        masterElector.run();

        assertTrue(masterElector.isMaster());
        Mockito.verify(jedis).expire(eq(key), anyInt());
        Mockito.verify(mockJedisPool).returnResource(jedis);
    }

    @Test
    public void testIsMasterGetNotValid() {
        String hostId = "host";
        masterElector.setHostId(hostId);
        when(jedis.get(key)).thenReturn(hostId + "not me");

        masterElector.run();

        assertFalse(masterElector.isMaster());
        Mockito.verify(mockJedisPool).returnResource(jedis);
    }

    @Test
    public void testIsMasterSetNxValid() {
        String hostId = "host";
        masterElector.setHostId(hostId);
        when(jedis.get(key)).thenReturn(null);
        //when(jedis.setnx(anyString(), anyString())).thenReturn(1L);
        when(jedis.set(eq(key), eq(hostId), eq("NX"), eq("EX"), anyInt())).thenReturn("OK");

        masterElector.run();

        assertTrue(masterElector.isMaster());
        Mockito.verify(mockJedisPool).returnResource(jedis);
    }

    @Test
    public void testIsMasterSetNxNotValid() {
        String hostId = "host";
        masterElector.setHostId(hostId);
        when(jedis.get(key)).thenReturn(null);
        //when(jedis.setnx(anyString(), anyString())).thenReturn(0L);
        when(jedis.set(eq(key), eq(hostId), eq("NX"), eq("EX"), anyInt())).thenReturn(null);

        masterElector.run();

        assertFalse(masterElector.isMaster());
        Mockito.verify(mockJedisPool).returnResource(jedis);
    }

}
