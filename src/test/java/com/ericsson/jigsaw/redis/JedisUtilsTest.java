/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.ericsson.jigsaw.redis.pool.ConnectionInfo;
import com.ericsson.jigsaw.redis.pool.JedisPool;

public class JedisUtilsTest {
  
    @Mock
    Jedis jedis;

    @Mock
    JedisPool jedisPool;
    
    @Mock
    JedisPool jedisPool2;
    
    @Mock
    JedisPool jedisPool3;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(jedisPool.getResource()).thenReturn(jedis);
    }

    @After
    public void tearDown() throws Exception {
    }
    
  

    @Test
    public void testPingOk() {
        when(jedis.ping()).thenReturn("PONG");
        assertEquals(true, JedisUtils.ping(jedisPool));
    }

    @Test
    public void testPingNotOk() {
        when(jedis.ping()).thenThrow(new JedisConnectionException("Test_conn_exception"));
        assertEquals(false, JedisUtils.ping(jedisPool));
    }

    @Test
    public void testIsStatusOk() {
        assertEquals(true, JedisUtils.isStatusOk("+OK"));
    }
}
