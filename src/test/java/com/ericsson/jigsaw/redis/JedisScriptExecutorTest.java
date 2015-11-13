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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import com.ericsson.jigsaw.redis.pool.JedisPool;

public class JedisScriptExecutorTest {

    JedisTemplate jedisTemplate;

    @Mock
    private Jedis jedis;

    @Mock
    JedisPool mockJedisPool;

    private List<String> keys1;

    private List<String> keys2;

    private List<String> args1;

    private List<String> args2;

    private JedisScriptExecutor scriptExecutor;

    private String script1 = "script1";
    private String sha1 = "sha1";
    private String script2 = "script2";
    private String sha2 = "sha2";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        keys1 = new ArrayList<String>();
        keys2 = new ArrayList<String>();
        args1 = new ArrayList<String>();
        args2 = new ArrayList<String>();

        scriptExecutor = new JedisScriptExecutor(mockJedisPool);
        jedisTemplate = new JedisTemplate(mockJedisPool);
        when(mockJedisPool.getResource()).thenReturn(jedis);
    }

    @After
    public void tearDown() throws Exception {
        scriptExecutor.getLuaMaps().remove(sha1);
        scriptExecutor.getLuaMaps().remove(sha2);
    }

    @Test
    public void testExecute() {
        Mockito.when(jedis.scriptLoad(script1)).thenReturn(sha1);
        Mockito.when(jedis.scriptLoad(script2)).thenReturn(sha2);
        scriptExecutor.load(script1);
        scriptExecutor.load(script2);

        scriptExecutor.execute(sha1, keys1, args1);
        Mockito.verify(jedis).evalsha(sha1, keys1, args1);

        scriptExecutor.execute(sha2, keys2, args2);
        Mockito.verify(jedis).evalsha(sha2, keys2, args2);

    }

    @Test
    public void testExecuteError() {
        Mockito.when(jedis.scriptLoad(script1)).thenReturn(sha1);
        Mockito.when(jedis.scriptLoad(script2)).thenReturn(sha2);
        scriptExecutor.load(script1);
        scriptExecutor.load(script2);

        Mockito.reset(jedis);

        Mockito.when(jedis.evalsha(sha1, keys1, args1)).thenThrow(new JedisDataException("test")).thenReturn(true);
        scriptExecutor.execute(sha1, keys1, args1);
        Mockito.verify(jedis, times(2)).evalsha(sha1, keys1, args1);
    }

    @Test
    public void testAdd() {
        Mockito.when(jedis.scriptLoad(script1)).thenReturn(sha1);
        Mockito.when(jedis.scriptLoad(script2)).thenReturn(sha2);

        scriptExecutor.load(script1);
        scriptExecutor.load(script2);

        Map<String, String> luaMaps = scriptExecutor.getLuaMaps();
        assertEquals(2, luaMaps.size());
        String script = luaMaps.get(sha1);
        assertEquals(script, script1);
        script = luaMaps.get(sha2);
        assertEquals(script, script2);

    }
}