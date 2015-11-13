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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;

import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisActionNoResult;
import com.ericsson.jigsaw.redis.pool.JedisPool;

public class JedisTemplateTest {

    private JedisTemplate jedisTemplate;

    @Mock
    private JedisPool jedisPool;

    @Mock
    private Jedis jedis;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        jedisTemplate = new JedisTemplate(jedisPool);
        when(jedisPool.getResource()).thenReturn(jedis);
    }

    @Test
    public void testExecuteReturnString() {
        final String returnAction = "String";
        assertEquals(returnAction, jedisTemplate.execute(new JedisAction<String>() {
            @Override
            public String action(Jedis jedis) {
                // TODO Auto-generated method stub
                return returnAction;
            }
        }));
        verify(jedisPool).returnResource(jedis);
    }

    @Test
    public void testExecuteConnectionException() {
        try {
            jedisTemplate.execute(new JedisAction<String>() {
                @Override
                public String action(Jedis jedis) {
                    // TODO Auto-generated method stub
                    throw new JedisConnectionException("Test_conn_exception");
                }
            });

            fail("Should throw exception");
        } catch (JigsawJedisException e) {
            ;
        }
        verify(jedisPool, times(1)).returnBrokenResource(jedis);
    }

    @Test
    public void testExecuteDataException() {
        try {
            jedisTemplate.execute(new JedisAction<String>() {
                @Override
                public String action(Jedis jedis) {
                    // TODO Auto-generated method stub
                    throw new JedisDataException("Test_conn_exception");
                }
            });

            fail("Should throw exception");
        } catch (JigsawJedisException e) {
            ;
        }
        verify(jedisPool, times(1)).returnResource(jedis);
    }

    @Test
    public void testExecuteReadOnlyException() {
        try {
            jedisTemplate.execute(new JedisAction<String>() {
                @Override
                public String action(Jedis jedis) {
                    // TODO Auto-generated method stub
                    throw new JedisDataException("READONLY");
                }
            });

            fail("Should throw exception");
        } catch (JigsawJedisException e) {
            ;
        }
        verify(jedisPool, times(1)).returnBrokenResource(jedis);
    }

    @Test
    public void testExecuteNoReturnException() {
        when(jedis.isConnected()).thenReturn(true);
        doThrow(new JedisConnectionException("return exception")).when(jedisPool).returnResource(jedis);
        try {
            jedisTemplate.execute(new JedisActionNoResult() {
                @Override
                public void action(Jedis jedis) {
                    // TODO Auto-generated method stub
                    throw new JedisDataException("data");
                }
            });

            fail("Should throw exception");
        } catch (JigsawJedisException e) {

            if (!(e.getCause() instanceof JedisDataException)){
                fail("error exception type.");
            }
            ;
        }

        verify(jedis, times(1)).quit();
        verify(jedis, times(1)).disconnect();
    }

}
