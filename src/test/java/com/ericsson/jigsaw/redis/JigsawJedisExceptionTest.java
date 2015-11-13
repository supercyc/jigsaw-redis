/*
 * ----------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 * ----------------------------------------------------------------------
 */

package com.ericsson.jigsaw.redis;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import redis.clients.jedis.exceptions.JedisDataException;

public class JigsawJedisExceptionTest {

   @Test
    public void testIsDataException(){
       JigsawJedisException e = new JigsawJedisException(new JedisDataException("data exception"));
       assertTrue(e.isJedisDataException());
       assertFalse(e.isJedisConnectionException());
   }

    @Test
    public void testInternalExeptionIsNull(){
        JigsawJedisException e = new JigsawJedisException("data exception");
        assertNull(e.getInternalJedisException());
        assertFalse(e.isJedisDataException());
        assertFalse(e.isJedisConnectionException());
    }
}
