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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyDouble;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.google.common.collect.Sets;

public class ClusterInfoServiceTest {

    private static final String MEMBER = "traffic-1";

    ClusterInfoService clusterInfoService;

    @Mock
    JedisPool mockJedisPool;

    @Mock
    Jedis jedis;

    String key = "clusterKey";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        when(mockJedisPool.getResource()).thenReturn(jedis);

    }

    @After
    public void tearDown() throws Exception {
        clusterInfoService = null;
    }


    @Test
    public void testGetCurrentMenber() {
        clusterInfoService = new ClusterInfoService(mockJedisPool, key, MEMBER);
        clusterInfoService.setTimeOutMilliSeconds(5);
        assertEquals(MEMBER, clusterInfoService.getCurrentMember());
    }

    @Test
    public void testGetCurrentMenberMemberParamNull() {
        clusterInfoService = new ClusterInfoService(mockJedisPool, key, null);
        clusterInfoService.setTimeOutMilliSeconds(5);
        assertNotNull(clusterInfoService.getCurrentMember());
    }

    @Test
    public void testGetCurrentMenberMemberParamEmpty() {
        clusterInfoService = new ClusterInfoService(mockJedisPool, key, "");
        clusterInfoService.setTimeOutMilliSeconds(5);
        assertNotNull(clusterInfoService.getCurrentMember());
        assertTrue(clusterInfoService.getCurrentMember().length() > 0);
    }

    @Test
    public void testGetCurrentMenberByGenerateMember() {
        clusterInfoService = new ClusterInfoService(mockJedisPool, key);
        clusterInfoService.setTimeOutMilliSeconds(5);
        assertNotNull(clusterInfoService.getCurrentMember());
    }

    @Test
    public void testGetMembers() {
        clusterInfoService = new ClusterInfoService(mockJedisPool, key, MEMBER);
        clusterInfoService.setTimeOutMilliSeconds(5);
        String traffic2 = "traffic-2";
        when(jedis.zrangeByScore(eq(key), anyDouble(), anyDouble())).thenReturn(Sets.newHashSet(traffic2, MEMBER));
        List<String> members = clusterInfoService.getMembers();
        assertEquals(MEMBER, members.get(0));
        assertEquals(traffic2, members.get(1));
        Mockito.verify(jedis).zrangeByScore(eq(key), anyDouble(), anyDouble());
        Mockito.verify(mockJedisPool).returnResource(jedis);
    }
}
