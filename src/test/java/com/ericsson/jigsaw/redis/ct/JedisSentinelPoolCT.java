/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.ct;

import static org.junit.Assert.*;

import org.junit.Test;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.JedisUtils;
import com.ericsson.jigsaw.redis.ct.base.CommandUtil;
import com.ericsson.jigsaw.redis.ct.base.RedisSUT;
import com.ericsson.jigsaw.redis.pool.ConnectionInfo;
import com.ericsson.jigsaw.redis.pool.JedisSentinelPool;

public class JedisSentinelPoolCT extends JedisTestBase {

    private String testkey = "test:key";
    private String masterName = "default";
    private JedisSentinelPool setinelPool;

    private RedisSUT redis_1;
    private RedisSUT redis_2;
    private int redisPort_1;
    private int redisPort_2;

    @Test
    public void testMasterSlaveFailOver() {
        if (!CommandUtil.isLinuxPlatform()) {
            return;
        }

        RedisSUT[] suts = redisTestEnv.getRedisServers();
        redis_1 = suts[0];
        redis_2 = suts[1];
        redisPort_1 = redisTestEnv.getRedisPortBase();
        redisPort_2 = redisPort_1 + 1;

        //Step1: create sentinel pool.
        setinelPool = createSentinelPool();
        redis_2.slavof(redis_1);
        System.out.println("=================");
        System.out.println("Step1:create sentinel pools passed");
        System.out.println("=================");
        sleep(10000);

        //Step2: verify sentinel pool, master connection.
        //2 is slaveof 1
        assertTrue(JedisUtils.ping(setinelPool));
        HostAndPort masterServerInfo = setinelPool.getMasterSwitchListener().getCurrentMasterAddress();
        System.out.println("Master server: " + masterServerInfo.toString());
        assertEquals(redisPort_1, masterServerInfo.getPort());
        long initValue = incrTestValue();
        System.out.println("=================");
        System.out.println("Step2:verify sentinel pools passed");
        System.out.println("=================");

        //Step3-1: fail-over
        redis_2.slavof(null);
        redis_1.slavof(redis_2);
        sleep(20000);

        //step3-2: verify fail-over succeed.
        //1 is slaveof 2
        assertTrue(JedisUtils.ping(setinelPool));
        long secondValue = incrTestValue();
        assertEquals(secondValue, initValue + 1);
        masterServerInfo = setinelPool.getMasterSwitchListener().getCurrentMasterAddress();
        System.out.println("Master server: " + masterServerInfo.toString());
        assertEquals(redisPort_2, masterServerInfo.getPort());
        System.out.println("=================");
        System.out.println("Step3:verify fail-over passed");
        System.out.println("=================");

        setinelPool.destroy();
        System.out.println("=================");
        System.out.println("Step4:destroy sentinel pool passed");
        System.out.println("=================");
    }

    protected JedisSentinelPool createSentinelPool() {
        HostAndPort[] sentinelInfos = new HostAndPort[1];
        sentinelInfos[0] = new HostAndPort("localhost", redisTestEnv.getRedisSentinelPortBase());

        JedisPoolConfig config = new JedisPoolConfig();
        return new JedisSentinelPool(sentinelInfos, masterName, config);
    }

    protected long incrTestValue() {
        JedisTemplate template = new JedisTemplate(setinelPool);
        try {
            Long result = template.execute(new JedisAction<Long>() {
                @Override
                public Long action(Jedis jedis) {
                    return jedis.incr(testkey);
                }
            });
            return result;
        } catch (JedisException e) {
            e.printStackTrace();
            return 0;
        }
    }
}
