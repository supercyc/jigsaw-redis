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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.pool.JedisPool;

public class JedisUtils {
    private static Logger logger = LoggerFactory.getLogger(JedisUtils.class);

    private static final String OK_CODE = "OK";
    private static final String OK_MULTI_CODE = "+OK";

    /**
     * Check status is OK or +OK.
     */
    public static boolean isStatusOk(String status) {
        return (status != null) && (OK_CODE.equals(status) || OK_MULTI_CODE.equals(status));
    }

    /**
     * Destroy jedis object outside the pool.
     */
    public static void destroyJedis(Jedis jedis) {
        if ((jedis != null) && jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                    // ignore
                }
                jedis.disconnect();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Ping the jedis instance, return true is the result is PONG.
     */
    public static boolean ping(JedisPool pool) {
        JedisTemplate template = new JedisTemplate(pool);
        try {
            String result = template.execute(new JedisAction<String>() {
                @Override
                public String action(Jedis jedis) {
                    return jedis.ping();
                }
            });
            return (result != null) && result.equals("PONG");
        } catch (JedisException e) {
            return false;
        }
    }

    /**
     * Return jedis connection to the pool, call different return
     * methods depends on the conectionBroken status.
     */
    public static void closeResource(JedisPool jedisPool, Jedis jedis, boolean conectionBroken) {
        try {
            if (conectionBroken) {
                jedisPool.returnBrokenResource(jedis);
            } else {
                jedisPool.returnResource(jedis);
            }
        } catch (Exception e) {
            logger.error(jedisPool.getFormattedPoolName() + "return back jedis failed, will fore close the jedis.", e);
            destroyJedis(jedis);
        }

    }
}
