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

import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.ericsson.jigsaw.redis.ct.base.RedisTestEnv;

public class JedisTestBase {

    static RedisTestEnv redisTestEnv;

    @BeforeClass
    public static void initRedisEnv() {
        redisTestEnv = new RedisTestEnv();
        redisTestEnv.init();
    }

    @AfterClass
    public static void stopRedisAll() {
        redisTestEnv.destroy();
    }

    protected void sleep(int milliSec) {
        try {
            Thread.sleep(milliSec);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
