/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import redis.clients.jedis.Jedis;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RedisOpTestUtil {

    private static final String defaultShardKey = "shard1";
    private static final String defaultCmd = "set";

    private static ObjectMapper mapper = new ObjectMapper();

    public static RedisOp generateOpLog(String key, String value) throws Exception {
        List<Object> arguments = new ArrayList<Object>();
        arguments.add(key);
        arguments.add(value);
        List<String> parameterTypes = new ArrayList<String>();
        parameterTypes.add("java.lang.String");
        parameterTypes.add("java.lang.String");
        return new RedisOp(defaultShardKey, defaultCmd, arguments, parameterTypes);

    }

    public static String generateJsonString(RedisOp oplog) throws JsonGenerationException, JsonMappingException,
            IOException {
        return mapper.writeValueAsString(oplog);
    }

    public static void waitQueueConsume(Jedis client) {
        boolean waitSuccess = false;
        for (int i = 0; i < 20; i++) {
            if (client.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE) > 0) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    //do nothing
                }
            } else {
                waitSuccess = true;
                break;
            }

        }
        assertTrue("wait queue consumed too long", waitSuccess);
    }
}
