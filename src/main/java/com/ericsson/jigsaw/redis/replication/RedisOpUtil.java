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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RedisOpUtil {

    private static ObjectMapper mapper = new ObjectMapper();
    private static Logger logger = LoggerFactory.getLogger(RedisOpUtil.class);

    public static String buildOperationLogString(Object shardKey, String command, Object[] args,
            List<String> parameterTypes) throws JsonGenerationException, JsonMappingException, IOException {

        RedisOp operationLog = buildRedisOperationObj(shardKey, command, args, parameterTypes);

        return mapper.writeValueAsString(operationLog);
    }

    protected static RedisOp buildRedisOperationObj(Object shardKey, String command, Object[] args,
            List<String> parameterTypes) {
        logger.debug("Build operation log object. shardKey:" + shardKey + " command:" + command + " arguments:"
                + Arrays.toString(args) + " parameterTypes: " + Arrays.toString(parameterTypes.toArray()));

        List<Object> argList = new ArrayList<Object>();
        for (Object arg : args) {
            argList.add(arg);
        }

        RedisOp operationLog = new RedisOp((String) shardKey, command, argList, parameterTypes);
        return operationLog;
    }

}
