/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication.proxy;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.ericsson.jigsaw.redis.replication.JedisReplicationConstants;
import com.ericsson.jigsaw.redis.replication.JedisReplicationShardedTemplate;
import com.ericsson.jigsaw.redis.replication.RedisOpUtil;
import com.ericsson.jigsaw.redis.replication.backlog.RedisBacklogManager;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;

public abstract class ReplicationProxy<T> extends BacklogProxy<T> {

    public ReplicationProxy(RedisBacklogManager backlogManager) {
        super(backlogManager);
    }

    @Override
    public void postInvoke(Method method, Object[] args) throws Throwable {
        // only record write kind of idempotent operation
        if (JedisReplicationConstants.WRITE_IDEMPOTENT_OPERATIONS.contains(method.getName())) {
            replicate(method, args);
        }
        super.postInvoke(method, args);
    }

    protected void replicate(Method method, Object[] args) throws JsonGenerationException, JsonMappingException,
            IOException {
        final Serializable redisOp = serialize(method, args);
        final List<Serializable> redisOps = Arrays.asList(redisOp);
        this.enqueue(redisOps);
    }

    protected Serializable serialize(Method method, Object[] args) throws JsonGenerationException,
            JsonMappingException, IOException {
        final List<String> parameterTypes = getParameterTypes(method);
        final String operationLog = RedisOpUtil.buildOperationLogString(JedisReplicationShardedTemplate.getShardKey(),
                method.getName(), args, parameterTypes);
        return operationLog;
    }

    protected void enqueue(List<Serializable> redisOps) {
        backlogManager.feed(JedisReplicationShardedTemplate.getShardKey(), redisOps);
    }

    protected List<String> getParameterTypes(Method method) {
        Class<?>[] parameterTypeClassList = method.getParameterTypes();
        List<String> parameterTypes = new ArrayList<String>();
        for (Class<?> clazz : parameterTypeClassList) {
            parameterTypes.add(clazz.getName());
        }
        return parameterTypes;
    }
}
