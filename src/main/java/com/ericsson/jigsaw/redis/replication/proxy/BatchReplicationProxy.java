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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.ericsson.jigsaw.redis.replication.backlog.RedisBacklogManager;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;

public abstract class BatchReplicationProxy<T> extends ReplicationProxy<T> {

    private ConcurrentHashMap<Object, List<Serializable>> operationMap = new ConcurrentHashMap<Object, List<Serializable>>();

    public BatchReplicationProxy(RedisBacklogManager backlogManager) {
        super(backlogManager);
    }

    @Override
    public void postInvoke(Method method, Object[] args) throws Throwable {
        if (isCommitMethod(method)) {
            commit();
        } else {
            super.postInvoke(method, args);
        }
    }

    protected abstract boolean isCommitMethod(Method method);

    @Override
    protected void replicate(Method method, Object[] args) throws JsonGenerationException, JsonMappingException,
            IOException {
        cacheRedisOp(method, args);
    }

    @Override
    protected void detachTarget() {
        // do noting
    }

    private void cacheRedisOp(Method method, Object[] args) throws JsonGenerationException, JsonMappingException,
            IOException {
        final T target = getTarget();
        operationMap.putIfAbsent(target, new ArrayList<Serializable>());
        final List<Serializable> redisOps = operationMap.get(target);
        final Serializable redisOp = super.serialize(method, args);
        redisOps.add(redisOp);
    }

    private void commit() {
        final T target = getTarget();
        List<Serializable> redisOps = operationMap.get(target);
        if (redisOps != null) {
            super.enqueue(redisOps);
        }
        operationMap.remove(target);
    }
}
