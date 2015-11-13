package com.ericsson.jigsaw.redis.backlog;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;

import com.ericsson.jigsaw.redis.replication.JedisReplicationShardedTemplate;
import com.ericsson.jigsaw.redis.replication.backlog.RedisBacklogManager;
import com.ericsson.jigsaw.redis.replication.proxy.PipelineProxy;

public class PipelineProxyKryo extends PipelineProxy {

    public PipelineProxyKryo(RedisBacklogManager dwManager) {
        super(dwManager);
    }

    @Override
    protected Serializable serialize(Method method, Object[] args) {
        final List<String> parameterTypes = getParameterTypes(method);
        byte[] operationLog = RedisOpUtilKryo.buildOperationLogByte(JedisReplicationShardedTemplate.getShardKey(),
                method.getName(), args, parameterTypes);
        return operationLog;
    }

    @Override
    protected void enqueue(List<Serializable> redisOps) {
        backlogManager.enqueueKryo(JedisReplicationShardedTemplate.getShardKey(), redisOps);
    }
}