package com.ericsson.jigsaw.redis.replication.proxy;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Pipeline;

import com.ericsson.jigsaw.redis.replication.backlog.RedisBacklogManager;

public class PipelineProxy extends BatchReplicationProxy<Pipeline> {

    private static Logger logger = LoggerFactory.getLogger(PipelineProxy.class);

    public PipelineProxy(RedisBacklogManager backlogManager) {
        super(backlogManager);
    }

    @Override
    protected boolean isCommitMethod(Method method) {
        return "sync".equals(method.getName()) || "syncAndReturnAll".equals(method.getName());
    }

    @Override
    protected Pipeline create() {
        logger.debug("Create pipeline proxy.");

        // proxied instance
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(Pipeline.class);
        enhancer.setCallback(this);

        // proxy instance
        return (Pipeline) enhancer.create();
    }
}
