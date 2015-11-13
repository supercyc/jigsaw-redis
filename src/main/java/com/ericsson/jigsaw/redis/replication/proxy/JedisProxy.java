package com.ericsson.jigsaw.redis.replication.proxy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import com.ericsson.jigsaw.redis.replication.backlog.RedisBacklogManager;

public class JedisProxy extends ReplicationProxy<Jedis> {

    private static Logger logger = LoggerFactory.getLogger(JedisProxy.class);
    private PipelineProxy pipelineProxy;

    public JedisProxy(RedisBacklogManager dwManager) {
        super(dwManager);
        pipelineProxy = new PipelineProxy(dwManager);
    }

    @Override
    protected Jedis create() {
        logger.debug("Create JedisProxy.");

        // proxied instance
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(Jedis.class);
        enhancer.setCallback(this);

        // create a fake proxy instance
        return (Jedis) enhancer.create(new Class[] { String.class }, new Object[] { "" });
    }

    @Override
    public Object invoke(Method method, Object[] args) throws Throwable {
        try {
            if ("pipelined".equals(method.getName())) {
                final Pipeline pipeline = getTarget().pipelined();
                return pipelineProxy.onBehalfOf(pipeline);
            }
            return super.invoke(method, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
