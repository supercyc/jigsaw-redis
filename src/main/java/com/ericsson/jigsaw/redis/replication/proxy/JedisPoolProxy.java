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

import java.lang.reflect.Method;

import net.sf.cglib.proxy.Enhancer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.replication.backlog.RedisBacklogManager;

public class JedisPoolProxy extends TargetProxy<JedisPool> {

    private static Logger logger = LoggerFactory.getLogger(JedisPoolProxy.class);

    private JedisProxy jedisProxy;

    private ThreadLocal<Jedis> localJedis = new ThreadLocal<Jedis>();

    public JedisPoolProxy(JedisPool target, RedisBacklogManager backlogManager) {
        super(target);
        jedisProxy = new JedisProxy(backlogManager);
    }

    @Override
    protected JedisPool create() {
        logger.debug("Create JedisPoolProxy.");

        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(JedisPool.class);
        enhancer.setCallback(this);

        // proxy instance
        return (JedisPool) enhancer.create();
    }

    @Override
    public Object invoke(Method method, Object[] args) throws Throwable {
        if ("getResource".equals(method.getName())) {
            final Jedis jedis = getTarget().getResource();
            final Jedis proxy = jedisProxy.onBehalfOf(jedis);
            this.localJedis.set(jedis);
            return proxy;
        } else if ("returnResource".equals(method.getName()) || "returnBrokenResource".equals(method.getName())) {
            final Jedis jedis = this.localJedis.get();
            final Object result = super.invoke(method, new Object[] { jedis });
            this.localJedis.remove();
            jedisProxy.detachTarget();
            return result;
        }

        return super.invoke(method, args);
    }
}
