/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisException;

import com.ericsson.jigsaw.redis.JedisShardedTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisAction;
import com.ericsson.jigsaw.redis.JedisTemplate.JedisActionNoResult;
import com.ericsson.jigsaw.redis.JedisTemplate.PipelineAction;
import com.ericsson.jigsaw.redis.JedisTemplate.PipelineActionNoResult;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.replication.backlog.RedisBacklogManager;
import com.ericsson.jigsaw.redis.replication.exception.JedisReplicationException;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;
import com.ericsson.jigsaw.redis.replication.proxy.JedisPoolProxy;
import com.ericsson.jigsaw.redis.shard.ShardingProvider;

/**
 * JedisReplicationShardedTemplate is the JedisTemplate, which has
 * multi-master-replication and key sharding feature.
 * <p/>
 * Pass more than one JedisPool to the it, it will calculate which
 * jedisPool will handle the key. If only one jedisPool passed, it
 * won't do the calculation, so please use
 * JedisReplicationShardedTemplate by default.
 * <p/>
 * But it doesn't support multi-key actions by default, like: 1.
 * Pipelined and Transaction process multi-keys 2. Methods in
 *
 * @redis.clients.jedis.MultiKeyCommands like mget, rpoplpush....
 *                                       <p/>
 *                                       To support them, please use a
 *                                       common sharding key relate to
 *                                       these keys when invoke the
 *                                       APIs.
 *                                       <p/>
 *                                       Only the kind of idempotent
 *                                       actions are supported in
 *                                       Jedis based replication case.
 */
public class JedisReplicationShardedTemplate {

    private final static ThreadLocal<String> shardKeyStore = new ThreadLocal<String>();

    private static Logger logger = LoggerFactory.getLogger(JedisReplicationShardedTemplate.class);

    private JedisShardedTemplate originalDataTemplate;

    public JedisReplicationShardedTemplate(ShardingProvider<JedisTemplate> dataShardingProvider,
            List<JedisPool> localDataJedisPools, List<JedisPool> dwQueueJedisPools) {
        this(dataShardingProvider, localDataJedisPools, dwQueueJedisPools, null);
    }

    public JedisReplicationShardedTemplate(ShardingProvider<JedisTemplate> dataShardingProvider,
            List<JedisPool> dataJedisPools, List<JedisPool> backlogQueueJedisPools,
            JedisExceptionHandler exceptionHandler) {
        List<JedisPool> orinalDataJedisPools;
        if ((backlogQueueJedisPools == null) || backlogQueueJedisPools.isEmpty()) {
            logger.debug("Initialize JedisReplicationShardedTemplate without redisOp backlog queue.");
            orinalDataJedisPools = dataJedisPools;
        } else {
            if (dataJedisPools.size() != backlogQueueJedisPools.size()) {
                String description = "Pool size not equal, data pool size: " + dataJedisPools.size()
                        + ", backlog queue pool size: " + backlogQueueJedisPools.size();

                throw new JedisReplicationException(description);
            }

            final RedisBacklogManager backlogManager = initBacklogManager(dataShardingProvider, backlogQueueJedisPools,
                    exceptionHandler);
            orinalDataJedisPools = wrapJedisPools(dataJedisPools, backlogManager);
        }

        this.originalDataTemplate = new JedisShardedTemplate(dataShardingProvider, exceptionHandler,
                orinalDataJedisPools);
    }

    private List<JedisPool> wrapJedisPools(List<JedisPool> jedisPools, RedisBacklogManager backlogManager) {
        final List<JedisPool> wrappedJedisPools = new ArrayList<JedisPool>();
        for (final JedisPool jedisPool : jedisPools) {
            wrappedJedisPools.add(new JedisPoolProxy(jedisPool, backlogManager).getProxy());
        }
        return wrappedJedisPools;
    }

    private RedisBacklogManager initBacklogManager(ShardingProvider<JedisTemplate> dataShardingProvider,
            List<JedisPool> dwQueueJedisPools, JedisExceptionHandler exceptionHandler) {

        logger.debug("Create and initialize backlog manager.");

        printJedisPoolInfo(dwQueueJedisPools);

        // create operation log queue handler
        final ShardingProvider<JedisTemplate> dwJedisTemplateShardingProvider = dataShardingProvider
                .derive(JedisTemplate.class);

        return new RedisBacklogManager(new JedisShardedTemplate(dwJedisTemplateShardingProvider, exceptionHandler,
                dwQueueJedisPools));
    }

    private void printJedisPoolInfo(List<JedisPool> jedisPools) {
        for (JedisPool pool : jedisPools) {
            String poolName = pool.getFormattedPoolName();
            HostAndPort hostPort = pool.getAddress();
            String host = hostPort.getHost();
            int port = hostPort.getPort();

            logger.debug("JedisPool: " + poolName + "-" + host + "-" + port);
        }
    }

    /*
     * Execute the action.
     * 
     * @key the action must process only this key, or this key is a
     * sharding key.
     */
    public <T> T execute(String key, JedisAction<T> jedisAction) throws JedisException {
        shardKeyStore.set(key);
        return this.originalDataTemplate.execute(key, jedisAction);
    }

    /*
     * Execute the action.
     * 
     * @key the action must process only this key, or this key is a
     * sharding key.
     */
    public void execute(String key, JedisActionNoResult jedisAction) throws JedisException {
        shardKeyStore.set(key);
        this.originalDataTemplate.execute(key, jedisAction);
    }

    /*
     * Execute the pipeline.
     * 
     * @key the action must process only this key, or this key is a
     * sharding key.
     */
    public List<Object> execute(String key, PipelineAction pipelineAction) throws JedisException {
        shardKeyStore.set(key);
        return this.originalDataTemplate.execute(key, pipelineAction);
    }

    /*
     * Execute the pipeline.
     * 
     * @key the action must process only this key, or this key is a
     * sharding key.
     */
    public void execute(String key, final PipelineActionNoResult pipelineAction) throws JedisException {
        shardKeyStore.set(key);
        this.originalDataTemplate.execute(key, pipelineAction);
    }

    // / Common Actions ///

    public Boolean del(final String key) {
        shardKeyStore.set(key);
        return this.originalDataTemplate.del(key);
    }

    public Boolean del(final String shardingKey, final String key) {
        shardKeyStore.set(shardingKey);
        return this.originalDataTemplate.del(shardingKey, key);
    }

    // / String Actions ///
    public String get(final String key) {
        shardKeyStore.set(key);
        return this.originalDataTemplate.get(key);
    }

    public String get(final String shardingKey, final String key) {
        shardKeyStore.set(shardingKey);
        return this.originalDataTemplate.get(shardingKey, key);
    }

    public List<String> mget(final String shardingKey, final String... keys) {
        shardKeyStore.set(shardingKey);
        return this.originalDataTemplate.mget(shardingKey, keys);
    }

    public void set(final String key, final String value) {
        shardKeyStore.set(key);
        this.originalDataTemplate.set(key, value);
    }

    public void set(final String shardingKey, final String key, final String value) {
        shardKeyStore.set(shardingKey);
        this.originalDataTemplate.set(shardingKey, key, value);
    }

    public void setex(final String key, final String value, final int seconds) {
        shardKeyStore.set(key);
        this.originalDataTemplate.setex(key, value, seconds);
    }

    public void setex(final String shardingKey, final String key, final String value, final int seconds) {
        shardKeyStore.set(shardingKey);
        this.originalDataTemplate.setex(shardingKey, key, value, seconds);
    }

    // / Set Actions ///
    public Boolean sadd(final String key, final String member) {
        shardKeyStore.set(key);
        return this.originalDataTemplate.sadd(key, member);
    }

    public Boolean sadd(final String shardingKey, final String key, final String member) {
        shardKeyStore.set(shardingKey);
        return this.originalDataTemplate.sadd(shardingKey, key, member);
    }

    public Boolean srem(final String key, final String member) {
        shardKeyStore.set(key);

        return this.originalDataTemplate.srem(key, member);
    }

    public Boolean srem(final String shardingKey, final String key, final String member) {

        shardKeyStore.set(shardingKey);
        return this.originalDataTemplate.srem(shardingKey, key, member);
    }

    public Set<String> smembers(final String key) {
        shardKeyStore.set(key);
        return this.originalDataTemplate.smembers(key);
    }

    public Set<String> smembers(final String shardingKey, final String key) {
        shardKeyStore.set(shardingKey);
        return this.originalDataTemplate.smembers(shardingKey, key);
    }

    // shard key store
    public static String getShardKey() {
        return shardKeyStore.get();
    }
}