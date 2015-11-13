package com.ericsson.jigsaw.redis.replication.backlog;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Pipeline;

import com.ericsson.jigsaw.redis.JedisShardedTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.PipelineActionNoResult;
import com.ericsson.jigsaw.redis.replication.JedisReplicationConstants;

public class RedisBacklogManager {

    private static Logger logger = LoggerFactory.getLogger(RedisBacklogManager.class);

    private JedisShardedTemplate jedisTemplate;

    public RedisBacklogManager(JedisShardedTemplate jedisTemplate) {
        this.jedisTemplate = jedisTemplate;
    }

    public void feed(Object shardKey, final List<Serializable> redisOps) {
        if ((redisOps != null) && (redisOps.size() != 0)) {
            JedisTemplate template = jedisTemplate.getShard(shardKey.toString());
            template.execute(new PipelineActionNoResult() {

                @Override
                public void action(Pipeline pipeline) {
                    for (Serializable redisOp : redisOps) {
                        pipeline.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, (String) redisOp);
                        logger.debug("feed redisOp to backlog: {}", redisOp);
                    }
                }
            });
        }
    }

    public void enqueueKryo(Object shardKey, final List<Serializable> operations) {
        if ((operations != null) && (operations.size() != 0)) {
            JedisTemplate template = jedisTemplate.getShard(shardKey.toString());
            template.execute(new PipelineActionNoResult() {

                @Override
                public void action(Pipeline pipeline) {
                    for (Serializable operation : operations) {
                        pipeline.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE.getBytes(), (byte[]) operation);
                    }
                }
            });
        }
    }
}
