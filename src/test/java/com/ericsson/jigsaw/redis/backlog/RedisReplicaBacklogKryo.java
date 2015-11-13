/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.backlog;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Pipeline;

import com.ericsson.jigsaw.redis.JedisTemplate;
import com.ericsson.jigsaw.redis.JedisTemplate.PipelineAction;
import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.replication.JedisReplicationConstants;
import com.ericsson.jigsaw.redis.replication.ReplicaBacklog;
import com.ericsson.jigsaw.redis.replication.backlog.RedisReplicaBacklog;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;

public class RedisReplicaBacklogKryo implements ReplicaBacklog {
    private static final Logger logger = LoggerFactory.getLogger(RedisReplicaBacklog.class);

    private JedisTemplate operationTemplate;

    private BlockingQueue<byte[]> buffer = new ArrayBlockingQueue<byte[]>(2000);

    private ExecutorService executorService;

    private boolean stopping = false;

    private boolean running = false;

    private boolean dryRun = false;

    public RedisReplicaBacklogKryo(JedisPool operationPool, JedisExceptionHandler jedisExceptionHandler) {
        this.operationTemplate = new JedisTemplate(operationPool, jedisExceptionHandler);
    }

    @Override
    public synchronized void start() {
        this.stopping = false;
        final CountDownLatch latch = new CountDownLatch(1);

        if (!this.running) {
            this.executorService = Executors.newSingleThreadExecutor();
            this.executorService.submit(new Runnable() {

                @Override
                public void run() {
                    running = true;
                    latch.countDown();
                    while (!stopping) {
                        try {

                            List<Object> oplogs = operationTemplate.execute(new PipelineAction() {
                                @Override
                                public List<Object> action(Pipeline pipeline) {
                                    for (int i = 0; i < JedisReplicationConstants.BATCH_POP_SIZE; i++) {
                                        pipeline.rpop(JedisReplicationConstants.DOUBLEWRITE_QUEUE.getBytes());
                                    }
                                    return null;
                                }
                            });

                            boolean hasValidValue = false;
                            for (Object operation : oplogs) {

                                if (operation != null) {
                                    hasValidValue = true;
                                    if (!dryRun) {
                                        buffer.put((byte[]) operation);
                                    }
                                }

                            }
                            //has no valid value means no traffic , sleep 500ms to avoid dry run
                            if (!hasValidValue) {
                                Thread.sleep(500);
                            }
                        } catch (InterruptedException ie) {
                            logger.warn("dequeue get interrupt");
                            break;
                        } catch (Exception e) {
                            logger.error("buffer the operation queue got error", e);
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e1) {
                                break;
                            }
                        }
                    }
                    running = false;
                }
            });
        } else {
            latch.countDown();
            logger.warn("queue batch pop is running, no need to start new one");
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            // do nothing
        }
    }

    @Override
    public synchronized void stop() {
        this.stopping = true;
        this.executorService.shutdownNow();
        try {
            if (!this.executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("operation queue runner did not terminate after shutdown for 10s");
            }
        } catch (InterruptedException e) {
            return;
        }
    }

    @Override
    public void push(String operation) {
        this.operationTemplate.lpush(JedisReplicationConstants.DOUBLEWRITE_QUEUE, operation);
    }

    @Override
    public List<String> batchPop(int size) {
        return null;
    }

    @Override
    public String pop() {
        return null;
    }

    @Override
    public long size() {
        return this.operationTemplate.llen(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
    }

    @Override
    public void clear() {
        this.operationTemplate.del(JedisReplicationConstants.DOUBLEWRITE_QUEUE);
        this.buffer.clear();
    }

    @Override
    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    @Override
    public byte[] popByte() {
        try {
            return this.buffer.take();
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public List<byte[]> batchPopByte(int size) {
        List<byte[]> operations = new ArrayList<byte[]>();
        buffer.drainTo(operations, size);
        return operations;
    }

    @Override
    public void returnOperationToQueue(List<String> operation) {
        // TODO Auto-generated method stub

    }
}
