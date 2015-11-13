/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.replication.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.jigsaw.redis.replication.JedisReplicationConstants;
import com.ericsson.jigsaw.redis.replication.RedisOp;
import com.ericsson.jigsaw.redis.replication.ReplicaBacklog;
import com.ericsson.jigsaw.redis.replication.ReplicaChannel;
import com.ericsson.jigsaw.redis.replication.ReplicaContext;
import com.ericsson.jigsaw.redis.replication.ReplicaDataSource;
import com.ericsson.jigsaw.redis.replication.ReplicationConstants;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ReplicaChannelImpl implements ReplicaChannel {

    protected static Logger logger = LoggerFactory.getLogger(ReplicaChannelImpl.class);

    private static ObjectMapper objectMapper = new ObjectMapper();

    private ExecutorService replicaExecutorService;
    private ExecutorService queueMonitorExecutorService;
    protected ExecutorService operationParseExecutorService;

    private boolean replicaRunning = false;
    private boolean queueMonitorRunning = false;
    protected boolean fullSyncingUp = false;
    private boolean forceSyncUp = false;

    protected boolean replicaStopping = false;
    private boolean queueMonitorStopping = false;

    private long lastSyncUpTime = 0;

    private ReplicaDataSource masterDataSource;
    protected ReplicaDataSource slaveDataSource;
    protected ReplicaBacklog replicaBacklog;

    private Long dwQueueMaxLen = JedisReplicationConstants.DW_QUEUE_MAX_LEN;
    private int monitorQueueInterval = JedisReplicationConstants.MONITOR_QUEUE_INTERVAL;

    public ReplicaChannelImpl(ReplicaDataSource masterDataSource, ReplicaDataSource slaveDataSource,
            ReplicaBacklog operationQueue) {
        this.masterDataSource = masterDataSource;
        this.slaveDataSource = slaveDataSource;
        this.replicaBacklog = operationQueue;
    }

    @Override
    public synchronized void start() {
        this.operationParseExecutorService = Executors.newFixedThreadPool(1);
        final CountDownLatch latch = new CountDownLatch(2);
        queueMonitorStopping = false;
        replicaStopping = false;

        if (!this.queueMonitorRunning) {
            this.queueMonitorExecutorService = Executors.newSingleThreadExecutor();
            this.queueMonitorExecutorService.execute(new Runnable() {

                @Override
                public void run() {
                    queueMonitorRunning = true;
                    latch.countDown();
                    while (!queueMonitorStopping) {
                        try {
                            // only master will monitor the queue
                            if (!ReplicaContext.getReplicaClusterRuntime().isMaster()) {
                                logger.debug("this node is not master, do not monitor queue");
                                Thread.sleep(JedisReplicationConstants.SLAVE_SLEEP_INTERVAL);
                                continue;
                            }
                            if (queueExceedLimit() || forceSyncUp) {
                                logger.warn("operation log queue has been full or force sync up");
                                fullSyncUp();
                                if (forceSyncUp) {
                                    forceSyncUp = false;
                                    logger.info("force sync up finished");
                                }
                            }
                            Thread.sleep(monitorQueueInterval);
                        } catch (InterruptedException ex) {
                            break;
                        } catch (Throwable ex) {
                            logger.error("catch exception while monitoring oplog queue size.", ex);
                        }
                    }
                    logger.debug("replica channel stopping, stop monitor queue size");
                    queueMonitorRunning = false;
                }
            });
        } else {
            latch.countDown();
            logger.warn("queue monitor is running, no need to start new one");
        }

        if (!this.replicaRunning) {
            this.replicaExecutorService = Executors.newSingleThreadExecutor();
            this.replicaExecutorService.execute(new Runnable() {
                @Override
                public void run() {
                    replicaRunning = true;
                    latch.countDown();
                    while (!replicaStopping) {
                        try {
                            try {
                                if (!ReplicaContext.getReplicaClusterRuntime().isMaster()) {
                                    logger.debug("this node is not master, do not replay");
                                    Thread.sleep(JedisReplicationConstants.SLAVE_SLEEP_INTERVAL);
                                    continue;
                                }
                                if (fullSyncingUp) {
                                    Thread.sleep(3000);
                                } else {
                                    int size = replay();
                                    if (size == 0) {
                                        Thread.sleep(500);
                                    }
                                }
                            } catch (InterruptedException e) {
                                logger.info("replica task sleep interrupt");
                                break;
                            }
                        } catch (Throwable ex) {
                            logger.error("catch exception while replicating.", ex);
                            logger.info("try again after 3 seconds");
                            try {
                                Thread.sleep(3000);
                            } catch (InterruptedException e) {
                                logger.info("replica exception sleep interrupt");
                                break;
                            }
                        }
                    }
                    replicaRunning = false;
                }
            });

        } else {
            latch.countDown();
            logger.warn("replica is running, no need to start new one");
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            // do nothing
        }

        this.replicaBacklog.start();
    }

    @Override
    public synchronized void stop() {
        this.replicaBacklog.stop();

        this.replicaStopping = true;
        this.replicaExecutorService.shutdownNow();
        this.queueMonitorStopping = true;
        this.queueMonitorExecutorService.shutdownNow();
        this.operationParseExecutorService.shutdownNow();

        try {
            if (!this.replicaExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("replica worker did not terminate after shutdown for 10s");
            }

            if (!this.queueMonitorExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("queue worker did not terminate after shutdown for 10s");
            }
            if (!this.operationParseExecutorService.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.warn("operation parser did not terminate after shutdown for 10s");
            }
        } catch (InterruptedException e) {
            logger.warn("stopping replica channel got interrupt", e);
            return;
        }
    }

    @Override
    public void reSyncUp() {
        this.forceSyncUp = true;
    }

    protected int replay() {
        final List<String> operations = this.replicaBacklog.batchPop(ReplicationConstants.DEFAULT_BATCH_COUNT);
        if ((operations == null) || operations.isEmpty()) {
            return 0;
        }
        //Split split = SimonManager.getStopwatch("parse_op").start();
        final List<RedisOp> operationOjbs = parse(operations);
        //split.stop();
        int retry = 0;
        while (!replicaStopping) {
            if (this.fullSyncingUp) {
                logger.info("replay retry stopped for full sync up.");
                break;
            }
            try {
                this.slaveDataSource.replay(operationOjbs);
                operationOjbs.clear();
                break;
            } catch (Exception e) {
                retry++;
                final int sleepTime = (retry <= 50 ? retry : 50) * 200;
                logger.error("replay error, retried " + retry + " times, try again " + sleepTime + " ms later");
                logger.debug("details:", e);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e1) {
                    break;
                }
            }
        }
        //if mmr is stopping but replay fail, return the ops to queue
        if (replicaStopping && (operationOjbs.size() != 0)) {
            replicaBacklog.returnOperationToQueue(operations);
        }
        return operations.size();
    }

    /**
     * synchronize parse operation log ,it seems that it's more faster
     * than the multi-thread way.
     *
     * @param operations
     * @return
     */
    private List<RedisOp> parse(List<String> operations) {
        final List<RedisOp> operationObjs = new ArrayList<RedisOp>();
        //final List<Future<RedisOperation>> operationFutures = new ArrayList<Future<RedisOperation>>();
        for (String operation : operations) {
            //operationFutures.add(this.operationParseExecutorService.submit(new OperationParseWorker(operation)));
            try {
                operationObjs.add(new OperationParseWorker(operation).call());
            } catch (Exception e) {
                logger.error("parsing operation log error, skip it. open debug level to check detail");
                logger.debug("parse operation log error", e);
            }
        }
        //FIXME: EZHIKON, delete following code after performance test
        //        for (int i = 0; i < operationFutures.size(); i++) {
        //            try {
        //                //operationObjs.add(operationFutures.get(i).get());
        //                operationObjs.add(new OperationParseWorker(operation).call());
        //            } catch (InterruptedException e) {
        //                // don't skip it avoid data loss
        //            } catch (ExecutionException e) {
        //                logger.error("parse operation error, skip it: {}", operations.get(i));
        //                logger.debug("details:", e);
        //            }
        //        }
        return operationObjs;
    }

    private void fullSyncUp() throws InterruptedException {

        if (this.fullSyncingUp) {
            logger.info("{} is syncing up, just skip this full sync up request", this);
            return;
        }

        fullSyncingUp = true;

        logger.info("{} start full sync up", this);

        int retry = 0;
        while (!queueMonitorStopping) {
            try {
                fullSyncUpInternal();
                break;
            } catch (Exception e) {
                retry++;
                final int sleepTime = (retry <= 50 ? retry : 50) * 200;
                logger.error("full sync up error, retried {} times, try again {} ms later", retry, sleepTime);
                logger.debug("details:", e);
                Thread.sleep(sleepTime);
            }
        }
        lastSyncUpTime = System.currentTimeMillis();
        fullSyncingUp = false;

    }

    private void fullSyncUpInternal() {
        try {
            this.masterDataSource.checkReadyForSyncUp();
            this.slaveDataSource.checkReadyForSyncUp();

            this.slaveDataSource.startSyncUp();
            this.masterDataSource.startSyncUp();
            this.replicaBacklog.setDryRun(true);
            this.replicaBacklog.clear();

            this.slaveDataSource.syncUpWith(masterDataSource.getConnectInfo().getAddress());

            this.waitSyncedUp();

            this.replicaBacklog.clear();
            this.replicaBacklog.setDryRun(false);
        } finally {
            try {
                this.slaveDataSource.stopSyncUp();
            } catch (Exception e) {
                logger.warn("stop sync got error: {}", e.getMessage());
                logger.debug("details:", e);
            }
            try {
                while (!queueMonitorStopping) {
                    if (this.masterDataSource.isNotStopedSyncedUpBy(this.slaveDataSource.getConnectInfo().getAddress())) {
                        logger.info("{} is still thinking {} is syncing up with it", this.masterDataSource,
                                this.slaveDataSource);
                        Thread.sleep(1000);
                    } else {
                        break;
                    }
                }
                this.masterDataSource.stopSyncUp();
            } catch (Exception e) {
                logger.warn("stop {} sync got error: {}", masterDataSource, e.getMessage());
                logger.debug("details:", e);
            }
        }
    }

    private void waitSyncedUp() {
        while (true) {
            try {
                if (this.slaveDataSource.isSyncedUp(this.masterDataSource.getConnectInfo().getAddress())
                        && this.masterDataSource.isSyncedUp(this.slaveDataSource.getConnectInfo().getAddress())) {
                    break;
                }
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                logger.error("replication sync up interrupt.", e);
                break;
            }
        }
    }

    @Override
    public boolean isSyncedUp() {
        if (!this.fullSyncingUp) {
            //if fullsyncingup=false, there maybe 2 situation,
            //1. full sync up is really done, but we still need to check sentinel has already monitor the master back
            //2. full sync up is not start yet, but resyncup is trigger, so forcesyncup=true, and should return false
            if (this.forceSyncUp) {
                return false;
            }
            return this.slaveDataSource.isSyncedUp() && this.masterDataSource.isSyncedUp();
        }
        return false;
    }

    @Override
    public long getLastSyncUpTime() {
        return lastSyncUpTime;
    }

    private boolean queueExceedLimit() {
        final Long size = this.replicaBacklog.size();
        if (size != null) {
            return size > dwQueueMaxLen;
        }
        return false;
    }

    @Override
    public ReplicaDataSource getMasterDataSource() {
        return masterDataSource;
    }

    @Override
    public ReplicaDataSource getSlaveDataSource() {
        return slaveDataSource;
    }

    @Override
    public ReplicaBacklog getReplicaBacklog() {
        return replicaBacklog;
    }

    public void setDwQueueMaxLen(Long dwQueueMaxLen) {
        this.dwQueueMaxLen = dwQueueMaxLen;
    }

    public void setMonitorQueueInterval(int monitorQueueInterval) {
        this.monitorQueueInterval = monitorQueueInterval;
    }

    @Override
    public String toString() {
        return "Replica Channel[" + this.masterDataSource + "->" + this.slaveDataSource + "]";
    }

    private static class OperationParseWorker implements Callable<RedisOp> {

        private String operation;

        private OperationParseWorker(String operation) {
            this.operation = operation;
        }

        @Override
        public RedisOp call() throws Exception {
            RedisOp op = objectMapper.readValue(operation, RedisOp.class);
            return op;
        }
    }
}
