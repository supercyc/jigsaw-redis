/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ericsson.jigsaw.redis.pool.JedisPool;
import com.ericsson.jigsaw.redis.replication.backlog.RedisReplicaBacklog;
import com.ericsson.jigsaw.redis.replication.exception.handler.JedisExceptionHandler;
import com.ericsson.jigsaw.redis.replication.impl.ReplicaChannelImpl;
import com.ericsson.jigsaw.redis.replication.impl.ReplicaDataSourceFactory;

public class ReplicaManager implements ReplicaService {

    private static Logger logger = LoggerFactory.getLogger(ReplicaManager.class);

    private ConcurrentHashMap<String, List<ReplicaChannel>> replicaChannelsMap = new ConcurrentHashMap<String, List<ReplicaChannel>>();

    private ReplicaService replicaServiceClient;

    private JedisExceptionHandler jedisExceptionHandler;

    public ReplicaManager(List<ReplicaInfo> replicaInfos, ReplicaService replicaServiceClient,
            JedisExceptionHandler jedisExceptionHandler) {
        this.replicaServiceClient = replicaServiceClient;
        this.jedisExceptionHandler = jedisExceptionHandler;
        for (ReplicaInfo replicaInfo : replicaInfos) {
            initChannels(replicaInfo.getAppName(), replicaInfo.getDataPools(), replicaInfo.getOperationPools());
        }
    }

    private void initChannels(String appName, List<JedisPool> masterDataPools, List<JedisPool> operationPools) {
        logger.info("init replica channel for {}", appName);
        if ((masterDataPools.size() != operationPools.size())) {
            throw new RuntimeException("pool size are not matched!");
        }
        replicaChannelsMap.putIfAbsent(appName, new ArrayList<ReplicaChannel>());
        List<ReplicaChannel> replicaChannels = replicaChannelsMap.get(appName);
        replicaChannels.clear();
        for (int i = 0; i < masterDataPools.size(); i++) {
            final ReplicaDataSource localReplicaDataSource = ReplicaDataSourceFactory.createLocalReplicaDataSource(
                    masterDataPools.get(i), jedisExceptionHandler);
            final ReplicaDataSource remoteReplicaDataSource = ReplicaDataSourceFactory.createRemoteReplicaDataSource(
                    appName, replicaServiceClient, i, jedisExceptionHandler);
            replicaChannels.add(new ReplicaChannelImpl(localReplicaDataSource, remoteReplicaDataSource,
                    new RedisReplicaBacklog(operationPools.get(i), jedisExceptionHandler)));
        }
    }

    public void scaleOut(ReplicaInfo replicaInfo) {
        logger.info("scale out application {}", replicaInfo.getAppName());
        stop(replicaInfo.getAppName());
        initChannels(replicaInfo.getAppName(), replicaInfo.getDataPools(), replicaInfo.getOperationPools());
    }

    public void start() {
        for (Map.Entry<String, List<ReplicaChannel>> entry : replicaChannelsMap.entrySet()) {
            for (ReplicaChannel replicaChannel : entry.getValue()) {
                replicaChannel.start();
            }
        }
    }

    public void stop() {
        for (Map.Entry<String, List<ReplicaChannel>> entry : replicaChannelsMap.entrySet()) {
            for (ReplicaChannel replicaChannel : entry.getValue()) {
                replicaChannel.stop();
            }
        }
    }

    public long getLastSyncUpTime(String appName, int index) {
        return this.replicaChannelsMap.get(appName).get(index).getLastSyncUpTime();
    }

    @Override
    public boolean isAllSyncedUp(String appName) {
        for (ReplicaChannel channel : this.replicaChannelsMap.get(appName)) {
            if (!channel.isSyncedUp()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isAllSyncedUp() {
        for (Map.Entry<String, List<ReplicaChannel>> entry : this.replicaChannelsMap.entrySet()) {
            for (ReplicaChannel channel : entry.getValue()) {
                if (!channel.isSyncedUp()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public boolean isSyncedUp(String appName, int index) {
        return this.replicaChannelsMap.get(appName).get(index).getMasterDataSource().isSyncedUp();
    }

    @Override
    public void startSyncUp(String appName, int index) {
        logger.debug("start syncup app {} with index {}", appName, index);
        this.replicaChannelsMap.get(appName).get(index).getMasterDataSource().startSyncUp();
    }

    @Override
    public String getConnectInfo(String appName, int index) {
        logger.debug("get redis address of app {} with index {}", appName, index);
        return this.replicaChannelsMap.get(appName).get(index).getMasterDataSource().getConnectInfo().toString();
    }

    @Override
    public void stopSyncUp(String appName, int index) {
        logger.debug("stop syncup app {} with index {}", appName, index);
        this.replicaChannelsMap.get(appName).get(index).getMasterDataSource().stopSyncUp();
    }

    @Override
    public void stop(String appName) {
        logger.info("stop replication for app {}", appName);
        for (ReplicaChannel replicaChannel : replicaChannelsMap.get(appName)) {
            replicaChannel.stop();
        }
    }

    @Override
    public void start(String appName) {
        logger.info("start replication for app {}", appName);
        for (ReplicaChannel replicaChannel : replicaChannelsMap.get(appName)) {
            replicaChannel.start();
        }
    }

    @Override
    public void reSyncUp(String appName) {
        logger.info("start resyncup for app {}", appName);
        for (final ReplicaChannel replicaChannel : replicaChannelsMap.get(appName)) {
            replicaChannel.reSyncUp();
        }
    }

}
