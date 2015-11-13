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

public class ReplicaServiceDelegate implements ReplicaService {

    private ReplicaService replicaServiceMock;

    public ReplicaServiceDelegate(ReplicaService replicaServiceMock) {
        super();
        this.replicaServiceMock = replicaServiceMock;
    }

    @Override
    public void stopSyncUp(String appName, int index) {
        replicaServiceMock.stopSyncUp(appName, index);
    }

    @Override
    public String getConnectInfo(String appName, int index) {
        return replicaServiceMock.getConnectInfo(appName, index);
    }

    @Override
    public void startSyncUp(String appName, int index) {
        replicaServiceMock.startSyncUp(appName, index);
    }

    @Override
    public boolean isAllSyncedUp(String appName) {
        return replicaServiceMock.isAllSyncedUp(appName);
    }

    @Override
    public void stop(String appName) {
        replicaServiceMock.stop(appName);
    }

    @Override
    public void start(String appName) {
        replicaServiceMock.start(appName);
    }

    @Override
    public boolean isAllSyncedUp() {
        return replicaServiceMock.isAllSyncedUp();
    }

    @Override
    public void reSyncUp(String appName) {
        replicaServiceMock.reSyncUp(appName);
    }

    @Override
    public boolean isSyncedUp(String appName, int index) {
        return replicaServiceMock.isSyncedUp(appName, index);
    }

}
