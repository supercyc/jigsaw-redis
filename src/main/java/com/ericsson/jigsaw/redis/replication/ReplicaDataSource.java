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

import java.util.List;

import redis.clients.jedis.HostAndPort;

public interface ReplicaDataSource {

    void checkReadyForSyncUp();

    void startSyncUp();

    void syncUpWith(HostAndPort address);

    void stopSyncUp();

    boolean isNotStopedSyncedUpBy(HostAndPort address);

    boolean isSyncedUp(HostAndPort address);

    boolean isSyncedUp();

    ReplicaConnectInfo getConnectInfo();

    void replay(List<RedisOp> operations);
}
