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

import org.apache.commons.lang3.StringUtils;

import com.ericsson.jigsaw.cluster.ClusterRuntime;

public class ReplicaContext {
    private static ClusterRuntime replicaClusterRuntime;
    private static String redisPassword;

    public static ClusterRuntime getReplicaClusterRuntime() {
        return replicaClusterRuntime;
    }

    public static void setReplicaClusterRuntime(ClusterRuntime replicaClusterRuntime) {
        ReplicaContext.replicaClusterRuntime = replicaClusterRuntime;
    }

    public static void setRedisPassword(String redisPasswd) {
        if (StringUtils.isNotBlank(redisPasswd)) {
            ReplicaContext.redisPassword = redisPasswd;
        }
    }

    //use for test
    public static void resetPassword() {
        ReplicaContext.redisPassword = null;
    }

    public static String getRedisPassword() {
        return redisPassword;
    }

}
