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

import com.ericsson.jigsaw.redis.pool.JedisPool;

public class ReplicaInfo {

    private String appName;
    private List<JedisPool> dataPools;
    private List<JedisPool> operationPools;

    public ReplicaInfo(String appName, List<JedisPool> dataPools, List<JedisPool> operationPools) {
        this.appName = appName;
        this.dataPools = dataPools;
        this.operationPools = operationPools;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public List<JedisPool> getDataPools() {
        return dataPools;
    }

    public void setDataPools(List<JedisPool> dataPools) {
        this.dataPools = dataPools;
    }

    public List<JedisPool> getOperationPools() {
        return operationPools;
    }

    public void setOperationPools(List<JedisPool> operationPools) {
        this.operationPools = operationPools;
    }

}
