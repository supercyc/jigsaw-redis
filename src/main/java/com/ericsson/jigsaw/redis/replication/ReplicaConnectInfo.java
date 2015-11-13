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

import redis.clients.jedis.HostAndPort;

public class ReplicaConnectInfo {

    private static final String CONNECTINFO_DELIMITER = ",";

    private HostAndPort address;
    private int database;

    public ReplicaConnectInfo(HostAndPort address, int database) {
        this.address = address;
        this.database = database;
    }

    public HostAndPort getAddress() {
        return address;
    }

    public void setAddress(HostAndPort address) {
        this.address = address;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    @Override
    public String toString() {
        StringBuilder addr = new StringBuilder();
        return addr.append(this.address).append(CONNECTINFO_DELIMITER).append(this.database).toString();
    }

    public static ReplicaConnectInfo fromString(String connectInfoStr) {
        String[] infos = connectInfoStr.split(CONNECTINFO_DELIMITER);
        String[] address = infos[0].split(":");
        HostAndPort hostAndPort = new HostAndPort(address[0], Integer.valueOf(address[1]));
        return new ReplicaConnectInfo(hostAndPort, Integer.valueOf(infos[1]));
    }
}
