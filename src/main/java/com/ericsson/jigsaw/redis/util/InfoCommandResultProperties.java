/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.util;


public class InfoCommandResultProperties extends CommandResultProperties {
    public static final String TAG_Server_Group = "Server";
    public static final String TAG_Sentinel_Group = "Sentinel";
    public static final String TAG_Clients_Group = "Clients";
    public static final String TAG_Memory_Group = "Memory";
    public static final String TAG_Stats_Group = "Stats";
    public static final String TAG_Replication_Group = "Replication";
    public static final String TAG_CPU_Group = "CPU";
    public static final String TAG_Keyspace_Group = "Keyspace";

    @SuppressWarnings("unchecked")
    public <T> T getServerGroupProperties() {
        return (T) attributes.get(TAG_Server_Group);
    }

    @SuppressWarnings("unchecked")
    public <T> T getSentinelGroupProperties() {
        return (T) attributes.get(TAG_Sentinel_Group);
    }

    @SuppressWarnings("unchecked")
    public <T> T getClientsGroupProperties() {
        return (T) attributes.get(TAG_Clients_Group);
    }

    @SuppressWarnings("unchecked")
    public <T> T getMemoryGroupProperties() {
        return (T) attributes.get(TAG_Memory_Group);
    }

    @SuppressWarnings("unchecked")
    public <T> T getStatsGroupProperties() {
        return (T) attributes.get(TAG_Stats_Group);
    }

    @SuppressWarnings("unchecked")
    public <T> T getReplicationGroupProperties() {
        return (T) attributes.get(TAG_Replication_Group);
    }

    @SuppressWarnings("unchecked")
    public <T> T getCPUGroupProperties() {
        return (T) attributes.get(TAG_CPU_Group);
    }

    @SuppressWarnings("unchecked")
    public <T> T getKeyspaceGroupProperties() {
        return (T) attributes.get(TAG_Keyspace_Group);
    }

    public boolean isRreplicationCompleted() {
        String masterLinkStatus = getAttribute(TAG_Replication_Group, "master_link_status");
        String masterSyncInProgress = getAttribute(TAG_Replication_Group, "master_sync_in_progress");
        if ("up".equals(masterLinkStatus) && "0".equals(masterSyncInProgress)) {
            return true;
        }
        return false;
    }
}
