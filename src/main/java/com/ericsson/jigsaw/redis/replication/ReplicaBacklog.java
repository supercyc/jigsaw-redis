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

public interface ReplicaBacklog {

    void push(String operation);

    String pop();

    byte[] popByte();

    void clear();

    long size();

    List<String> batchPop(int size);

    List<byte[]> batchPopByte(int size);

    void stop();

    void start();

    void setDryRun(boolean dryRun);

    void returnOperationToQueue(List<String> operation);
}
