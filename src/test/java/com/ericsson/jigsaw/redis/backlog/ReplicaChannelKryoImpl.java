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

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.ericsson.jigsaw.redis.replication.RedisOp;
import com.ericsson.jigsaw.redis.replication.ReplicaBacklog;
import com.ericsson.jigsaw.redis.replication.ReplicaDataSource;
import com.ericsson.jigsaw.redis.replication.ReplicationConstants;
import com.ericsson.jigsaw.redis.replication.impl.ReplicaChannelImpl;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

public class ReplicaChannelKryoImpl extends ReplicaChannelImpl {

    private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.register(RedisOp.class);
            return kryo;
        }
    };

    public ReplicaChannelKryoImpl(ReplicaDataSource masterDataSource, ReplicaDataSource slaveDataSource,
            ReplicaBacklog replicaBacklog) {
        super(masterDataSource, slaveDataSource, replicaBacklog);
    }

    @Override
    protected int replay() {
        final List<byte[]> operations = this.replicaBacklog.batchPopByte(ReplicationConstants.DEFAULT_BATCH_COUNT);
        if ((operations == null) || operations.isEmpty()) {
            return 0;
        }
        final List<RedisOp> operationOjbs = parse(operations);
        int retry = 0;
        while (!replicaStopping) {
            if (this.fullSyncingUp) {
                logger.info("replay retry stopped for full sync up.");
                break;
            }
            try {
                this.slaveDataSource.replay(operationOjbs);
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
        return operations.size();
    }

    private List<RedisOp> parse(List<byte[]> operations) {
        final List<RedisOp> operationObjs = new ArrayList<RedisOp>();
        final List<Future<RedisOp>> operationFutures = new ArrayList<Future<RedisOp>>();
        for (byte[] operation : operations) {
            //operationFutures.add(this.operationParseExecutorService.submit(new OperationByteParseWorker(operation)));
            try {
                operationObjs.add(new OperationByteParseWorker(operation).call());
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        //        for (int i = 0; i < operationFutures.size(); i++) {
        //            try {
        //                operationObjs.add(operationFutures.get(i).get());
        //            } catch (InterruptedException e) {
        //                // don't skip it avoid data loss
        //            } catch (ExecutionException e) {
        //                logger.error("parse operation error, skip it: {}", operations.get(i));
        //                logger.debug("details:", e);
        //            }
        //        }
        return operationObjs;
    }

    private class OperationByteParseWorker implements Callable<RedisOp> {

        private byte[] operation;

        private OperationByteParseWorker(byte[] operation) {
            this.operation = operation;
        }

        @Override
        public RedisOp call() throws Exception {
            Input input = new Input(new ByteArrayInputStream(this.operation));
            Kryo kryo = kryos.get();
            RedisOp op = kryo.readObject(input, RedisOp.class);
            kryo.reset();
            return op;
        }
    }
}
