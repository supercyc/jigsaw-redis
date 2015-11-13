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

import java.io.ByteArrayOutputStream;
import java.util.List;

import com.ericsson.jigsaw.redis.replication.RedisOp;
import com.ericsson.jigsaw.redis.replication.RedisOpUtil;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

public class RedisOpUtilKryo extends RedisOpUtil {
    private static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.register(RedisOp.class);
            return kryo;
        }
    };

    public static byte[] buildOperationLogByte(Object shardKey, String command, Object[] args,
            List<String> parameterTypes) {
        RedisOp operationLog = buildRedisOperationObj(shardKey, command, args, parameterTypes);
        Output output = new Output(new ByteArrayOutputStream());
        try {
            kryos.get().writeObject(output, operationLog);
            return output.toBytes();
        } finally {
            output.close();
        }
    }
}
