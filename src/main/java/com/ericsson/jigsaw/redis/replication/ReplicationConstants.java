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

import java.util.HashMap;
import java.util.Map;

public class ReplicationConstants {
    // 1 second
    public static final int DEFAULT_DEQUEUE_TIMEOUT = 1;

    // dequeue and replay default batch count
    public static final int DEFAULT_BATCH_COUNT = 50;

    // Primitive type class map
    public static final Map<String, Class<?>> PRIMITIVE_CLASS_MAP = new HashMap<String, Class<?>>() {
        private static final long serialVersionUID = 1L;
        {
            put("int", Integer.TYPE);
            put("long", Long.TYPE);
            put("double", Double.TYPE);
            put("float", Float.TYPE);
            put("bool", Boolean.TYPE);
            put("char", Character.TYPE);
            put("byte", Byte.TYPE);
            put("short", Short.TYPE);
            put("void", Void.TYPE);
        }
    };
}
