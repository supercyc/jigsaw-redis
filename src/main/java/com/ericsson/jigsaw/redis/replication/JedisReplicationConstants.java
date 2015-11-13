package com.ericsson.jigsaw.redis.replication;

import java.util.HashSet;
import java.util.Set;

public class JedisReplicationConstants {

    public static final String DOUBLEWRITE_QUEUE = "dwqueue";

    public static final long MAX_RETRY_COUNT = 5L;

    public static final int BATCH_POP_SIZE = 50;

    // Default max size 1M record
    public static final long DW_QUEUE_MAX_LEN = 1000000L;
    // default interval to check queue size
    public static final int MONITOR_QUEUE_INTERVAL = 5000;

    public static final int SLAVE_SLEEP_INTERVAL = 3000;

    // error message
    public static final String OPERATION_UNSUPPORTED = "Unsupported operation!";
    public static final String OPERATION_ENQUEUE_FAILURE = "Operation log enqueue failure!";

    public static final Set<String> WRITE_IDEMPOTENT_OPERATIONS = new HashSet<String>() {
        private static final long serialVersionUID = -5241991705776410624L;
        {
            add("del");
            add("sadd");
            add("set");
            add("setex");
            add("srem");
        }
    };
}
