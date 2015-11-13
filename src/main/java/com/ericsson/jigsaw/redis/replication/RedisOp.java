package com.ericsson.jigsaw.redis.replication;

import java.util.Arrays;
import java.util.List;

public class RedisOp {

    private String shardKey;
    private String cmd;
    private List<Object> args;
    private List<String> paramTypes;

    public RedisOp() {
    }

    public RedisOp(String shardKey, String cmd, List<Object> args, List<String> paramTypes) {
        this.shardKey = shardKey;
        this.cmd = cmd;
        this.args = args;
        this.paramTypes = paramTypes;
    }

    public String getShardKey() {
        return shardKey;
    }

    public void setShardKey(String shardKey) {
        this.shardKey = shardKey;
    }

    public String getCmd() {
        return cmd;
    }

    public void setCmd(String command) {
        this.cmd = command;
    }

    public List<Object> getArgs() {
        return args;
    }

    public void setArgs(List<Object> arguments) {
        this.args = arguments;
    }

    public List<String> getParamTypes() {
        return paramTypes;
    }

    public void setParamTypes(List<String> parameterTypes) {
        this.paramTypes = parameterTypes;
    }

    @Override
    public String toString() {
        return "Shard: " + shardKey + " Operation: " + cmd + " Arguments: " + Arrays.toString(args.toArray())
                + " ParameterTypes:" + Arrays.toString(paramTypes.toArray());
    }
}
