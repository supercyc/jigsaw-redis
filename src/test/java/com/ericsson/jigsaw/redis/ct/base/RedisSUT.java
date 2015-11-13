/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.ct.base;

public class RedisSUT {
    private String confFilePath;

    private String ipStr = "127.0.0.1";

    private int port = 6379;

    private boolean isSentinel;

    private RedisExecConfig redisEnv;

    public RedisSUT(String confFilePath, String ipStr, int port, boolean isSentinel, RedisExecConfig env) {
        super();
        this.confFilePath = confFilePath;
        this.ipStr = ipStr;
        this.port = port;
        this.isSentinel = isSentinel;
        this.redisEnv = env;
    }

    public boolean start() {
        String cmd;
        if (this.isSentinel) {
            cmd = redisEnv.getRedisSentinelExecPath() + " " + this.confFilePath + " > redis_sentinel.log &";
        } else {
            cmd = redisEnv.getRedisServerExecPath() + " " + this.confFilePath;
        }
        return CommandUtil.runShell(cmd);
    }

    public boolean stop() {
        String cmd = "ps -ef | grep " + this.confFilePath
                + " | grep -v grep | grep -v java | awk '{print $2}' | xargs kill -9";
        return CommandUtil.runShell(cmd);
    }

    public boolean cli(String cmd) {
        String prefix = redisEnv.getRedisCliExecPath() + " -h " + this.ipStr + " -p " + String.valueOf(this.port) + " ";
        return CommandUtil.runShell(prefix + cmd);
    }

    public boolean slavof(RedisSUT master) {
        String cmd = "slaveof ";
        if (master != null) {
            cmd = cmd + master.getIpStr() + " " + String.valueOf(master.getPort());
        } else {
            cmd = cmd + " no one";
        }
        return cli(cmd);
    }

    public String getConfFilePath() {
        return confFilePath;
    }

    public String getIpStr() {
        return ipStr;
    }

    public int getPort() {
        return port;
    }

    public boolean isSentinel() {
        return isSentinel;
    }
}
