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

public class RedisExecConfig {
    private String redisCliExecPath;

    private String redisServerExecPath;

    private String redisSentinelExecPath;

    public static RedisExecConfig creatConfig(String redisCliExecPath, String redisServerExecPath,
            String redisSentinelExecPath) {
        CommandUtil.killRedisAll();

        if (CommandUtil.isLinuxPlatform()) {
            String cmd = "chmod +x " + redisCliExecPath;
            CommandUtil.runShell(cmd);
            cmd = "chmod +x " + redisServerExecPath;
            CommandUtil.runShell(cmd);
            cmd = "chmod +x " + redisSentinelExecPath;
            CommandUtil.runShell(cmd);
        }

        return new RedisExecConfig(redisCliExecPath, redisServerExecPath, redisSentinelExecPath);
    }

    private RedisExecConfig(String redisCliExecPath, String redisServerExecPath, String redisSentinelExecPath) {
        super();
        this.redisCliExecPath = redisCliExecPath;
        this.redisServerExecPath = redisServerExecPath;
        this.redisSentinelExecPath = redisSentinelExecPath;
    }

    public String getRedisCliExecPath() {
        return redisCliExecPath;
    }

    public String getRedisServerExecPath() {
        return redisServerExecPath;
    }

    public String getRedisSentinelExecPath() {
        return redisSentinelExecPath;
    }

}
