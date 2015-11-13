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

public class RedisTestEnv {

    //redis conf file should be like redis_x.conf
    //sentinel conf file should be like sentinel_x.conf

    static final String REDIS_SENTINEL = "redis-sentinel";
    static final String REDIS_SERVER = "redis-server";
    static final String REDIS_CLI = "redis-cli";

    static final String LOCAL_IP = "127.0.0.1";

    private String execPrefix32 = "bin32/";
    private String execPrefix64 = "bin64/";

    private String redisSentinelExecFile;
    private String redisServerExecFile;
    private String redisCliExecFile;

    //port like 6380,6381
    protected int redisPortBase = 6380;
    //port like 26380,26381
    protected int redisSentinelPortBase = 26380;

    private RedisSUT[] redisServers;
    private RedisSUT[] redisSentinels;

    public void init() {
        if (CommandUtil.isLinuxPlatform()) {

            CommandUtil.runShell("ls -l");

            initExecFile();
            RedisExecConfig cfg = RedisExecConfig.creatConfig(redisCliExecFile, redisServerExecFile,
                    redisSentinelExecFile);

            //redisServers
            int redisServerNum = 2;
            redisServers = new RedisSUT[redisServerNum];
            for (int i = 0; i < redisServerNum; i++) {
                String conf = "redis_" + (i + 1) + ".conf";
                redisServers[i] = new RedisSUT(CommandUtil.getResourcePath(conf), LOCAL_IP, redisPortBase + i, false,
                        cfg);
                redisServers[i].start();
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            //slave-master
            redisServers[1].slavof(redisServers[0]);

            //sentinel
            int redisSentinelNum = 1;
            redisSentinels = new RedisSUT[redisSentinelNum];
            for (int i = 0; i < redisSentinelNum; i++) {
                String conf = "sentinel_" + (i + 1) + ".conf";
                redisSentinels[i] = new RedisSUT(CommandUtil.getResourcePath(conf), LOCAL_IP,
                        redisSentinelPortBase + i, true, cfg);
                redisSentinels[i].start();
            }

        } else {
            System.out.println("Pls run this CT on Linux platform");
        }

    }

    protected void initExecFile() {
        if (CommandUtil.isOS64()) {
            System.out.println("64 bit OS");
            redisSentinelExecFile = CommandUtil.getResourcePath(execPrefix64 + REDIS_SENTINEL);
            redisServerExecFile = CommandUtil.getResourcePath(execPrefix64 + REDIS_SERVER);
            redisCliExecFile = CommandUtil.getResourcePath(execPrefix64 + REDIS_CLI);
        } else {
            System.out.println("32 bit OS");
            redisSentinelExecFile = CommandUtil.getResourcePath(execPrefix32 + REDIS_SENTINEL);
            redisServerExecFile = CommandUtil.getResourcePath(execPrefix32 + REDIS_SERVER);
            redisCliExecFile = CommandUtil.getResourcePath(execPrefix32 + REDIS_CLI);
        }
    }

    public void destroy() {
        if (CommandUtil.isLinuxPlatform()) {
            CommandUtil.killRedisAll();
        }
    }

    public RedisSUT[] getRedisServers() {
        return redisServers;
    }

    public RedisSUT[] getRedisSentinels() {
        return redisSentinels;
    }

    public int getRedisPortBase() {
        return redisPortBase;
    }

    public int getRedisSentinelPortBase() {
        return redisSentinelPortBase;
    }
}
