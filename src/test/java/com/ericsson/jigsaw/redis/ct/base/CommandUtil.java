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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

public class CommandUtil {

    public static boolean runShell(String cmd) {
        String[] cmds = new String[3];
        cmds[0] = "/bin/sh";
        cmds[1] = "-c";
        cmds[2] = cmd;

        System.out.println("shell command: ");
        System.out.println(cmd);

        try {
            Process process = Runtime.getRuntime().exec(cmds);
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            process.waitFor();

            br.close();
            Thread.sleep(1000);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static boolean isOS64() {
        String[] cmds = new String[3];
        cmds[0] = "/bin/sh";
        cmds[1] = "-c";
        cmds[2] = "getconf LONG_BIT";
        boolean is64 = false;

        try {
            Process process = Runtime.getRuntime().exec(cmds);
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                if (line.contains("64")) {
                    is64 = true;
                }
            }
            process.waitFor();

            br.close();
            Thread.sleep(1000);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

        return is64;
    }

    public static boolean isLinuxPlatform() {
        String os = System.getProperty("os.name");
        return os.contains("Linux");
    }

    public static void killRedisAll() {

        String cmd = "ps -ef | grep redis | grep -v grep | grep -v java | awk '{print $2}' | xargs kill -9";
        runShell(cmd);

    }

    public static String getResourcePath(String fileName) {
        URL url = Thread.currentThread().getContextClassLoader().getResource(fileName);
        return url.getPath();
    }
}
