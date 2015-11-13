/*------------------------------------------------------------------------------
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *----------------------------------------------------------------------------*/
package com.ericsson.jigsaw.redis.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class InfoCommandResultPropertiesTest {
    private InfoCommandResultProperties infoCmdResultProperties;
    private StringBuffer infoCmdResult = new StringBuffer();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        infoCmdResultProperties = new InfoCommandResultProperties();
        infoCmdResult.append("# Server\n");
        infoCmdResult.append("redis_version:2.8.13\n");
        infoCmdResult.append("redis_git_sha1:00000000\n");
        infoCmdResult.append("redis_git_dirty:0\n");
        infoCmdResult.append("redis_build_id:21449bb65e3d6392\n");
        infoCmdResult.append("redis_mode:standalone\n");
        infoCmdResult.append("os:Linux 2.6.32-431.40.2.el6.x86_64 x86_64\n");
        infoCmdResult.append("arch_bits:64\n");
        infoCmdResult.append("multiplexing_api:epoll\n");
        infoCmdResult.append("gcc_version:4.4.7\n");
        infoCmdResult.append("process_id:15134\n");
        infoCmdResult.append("run_id:ae97d1c0ced6cfd16f6763d0644050072c1c4bbf\n");
        infoCmdResult.append("tcp_port:6381\n");
        infoCmdResult.append("uptime_in_seconds:10797\n");
        infoCmdResult.append("uptime_in_days:0\n");
        infoCmdResult.append("hz:10\n");
        infoCmdResult.append("lru_clock:4965\n");
        infoCmdResult.append("config_file:/etc/redis/redis2.conf\n");
        infoCmdResult.append("\n");
        infoCmdResult.append("# Clients\n");
        infoCmdResult.append("connected_clients:3\n");
        infoCmdResult.append("client_longest_output_list:0\n");
        infoCmdResult.append("client_biggest_input_buf:0\n");
        infoCmdResult.append("blocked_clients:0\n");
        infoCmdResult.append("\n");
        infoCmdResult.append("# Memory\n");
        infoCmdResult.append("used_memory:1925232\n");
        infoCmdResult.append("used_memory_human:1.84M\n");
        infoCmdResult.append("used_memory_rss:8491008\n");
        infoCmdResult.append("used_memory_peak:2015720\n");
        infoCmdResult.append("used_memory_peak_human:1.92M\n");
        infoCmdResult.append("used_memory_lua:33792\n");
        infoCmdResult.append("mem_fragmentation_ratio:4.41\n");
        infoCmdResult.append("mem_allocator:jemalloc-3.6.0\n");
        infoCmdResult.append("\n");
        infoCmdResult.append("# Persistence\n");
        infoCmdResult.append("loading:0\n");
        infoCmdResult.append("rdb_changes_since_last_save:66\n");
        infoCmdResult.append("rdb_bgsave_in_progress:0\n");
        infoCmdResult.append("rdb_last_save_time:1426068277\n");
        infoCmdResult.append("rdb_last_bgsave_status:ok\n");
        infoCmdResult.append("rdb_last_bgsave_time_sec:0\n");
        infoCmdResult.append("rdb_current_bgsave_time_sec:-1\n");
        infoCmdResult.append("aof_enabled:1\n");
        infoCmdResult.append("aof_rewrite_in_progress:0\n");
        infoCmdResult.append("aof_rewrite_scheduled:0\n");
        infoCmdResult.append("aof_last_rewrite_time_sec:0\n");
        infoCmdResult.append("aof_current_rewrite_time_sec:-1\n");
        infoCmdResult.append("aof_last_bgrewrite_status:ok\n");
        infoCmdResult.append("aof_last_write_status:ok\n");
        infoCmdResult.append("aof_current_size:10732\n");
        infoCmdResult.append("aof_base_size:2559\n");
        infoCmdResult.append("aof_pending_rewrite:0\n");
        infoCmdResult.append("aof_buffer_length:0\n");
        infoCmdResult.append("aof_rewrite_buffer_length:0\n");
        infoCmdResult.append("aof_pending_bio_fsync:0\n");
        infoCmdResult.append("aof_delayed_fsync:0\n");
        infoCmdResult.append("\n");
        infoCmdResult.append("# Stats\n");
        infoCmdResult.append("total_connections_received:19\n");
        infoCmdResult.append("total_commands_processed:12849\n");
        infoCmdResult.append("instantaneous_ops_per_sec:6\n");
        infoCmdResult.append("rejected_connections:0\n");
        infoCmdResult.append("sync_full:2\n");
        infoCmdResult.append("sync_partial_ok:0\n");
        infoCmdResult.append("sync_partial_err:0\n");
        infoCmdResult.append("expired_keys:0\n");
        infoCmdResult.append("evicted_keys:0\n");
        infoCmdResult.append("keyspace_hits:920\n");
        infoCmdResult.append("keyspace_misses:0\n");
        infoCmdResult.append("pubsub_channels:0\n");
        infoCmdResult.append("pubsub_patterns:0\n");
        infoCmdResult.append("latest_fork_usec:1251\n");
        infoCmdResult.append("\n");
        infoCmdResult.append("# Replication\n");
        infoCmdResult.append("role:slave\n");
        infoCmdResult.append("master_host:192.168.0.13\n");
        infoCmdResult.append("master_port:6379\n");
        infoCmdResult.append("master_link_status:up\n");
        infoCmdResult.append("master_last_io_seconds_ago:0\n");
        infoCmdResult.append("master_sync_in_progress:0\n");
        infoCmdResult.append("slave_repl_offset:1774361\n");
        infoCmdResult.append("slave_priority:100\n");
        infoCmdResult.append("slave_read_only:1\n");
        infoCmdResult.append("connected_slaves:1\n");
        infoCmdResult.append("slave0:ip=192.168.0.14,port=6381,state=online,offset=7015,lag=0\n");
        infoCmdResult.append("master_repl_offset:7749\n");
        infoCmdResult.append("repl_backlog_active:1\n");
        infoCmdResult.append("repl_backlog_size:1048576\n");
        infoCmdResult.append("repl_backlog_first_byte_offset:2\n");
        infoCmdResult.append("repl_backlog_histlen:7748\n");
        infoCmdResult.append("\n");
        infoCmdResult.append("# CPU\n");
        infoCmdResult.append("used_cpu_sys:8.50\n");
        infoCmdResult.append("used_cpu_user:3.96\n");
        infoCmdResult.append("used_cpu_sys_children:0.01\n");
        infoCmdResult.append("used_cpu_user_children:0.00\n");
        infoCmdResult.append("\n");
        infoCmdResult.append("# Keyspace\n");
        infoCmdResult.append("db0:keys=14,expires=6,avg_ttl=0\n");
        infoCmdResult.append("\n");
        infoCmdResult.append("# Sentinel\n");
        infoCmdResult.append("sentinel_masters:1\n");
        infoCmdResult.append("sentinel_tilt:0\n");
        infoCmdResult.append("sentinel_running_scripts:0\n");
        infoCmdResult.append("sentinel_scripts_queue_length:0\n");
        infoCmdResult.append("master0:name=default0,status=ok,address=192.168.0.13:6379,slaves=2,sentinels=2\n");
    }

    @After
    public void tearDown() throws Exception {
        infoCmdResultProperties = null;
    }

    @Test
    public void testGetGroupProperties() {

        infoCmdResultProperties.loadProperties(infoCmdResult.toString());

        Map<String, String> pros = infoCmdResultProperties
                .getGroupProperties(InfoCommandResultProperties.TAG_Sentinel_Group);
        String properties = pros.get("sentinel_masters");

        assertNotNull(pros);
        assertEquals(properties, "1");

        pros = infoCmdResultProperties.getGroupProperties(InfoCommandResultProperties.TAG_Replication_Group);
        properties = pros.get("slave0");

        assertNotNull(pros);
        assertEquals(properties, "ip=192.168.0.14,port=6381,state=online,offset=7015,lag=0");
    }

    @Test
    public void testGetPropertiesNotLoad() {
        String properties = infoCmdResultProperties.getAttribute(InfoCommandResultProperties.TAG_Sentinel_Group,
                "sentinel_masters");

        assertEquals(properties, null);
    }

    @Test
    public void testGetPropertiesErrorCmdResult() {

        infoCmdResultProperties.loadProperties("(error) ERR unknown command 'efe'");

        String properties = infoCmdResultProperties.getAttribute(InfoCommandResultProperties.TAG_Sentinel_Group,
                "sentinel_masters");

        assertEquals(properties, null);
    }

    @Test
    public void testGetPropertiesNotCompleteCmdResult() {

        StringBuffer notCompletedCmdResult = new StringBuffer("# Sentinel\n");
        notCompletedCmdResult.append("sentinel_masters:");

        infoCmdResultProperties.loadProperties(notCompletedCmdResult.toString());

        String properties = infoCmdResultProperties.getAttribute(InfoCommandResultProperties.TAG_Sentinel_Group,
                "sentinel_masters");

        assertEquals(properties, null);
    }

    @Test
    public void isRreplicationCompletedTest() {
        infoCmdResultProperties.loadProperties(infoCmdResult.toString());
        assertEquals(infoCmdResultProperties.isRreplicationCompleted(), true);
    }

}
