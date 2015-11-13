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

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public interface ReplicaService {

    @POST
    @Path("/{appName}/{index}/syncup/start")
    void startSyncUp(@PathParam("appName") String appName, @PathParam("index") int index);

    @POST
    @Path("/{appName}/{index}/syncup/stop")
    void stopSyncUp(@PathParam("appName") String appName, @PathParam("index") int index);

    @GET
    @Path("/{appName}/{index}/connectinfo")
    String getConnectInfo(@PathParam("appName") String appName, @PathParam("index") int index);

    @GET
    @Path("/{appName}/syncedup")
    boolean isAllSyncedUp(@PathParam("appName") String appName);

    @GET
    @Path("/syncedup")
    boolean isAllSyncedUp();

    @GET
    @Path("/{appName}/{index}/syncedup")
    boolean isSyncedUp(@PathParam("appName") String appName, @PathParam("index") int index);

    @POST
    @Path("/{appName}/resyncup")
    void reSyncUp(@PathParam("appName") String appName);

    @POST
    @Path("/{appName}/stop")
    void stop(@PathParam("appName") String appName);

    @POST
    @Path("/{appName}/start")
    void start(@PathParam("appName") String appName);

}
