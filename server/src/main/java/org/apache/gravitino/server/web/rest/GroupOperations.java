/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.dto.requests.GroupAddRequest;
import org.apache.gravitino.dto.responses.GroupResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NameBindings.AccessControlInterfaces
@Path("/metalakes/{metalake}/groups")
public class GroupOperations {

  private static final Logger LOG = LoggerFactory.getLogger(GroupOperations.class);

  private final AccessControlDispatcher accessControlManager;

  @Context private HttpServletRequest httpRequest;

  public GroupOperations() {
    // Because accessManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So GroupOperations chooses to retrieve
    // accessControlManager from GravitinoEnv instead of injection here.
    this.accessControlManager = GravitinoEnv.getInstance().accessControlDispatcher();
  }

  @GET
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-group", absolute = true)
  public Response getGroup(
      @PathParam("metalake") String metalake, @PathParam("group") String group) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              TreeLockUtils.doWithTreeLock(
                  AuthorizationUtils.ofGroup(metalake, group),
                  LockType.READ,
                  () ->
                      Utils.ok(
                          new GroupResponse(
                              DTOConverters.toDTO(
                                  accessControlManager.getGroup(metalake, group))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.GET, group, metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "add-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "add-group", absolute = true)
  public Response addGroup(@PathParam("metalake") String metalake, GroupAddRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              TreeLockUtils.doWithTreeLock(
                  NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
                  LockType.WRITE,
                  () ->
                      Utils.ok(
                          new GroupResponse(
                              DTOConverters.toDTO(
                                  accessControlManager.addGroup(metalake, request.getName()))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(
          OperationType.ADD, request.getName(), metalake, e);
    }
  }

  @DELETE
  @Path("{group}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "remove-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-group", absolute = true)
  public Response removeGroup(
      @PathParam("metalake") String metalake, @PathParam("group") String group) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean removed =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(AuthorizationUtils.ofGroupNamespace(metalake).levels()),
                    LockType.WRITE,
                    () -> accessControlManager.removeGroup(metalake, group));
            if (!removed) {
              LOG.warn("Failed to remove group {} under metalake {}", group, metalake);
            }
            return Utils.ok(new RemoveResponse(removed));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.REMOVE, group, metalake, e);
    }
  }
}
