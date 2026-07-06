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

import static org.apache.gravitino.server.web.rest.BulkOperationUtils.bulkOperation;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.dto.requests.GroupNamesRequest;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST operations for bulk group mutations. */
@NameBindings.AccessControlInterfaces
@Path("/bulk/metalakes/{metalake}/groups")
public class BulkGroupOperations {

  private static final Logger LOG = LoggerFactory.getLogger(BulkGroupOperations.class);

  private static final String MANAGE_GROUPS_EXPRESSION =
      "METALAKE::OWNER || METALAKE::MANAGE_GROUPS";

  private final AccessControlDispatcher accessControlManager;
  private final OwnerDispatcher ownerDispatcher;

  @Context private HttpServletRequest httpRequest;

  /** Creates a new BulkGroupOperations instance. */
  public BulkGroupOperations() {
    this.accessControlManager = GravitinoEnv.getInstance().accessControlDispatcher();
    this.ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
  }

  /**
   * Adds groups in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk group names request.
   * @return The bulk operation result.
   */
  @POST
  @Path("/add")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-add-groups." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-add-groups", absolute = true)
  @AuthorizationExpression(expression = MANAGE_GROUPS_EXPRESSION)
  public Response addGroups(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      GroupNamesRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetalakeManager.checkMetalakeInUse(metalake);
            return Utils.ok(
                bulkOperation(
                    request.getGroupNames(),
                    name -> accessControlManager.addGroup(metalake, name).name()));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.ADD, "", metalake, e);
    }
  }

  /**
   * Removes groups in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk group names request.
   * @return The bulk operation result.
   */
  @POST
  @Path("/remove")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-remove-groups." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-remove-groups", absolute = true)
  @AuthorizationExpression(expression = MANAGE_GROUPS_EXPRESSION)
  public Response removeGroups(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      GroupNamesRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetalakeManager.checkMetalakeInUse(metalake);
            return Utils.ok(
                bulkOperation(
                    request.getGroupNames(),
                    name -> {
                      checkMetalakeOwner(metalake, name);
                      if (!accessControlManager.removeGroup(metalake, name)) {
                        LOG.warn("Failed to remove group {} under metalake {}", name, metalake);
                        throw new IllegalArgumentException("Group does not exist");
                      }
                      return name;
                    }));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.REMOVE, "", metalake, e);
    }
  }

  private void checkMetalakeOwner(String metalake, String group) {
    ownerDispatcher
        .getOwner(metalake, MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE))
        .ifPresent(
            owner -> {
              if (owner.type() == Owner.Type.GROUP && owner.name().equals(group)) {
                throw new IllegalArgumentException(
                    String.format(
                        "Cannot remove group %s from metalake %s because the group is the owner "
                            + "of the metalake.",
                        group, metalake));
              }
            });
  }
}
