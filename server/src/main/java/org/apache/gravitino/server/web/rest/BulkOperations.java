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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.bulk.BulkItemResult;
import org.apache.gravitino.bulk.BulkManager;
import org.apache.gravitino.bulk.GroupAdd;
import org.apache.gravitino.bulk.UserAdd;
import org.apache.gravitino.dto.authorization.GroupDTO;
import org.apache.gravitino.dto.authorization.UserDTO;
import org.apache.gravitino.dto.requests.BulkGroupAddRequest;
import org.apache.gravitino.dto.requests.BulkRemoveRequest;
import org.apache.gravitino.dto.requests.BulkUserAddRequest;
import org.apache.gravitino.dto.responses.BulkError;
import org.apache.gravitino.dto.responses.BulkGroupResponse;
import org.apache.gravitino.dto.responses.BulkRemoveResponse;
import org.apache.gravitino.dto.responses.BulkSummary;
import org.apache.gravitino.dto.responses.BulkUserResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;

/** Provides best-effort bulk APIs for metalake access-control entities. */
@NameBindings.AccessControlInterfaces
@Path("/bulk/metalakes/{metalake}")
public class BulkOperations {

  private static final String USERS_FIELD_NAME = "users";
  private static final String GROUPS_FIELD_NAME = "groups";
  private static final String NAMES_FIELD_NAME = "names";

  private final BulkManager bulkManager;
  private final AccessControlDispatcher accessControlDispatcher;
  private final OwnerDispatcher ownerDispatcher;

  @Context private HttpServletRequest httpRequest;

  /** Creates a new bulk operations resource. */
  public BulkOperations() {
    this.bulkManager = GravitinoEnv.getInstance().bulkManager();
    this.accessControlDispatcher = GravitinoEnv.getInstance().accessControlDispatcher();
    this.ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
  }

  /**
   * Adds users in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk user add request.
   * @return The bulk user response.
   */
  @POST
  @Path("users/add")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-add-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-add-user", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::MANAGE_USERS")
  public Response addUsers(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkUserAddRequest request) {
    if (request == null) {
      return ExceptionHandlers.handleUserException(
          OperationType.ADD,
          "",
          metalake,
          new IllegalArgumentException("Request body cannot be null"));
    }

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            bulkManager.checkBulkSize(USERS_FIELD_NAME, request.getUsers().length);
            MetalakeManager.checkMetalakeInUse(metalake);
            List<BulkItemResult<User>> results =
                accessControlDispatcher.addUsers(
                    metalake,
                    Arrays.stream(request.getUsers())
                        .map(user -> new UserAdd(user.getName()))
                        .collect(Collectors.toList()));
            UserDTO[] users =
                results.stream()
                    .filter(BulkItemResult::succeeded)
                    .map(result -> DTOConverters.toDTO(result.value().get()))
                    .toArray(UserDTO[]::new);
            BulkError[] errors =
                results.stream()
                    .filter(result -> !result.succeeded())
                    .map(bulkManager::toBulkError)
                    .toArray(BulkError[]::new);
            return Utils.ok(
                new BulkUserResponse(
                    users, errors, new BulkSummary(results.size(), users.length, errors.length)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.ADD, "", metalake, e);
    }
  }

  /**
   * Removes users in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk remove request.
   * @return The bulk remove response.
   */
  @POST
  @Path("users/remove")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-remove-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-remove-user", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::MANAGE_USERS")
  public Response removeUsers(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkRemoveRequest request) {
    if (request == null) {
      return ExceptionHandlers.handleUserException(
          OperationType.REMOVE,
          "",
          metalake,
          new IllegalArgumentException("Request body cannot be null"));
    }

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            bulkManager.checkBulkSize(NAMES_FIELD_NAME, request.getNames().length);
            MetalakeManager.checkMetalakeInUse(metalake);
            Optional<Owner> metalakeOwner =
                ownerDispatcher.getOwner(
                    metalake, MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE));
            List<BulkItemResult<String>> results =
                accessControlDispatcher.removeUsers(
                    metalake, Arrays.asList(request.getNames()), metalakeOwner);
            String[] names =
                results.stream()
                    .filter(BulkItemResult::succeeded)
                    .map(BulkItemResult::name)
                    .toArray(String[]::new);
            BulkError[] errors =
                results.stream()
                    .filter(result -> !result.succeeded())
                    .map(bulkManager::toBulkError)
                    .toArray(BulkError[]::new);
            return Utils.ok(
                new BulkRemoveResponse(
                    names, errors, new BulkSummary(results.size(), names.length, errors.length)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserException(OperationType.REMOVE, "", metalake, e);
    }
  }

  /**
   * Adds groups in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk group add request.
   * @return The bulk group response.
   */
  @POST
  @Path("groups/add")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-add-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-add-group", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::MANAGE_GROUPS")
  public Response addGroups(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkGroupAddRequest request) {
    if (request == null) {
      return ExceptionHandlers.handleGroupException(
          OperationType.ADD,
          "",
          metalake,
          new IllegalArgumentException("Request body cannot be null"));
    }

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            bulkManager.checkBulkSize(GROUPS_FIELD_NAME, request.getGroups().length);
            MetalakeManager.checkMetalakeInUse(metalake);
            List<BulkItemResult<Group>> results =
                accessControlDispatcher.addGroups(
                    metalake,
                    Arrays.stream(request.getGroups())
                        .map(group -> new GroupAdd(group.getName()))
                        .collect(Collectors.toList()));
            GroupDTO[] groups =
                results.stream()
                    .filter(BulkItemResult::succeeded)
                    .map(result -> DTOConverters.toDTO(result.value().get()))
                    .toArray(GroupDTO[]::new);
            BulkError[] errors =
                results.stream()
                    .filter(result -> !result.succeeded())
                    .map(bulkManager::toBulkError)
                    .toArray(BulkError[]::new);
            return Utils.ok(
                new BulkGroupResponse(
                    groups, errors, new BulkSummary(results.size(), groups.length, errors.length)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.ADD, "", metalake, e);
    }
  }

  /**
   * Removes groups in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk remove request.
   * @return The bulk remove response.
   */
  @POST
  @Path("groups/remove")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-remove-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-remove-group", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::MANAGE_GROUPS")
  public Response removeGroups(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkRemoveRequest request) {
    if (request == null) {
      return ExceptionHandlers.handleGroupException(
          OperationType.REMOVE,
          "",
          metalake,
          new IllegalArgumentException("Request body cannot be null"));
    }

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            bulkManager.checkBulkSize(NAMES_FIELD_NAME, request.getNames().length);
            MetalakeManager.checkMetalakeInUse(metalake);
            Optional<Owner> metalakeOwner =
                ownerDispatcher.getOwner(
                    metalake, MetadataObjects.of(null, metalake, MetadataObject.Type.METALAKE));
            List<BulkItemResult<String>> results =
                accessControlDispatcher.removeGroups(
                    metalake, Arrays.asList(request.getNames()), metalakeOwner);
            String[] names =
                results.stream()
                    .filter(BulkItemResult::succeeded)
                    .map(BulkItemResult::name)
                    .toArray(String[]::new);
            BulkError[] errors =
                results.stream()
                    .filter(result -> !result.succeeded())
                    .map(bulkManager::toBulkError)
                    .toArray(BulkError[]::new);
            return Utils.ok(
                new BulkRemoveResponse(
                    names, errors, new BulkSummary(results.size(), names.length, errors.length)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupException(OperationType.REMOVE, "", metalake, e);
    }
  }
}
