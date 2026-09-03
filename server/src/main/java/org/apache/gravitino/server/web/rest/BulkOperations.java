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
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.bulk.BulkItemResult;
import org.apache.gravitino.bulk.BulkManager;
import org.apache.gravitino.bulk.GroupAdd;
import org.apache.gravitino.bulk.RoleAdd;
import org.apache.gravitino.bulk.UserAdd;
import org.apache.gravitino.dto.authorization.GroupDTO;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.authorization.RoleDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.authorization.UserDTO;
import org.apache.gravitino.dto.requests.BulkGroupAddRequest;
import org.apache.gravitino.dto.requests.BulkRemoveRequest;
import org.apache.gravitino.dto.requests.BulkRoleAddRequest;
import org.apache.gravitino.dto.requests.BulkUserAddRequest;
import org.apache.gravitino.dto.requests.RoleCreateRequest;
import org.apache.gravitino.dto.responses.BulkError;
import org.apache.gravitino.dto.responses.BulkGroupResponse;
import org.apache.gravitino.dto.responses.BulkRemoveResponse;
import org.apache.gravitino.dto.responses.BulkRoleResponse;
import org.apache.gravitino.dto.responses.BulkSummary;
import org.apache.gravitino.dto.responses.BulkUserResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.IllegalMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

/** Provides best-effort bulk APIs for metalake access-control entities. */
@NameBindings.AccessControlInterfaces
@Path("/bulk/metalakes/{metalake}")
public class BulkOperations {

  private static final String USERS_FIELD_NAME = "users";
  private static final String GROUPS_FIELD_NAME = "groups";
  private static final String ROLES_FIELD_NAME = "roles";
  private static final String NAMES_FIELD_NAME = "names";
  private static final String DELETE_ROLE_AUTHORIZATION_EXPRESSION =
      "METALAKE::OWNER || ROLE::OWNER";

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

  /**
   * Adds roles in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk role add request.
   * @return The bulk role response.
   */
  @POST
  @Path("roles/add")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-add-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-add-role", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::CREATE_ROLE")
  public Response addRoles(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkRoleAddRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            bulkManager.checkBulkSize(ROLES_FIELD_NAME, request.getRoles().length);
            MetalakeManager.checkMetalakeInUse(metalake);

            List<RoleAdd> roles = new ArrayList<>();
            List<Integer> originalIndexes = new ArrayList<>();
            List<BulkItemResult<Role>> results = new ArrayList<>();
            for (int index = 0; index < request.getRoles().length; index++) {
              try {
                roles.add(toRoleAdd(metalake, request.getRoles()[index]));
                originalIndexes.add(index);
              } catch (Exception e) {
                results.add(BulkItemResult.failure(index, request.getRoles()[index].getName(), e));
              }
            }
            results.addAll(
                remapRoleResults(
                    accessControlDispatcher.createRoles(metalake, roles), originalIndexes));
            results.sort(Comparator.comparingInt(BulkItemResult::index));

            RoleDTO[] rolesResponse =
                results.stream()
                    .filter(BulkItemResult::succeeded)
                    .map(result -> DTOConverters.toDTO(result.value().get()))
                    .toArray(RoleDTO[]::new);
            BulkError[] errors =
                results.stream()
                    .filter(result -> !result.succeeded())
                    .map(bulkManager::toBulkError)
                    .toArray(BulkError[]::new);
            return Utils.ok(
                new BulkRoleResponse(
                    rolesResponse,
                    errors,
                    new BulkSummary(results.size(), rolesResponse.length, errors.length)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.CREATE, "", metalake, e);
    }
  }

  /**
   * Removes roles in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk remove request.
   * @return The bulk remove response.
   */
  @POST
  @Path("roles/remove")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-remove-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-remove-role", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response removeRoles(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkRemoveRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            bulkManager.checkBulkSize(NAMES_FIELD_NAME, request.getNames().length);
            MetalakeManager.checkMetalakeInUse(metalake);

            List<String> roles = new ArrayList<>();
            List<Integer> originalIndexes = new ArrayList<>();
            List<BulkItemResult<String>> results = new ArrayList<>();
            for (int index = 0; index < request.getNames().length; index++) {
              String role = request.getNames()[index];
              try {
                checkDeleteRoleAuthorization(metalake, role);
                roles.add(role);
                originalIndexes.add(index);
              } catch (Exception e) {
                results.add(BulkItemResult.failure(index, role, e));
              }
            }
            results.addAll(
                remapStringResults(
                    accessControlDispatcher.deleteRoles(metalake, roles), originalIndexes));
            results.sort(Comparator.comparingInt(BulkItemResult::index));

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
      return ExceptionHandlers.handleRoleException(OperationType.DELETE, "", metalake, e);
    }
  }

  private RoleAdd toRoleAdd(String metalake, RoleCreateRequest request) {
    Set<MetadataObject> metadataObjects = Sets.newHashSet();
    for (SecurableObjectDTO object : request.getSecurableObjects()) {
      MetadataObject metadataObject = MetadataObjects.parse(object.getFullName(), object.type());
      if (metadataObjects.contains(metadataObject)) {
        throw new IllegalArgumentException(
            String.format(
                "Doesn't support specifying duplicated securable objects %s type %s",
                object.fullName(), object.type()));
      } else {
        metadataObjects.add(metadataObject);
      }

      Set<Privilege> privileges = Sets.newHashSet(object.privileges());
      AuthorizationUtils.checkDuplicatedNamePrivilege(privileges);
      try {
        for (Privilege privilege : object.privileges()) {
          AuthorizationUtils.checkPrivilege((PrivilegeDTO) privilege, object, metalake);
        }
        MetadataObjectUtil.checkMetadataObject(metalake, object);
      } catch (NoSuchMetadataObjectException nsm) {
        throw new IllegalMetadataObjectException(nsm);
      }
    }

    List<SecurableObject> securableObjects =
        Arrays.stream(request.getSecurableObjects())
            .map(
                securableObjectDTO ->
                    SecurableObjects.parse(
                        securableObjectDTO.fullName(),
                        securableObjectDTO.type(),
                        securableObjectDTO.privileges().stream()
                            .map(
                                privilege ->
                                    DTOConverters.fromPrivilegeDTO((PrivilegeDTO) privilege))
                            .collect(Collectors.toList())))
            .collect(Collectors.toList());
    return new RoleAdd(request.getName(), request.getProperties(), securableObjects);
  }

  private void checkDeleteRoleAuthorization(String metalake, String role) {
    boolean allowed =
        MetadataAuthzHelper.checkAccess(
            NameIdentifierUtil.ofRole(metalake, role),
            Entity.EntityType.ROLE,
            DELETE_ROLE_AUTHORIZATION_EXPRESSION);
    if (!allowed) {
      throw new ForbiddenException(
          "User '%s' is not authorized to perform operation '%s' on metadata '%s' with expression '%s'",
          PrincipalUtils.getCurrentUserName(),
          "removeRoles",
          NameIdentifierUtil.ofRole(metalake, role),
          DELETE_ROLE_AUTHORIZATION_EXPRESSION);
    }
  }

  private List<BulkItemResult<Role>> remapRoleResults(
      List<BulkItemResult<Role>> results, List<Integer> originalIndexes) {
    return results.stream()
        .map(
            result ->
                result.succeeded()
                    ? BulkItemResult.success(
                        originalIndexes.get(result.index()), result.name(), result.value().get())
                    : BulkItemResult.<Role>failure(
                        originalIndexes.get(result.index()), result.name(), result.error().get()))
        .collect(Collectors.toList());
  }

  private List<BulkItemResult<String>> remapStringResults(
      List<BulkItemResult<String>> results, List<Integer> originalIndexes) {
    return results.stream()
        .map(
            result ->
                result.succeeded()
                    ? BulkItemResult.<String>success(
                        originalIndexes.get(result.index()), result.name())
                    : BulkItemResult.<String>failure(
                        originalIndexes.get(result.index()), result.name(), result.error().get()))
        .collect(Collectors.toList());
  }
}
