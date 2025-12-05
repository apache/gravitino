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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConverter.CAN_OPERATE_METADATA_PRIVILEGE;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Locale;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.requests.PrivilegeGrantRequest;
import org.apache.gravitino.dto.requests.PrivilegeRevokeRequest;
import org.apache.gravitino.dto.requests.RoleGrantRequest;
import org.apache.gravitino.dto.requests.RoleRevokeRequest;
import org.apache.gravitino.dto.responses.GroupResponse;
import org.apache.gravitino.dto.responses.RoleResponse;
import org.apache.gravitino.dto.responses.UserResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.MetadataObjectUtil;

@NameBindings.AccessControlInterfaces
@Path("/metalakes/{metalake}/permissions")
public class PermissionOperations {

  private final AccessControlDispatcher accessControlManager;

  @Context private HttpServletRequest httpRequest;

  public PermissionOperations() {
    // Because accessManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So PermissionOperations chooses to retrieve
    // accessControlManager from GravitinoEnv instead of injection here.
    this.accessControlManager = GravitinoEnv.getInstance().accessControlDispatcher();
  }

  @PUT
  @Path("users/{user}/grant/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "grant-roles-to-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "grant-roles-to-user", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::MANAGE_GRANTS")
  public Response grantRolesToUser(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("user") String user,
      RoleGrantRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new UserResponse(
                    DTOConverters.toDTO(
                        accessControlManager.grantRolesToUser(
                            metalake, request.getRoleNames(), user))));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserPermissionOperationException(
          OperationType.GRANT, StringUtils.join(request.getRoleNames(), ","), user, e);
    }
  }

  @PUT
  @Path("groups/{group}/grant/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "grant-roles-to-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "grant-roles-to-group", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::MANAGE_GRANTS")
  public Response grantRolesToGroup(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("group") String group,
      RoleGrantRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new GroupResponse(
                    DTOConverters.toDTO(
                        accessControlManager.grantRolesToGroup(
                            metalake, request.getRoleNames(), group))));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupPermissionOperationException(
          OperationType.GRANT, StringUtils.join(request.getRoleNames(), ","), group, e);
    }
  }

  @PUT
  @Path("users/{user}/revoke/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "revoke-roles-from-user." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "revoke-roles-from-user", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::MANAGE_GRANTS")
  public Response revokeRolesFromUser(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("user") String user,
      RoleRevokeRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new UserResponse(
                    DTOConverters.toDTO(
                        accessControlManager.revokeRolesFromUser(
                            metalake, request.getRoleNames(), user))));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleUserPermissionOperationException(
          OperationType.REVOKE, StringUtils.join(request.getRoleNames(), ","), user, e);
    }
  }

  @PUT
  @Path("groups/{group}/revoke")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "revoke-roles-from-group." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "revokes-role-from-group", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER || METALAKE::MANAGE_GRANTS")
  public Response revokeRolesFromGroup(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("group") String group,
      RoleRevokeRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            return Utils.ok(
                new GroupResponse(
                    DTOConverters.toDTO(
                        accessControlManager.revokeRolesFromGroup(
                            metalake, request.getRoleNames(), group))));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleGroupPermissionOperationException(
          OperationType.REVOKE, StringUtils.join(request.getRoleNames()), group, e);
    }
  }

  @PUT
  @Path("roles/{role}/{type}/{fullName}/grant/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "grant-privilege-to-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "grant-privilege-to-role", absolute = true)
  @AuthorizationExpression(
      expression = CAN_OPERATE_METADATA_PRIVILEGE,
      errorMessage = "Current user can not grant privilege to role.")
  public Response grantPrivilegeToRole(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("role") String role,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      PrivilegeGrantRequest privilegeGrantRequest) {
    try {
      MetadataObject object =
          MetadataObjects.parse(
              fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));

      return Utils.doAs(
          httpRequest,
          () -> {
            privilegeGrantRequest.validate();

            for (PrivilegeDTO privilegeDTO : privilegeGrantRequest.getPrivileges()) {
              AuthorizationUtils.checkPrivilege(privilegeDTO, object, metalake);
            }

            MetadataObjectUtil.checkMetadataObject(metalake, object);
            return Utils.ok(
                new RoleResponse(
                    DTOConverters.toDTO(
                        accessControlManager.grantPrivilegeToRole(
                            metalake,
                            role,
                            object,
                            privilegeGrantRequest.getPrivileges().stream()
                                .map(DTOConverters::fromPrivilegeDTO)
                                .collect(Collectors.toSet())))));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRolePermissionOperationException(
          OperationType.GRANT, fullName, role, e);
    }
  }

  @PUT
  @Path("roles/{role}/{type}/{fullName}/revoke/")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "revoke-privilege-from-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "revoke-privilege-from-role", absolute = true)
  @AuthorizationExpression(
      expression = CAN_OPERATE_METADATA_PRIVILEGE,
      errorMessage = "Current user can not revoke privilege from role.")
  public Response revokePrivilegeFromRole(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("role") String role,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      PrivilegeRevokeRequest privilegeRevokeRequest) {
    try {
      MetadataObject object =
          MetadataObjects.parse(
              fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));

      return Utils.doAs(
          httpRequest,
          () -> {
            privilegeRevokeRequest.validate();

            for (PrivilegeDTO privilegeDTO : privilegeRevokeRequest.getPrivileges()) {
              AuthorizationUtils.checkPrivilege(privilegeDTO, object, metalake);
            }

            MetadataObjectUtil.checkMetadataObject(metalake, object);
            return Utils.ok(
                new RoleResponse(
                    DTOConverters.toDTO(
                        accessControlManager.revokePrivilegesFromRole(
                            metalake,
                            role,
                            object,
                            privilegeRevokeRequest.getPrivileges().stream()
                                .map(DTOConverters::fromPrivilegeDTO)
                                .collect(Collectors.toSet())))));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRolePermissionOperationException(
          OperationType.REVOKE, fullName, role, e);
    }
  }
}
