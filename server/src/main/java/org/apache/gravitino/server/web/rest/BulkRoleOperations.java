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
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.List;
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
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.requests.BulkRoleCreateRequest;
import org.apache.gravitino.dto.requests.RoleCreateRequest;
import org.apache.gravitino.dto.requests.RoleNamesRequest;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST operations for bulk role mutations. */
@NameBindings.AccessControlInterfaces
@Path("/bulk/metalakes/{metalake}/roles")
public class BulkRoleOperations {

  private static final Logger LOG = LoggerFactory.getLogger(BulkRoleOperations.class);

  private static final String CREATE_ROLE_EXPRESSION = "METALAKE::OWNER || METALAKE::CREATE_ROLE";
  private static final String DELETE_ROLE_EXPRESSION = "METALAKE::OWNER || ROLE::OWNER";

  private final AccessControlDispatcher accessControlManager;

  @Context private HttpServletRequest httpRequest;

  /** Creates a new BulkRoleOperations instance. */
  public BulkRoleOperations() {
    this.accessControlManager = GravitinoEnv.getInstance().accessControlDispatcher();
  }

  /**
   * Adds roles in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk role create request.
   * @return The bulk operation result.
   */
  @POST
  @Path("/add")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-add-roles." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-add-roles", absolute = true)
  @AuthorizationExpression(expression = CREATE_ROLE_EXPRESSION)
  public Response addRoles(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkRoleCreateRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetalakeManager.checkMetalakeInUse(metalake);
            return Utils.ok(
                bulkOperation(
                    request.getRoles(),
                    roleRequest -> {
                      List<SecurableObject> securableObjects =
                          toSecurableObjects(metalake, roleRequest);
                      return accessControlManager
                          .createRole(
                              metalake,
                              roleRequest.getName(),
                              roleRequest.getProperties(),
                              securableObjects)
                          .name();
                    },
                    RoleCreateRequest::getName));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.CREATE, "", metalake, e);
    }
  }

  /**
   * Removes roles in bulk.
   *
   * @param metalake The metalake name.
   * @param request The bulk role names request.
   * @return The bulk operation result.
   */
  @POST
  @Path("/remove")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "bulk-remove-roles." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "bulk-remove-roles", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response removeRoles(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      RoleNamesRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetalakeManager.checkMetalakeInUse(metalake);
            checkRoleDeletePrivileges(metalake, request.getRoleNames());
            return Utils.ok(
                bulkOperation(
                    request.getRoleNames(),
                    name -> {
                      if (!accessControlManager.deleteRole(metalake, name)) {
                        LOG.warn("Failed to delete role {} under metalake {}", name, metalake);
                        throw new IllegalArgumentException("Role does not exist");
                      }
                      return name;
                    }));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.DELETE, "", metalake, e);
    }
  }

  private void checkRoleDeletePrivileges(String metalake, String[] roleNames) {
    for (String roleName : roleNames) {
      if (!MetadataAuthzHelper.checkAccess(
          NameIdentifierUtil.ofRole(metalake, roleName),
          Entity.EntityType.ROLE,
          DELETE_ROLE_EXPRESSION)) {
        throw new ForbiddenException(
            "No privilege to delete role %s under metalake %s", roleName, metalake);
      }
    }
  }

  private List<SecurableObject> toSecurableObjects(String metalake, RoleCreateRequest request) {
    Set<MetadataObject> metadataObjects = Sets.newHashSet();
    for (SecurableObjectDTO object : request.getSecurableObjects()) {
      MetadataObject metadataObject = MetadataObjects.parse(object.getFullName(), object.type());
      if (metadataObjects.contains(metadataObject)) {
        throw new IllegalArgumentException(
            String.format(
                "Doesn't support specifying duplicated securable objects %s type %s",
                object.fullName(), object.type()));
      }
      metadataObjects.add(metadataObject);

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

    return Arrays.stream(request.getSecurableObjects())
        .map(
            securableObjectDTO ->
                SecurableObjects.parse(
                    securableObjectDTO.fullName(),
                    securableObjectDTO.type(),
                    securableObjectDTO.privileges().stream()
                        .map(privilege -> DTOConverters.fromPrivilegeDTO((PrivilegeDTO) privilege))
                        .collect(Collectors.toList())))
        .collect(Collectors.toList());
  }
}
