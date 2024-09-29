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
import com.google.common.collect.Lists;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
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
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.requests.RoleCreateRequest;
import org.apache.gravitino.dto.responses.DeleteResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.RoleResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NameBindings.AccessControlInterfaces
@Path("/metalakes/{metalake}/roles")
public class RoleOperations {
  private static final Logger LOG = LoggerFactory.getLogger(RoleOperations.class);

  private final AccessControlDispatcher accessControlManager;

  @Context private HttpServletRequest httpRequest;

  public RoleOperations() {
    this.accessControlManager = GravitinoEnv.getInstance().accessControlDispatcher();
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-role", absolute = true)
  public Response listRoles(@PathParam("metalake") String metalake) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              TreeLockUtils.doWithTreeLock(
                  NameIdentifier.of(metalake),
                  LockType.READ,
                  () -> {
                    String[] names = accessControlManager.listRoleNames(metalake);
                    return Utils.ok(new NameListResponse(names));
                  }));
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.LIST, "", metalake, e);
    }
  }

  @GET
  @Path("{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-role", absolute = true)
  public Response getRole(@PathParam("metalake") String metalake, @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () ->
              TreeLockUtils.doWithTreeLock(
                  AuthorizationUtils.ofRole(metalake, role),
                  LockType.READ,
                  () ->
                      Utils.ok(
                          new RoleResponse(
                              DTOConverters.toDTO(accessControlManager.getRole(metalake, role))))));
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.GET, role, metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-role", absolute = true)
  public Response createRole(@PathParam("metalake") String metalake, RoleCreateRequest request) {
    try {

      return Utils.doAs(
          httpRequest,
          () -> {
            for (SecurableObjectDTO object : request.getSecurableObjects()) {
              for (Privilege privilege : object.privileges()) {
                if (!fromPrivilegeDTO(privilege).supportsMetadataObject(object.type())) {
                  throw new IllegalArgumentException(
                      String.format(
                          "Securable object %s type %s don't support privilege %s",
                          object.fullName(), object.type(), privilege));
                }
              }
              AuthorizationUtils.checkSecurableObject(metalake, object);
            }

            List<SecurableObject> securableObjects =
                request.getSecurableObjects() == null
                    ? Lists.newArrayList()
                    : Arrays.stream(request.getSecurableObjects())
                        .map(
                            securableObjectDTO ->
                                SecurableObjects.parse(
                                    securableObjectDTO.fullName(),
                                    securableObjectDTO.type(),
                                    securableObjectDTO.privileges().stream()
                                        .map(privilege -> fromPrivilegeDTO(privilege))
                                        .collect(Collectors.toList())))
                        .collect(Collectors.toList());

            return TreeLockUtils.doWithTreeLock(
                NameIdentifier.of(AuthorizationUtils.ofRoleNamespace(metalake).levels()),
                LockType.WRITE,
                () ->
                    Utils.ok(
                        new RoleResponse(
                            DTOConverters.toDTO(
                                accessControlManager.createRole(
                                    metalake,
                                    request.getName(),
                                    request.getProperties(),
                                    securableObjects)))));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @DELETE
  @Path("{role}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-role." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-role", absolute = true)
  public Response deleteRole(
      @PathParam("metalake") String metalake, @PathParam("role") String role) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted =
                TreeLockUtils.doWithTreeLock(
                    NameIdentifier.of(AuthorizationUtils.ofRoleNamespace(metalake).levels()),
                    LockType.WRITE,
                    () -> accessControlManager.deleteRole(metalake, role));
            if (!deleted) {
              LOG.warn("Failed to delete role {} under metalake {}", role, metalake);
            }
            return Utils.ok(new DeleteResponse(deleted));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.DELETE, role, metalake, e);
    }
  }

  private static Privilege fromPrivilegeDTO(Privilege privilegeDTO) {
    if (privilegeDTO.condition().equals(Privilege.Condition.ALLOW)) {
      return Privileges.allow(privilegeDTO.name());
    } else {
      return Privileges.deny(privilegeDTO.name());
    }
  }
}
