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
import java.util.Locale;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
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
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataFilterHelper;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;

@NameBindings.AccessControlInterfaces
@Path("/metalakes/{metalake}/objects/{type}/{fullName}/roles")
public class MetadataObjectRoleOperations {

  private final AccessControlDispatcher accessControlDispatcher;

  @Context private HttpServletRequest httpRequest;

  private static final String LIST_ROLE_PRIVILEGE = "METALAKE::OWNER || ROLE::OWNER || ROLE::SELF";

  public MetadataObjectRoleOperations() {
    // Because accessControlManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So MedataObjectRoleOperations chooses to
    // retrieve
    // accessControlDispatcher from GravitinoEnv instead of injection here.
    this.accessControlDispatcher = GravitinoEnv.getInstance().accessControlDispatcher();
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-role-by-object." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-role-by-object", absolute = true)
  public Response listRoles(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName) {
    try {
      MetadataObject object =
          MetadataObjects.parse(
              fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));

      return Utils.doAs(
          httpRequest,
          () -> {
            String[] names = accessControlDispatcher.listRoleNamesByObject(metalake, object);
            names =
                MetadataFilterHelper.filterByExpression(
                    metalake,
                    LIST_ROLE_PRIVILEGE,
                    Entity.EntityType.ROLE,
                    names,
                    (roleName) -> NameIdentifierUtil.ofRole(metalake, roleName));
            return Utils.ok(new NameListResponse(names));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleRoleException(OperationType.LIST, "", metalake, e);
    }
  }
}
