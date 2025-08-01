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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConverter.CAN_SET_OWNER;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Locale;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.dto.requests.OwnerSetRequest;
import org.apache.gravitino.dto.responses.OwnerResponse;
import org.apache.gravitino.dto.responses.SetResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.NameBindings;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.MetadataObjectUtil;

@NameBindings.AccessControlInterfaces
@Path("/metalakes/{metalake}/owners")
public class OwnerOperations {

  private final OwnerDispatcher ownerDispatcher;

  @Context private HttpServletRequest httpRequest;

  public OwnerOperations() {
    // Because ownerManager may be null when Gravitino doesn't enable authorization,
    // and Jersey injection doesn't support null value. So OwnerOperations chooses to retrieve
    // ownerManager from GravitinoEnv instead of injection here.
    this.ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
  }

  @GET
  @Path("{metadataObjectType}/{fullName}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-object-owner." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-object-owner", absolute = true)
  public Response getOwnerForObject(
      @PathParam("metalake") String metalake,
      @PathParam("metadataObjectType") String metadataObjectType,
      @PathParam("fullName") String fullName) {
    try {
      MetadataObject object =
          MetadataObjects.parse(
              fullName, MetadataObject.Type.valueOf(metadataObjectType.toUpperCase(Locale.ROOT)));
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObjectUtil.checkMetadataObject(metalake, object);
            Optional<Owner> owner = ownerDispatcher.getOwner(metalake, object);
            if (owner.isPresent()) {
              return Utils.ok(new OwnerResponse(DTOConverters.toDTO(owner.get())));
            } else {
              return Utils.ok(new OwnerResponse(null));
            }
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleOwnerException(
          OperationType.GET, String.format("metadata object %s", fullName), metalake, e);
    }
  }

  @PUT
  @Path("{metadataObjectType}/{fullName}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "set-object-owner." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "set-object-owner", absolute = true)
  @AuthorizationExpression(
      expression = CAN_SET_OWNER,
      errorMessage = "Current user can not set this objects's owner")
  public Response setOwnerForObject(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("metadataObjectType") String metadataObjectType,
      @PathParam("fullName") String fullName,
      OwnerSetRequest request) {
    try {
      MetadataObject object =
          MetadataObjects.parse(
              fullName, MetadataObject.Type.valueOf(metadataObjectType.toUpperCase(Locale.ROOT)));

      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetadataObjectUtil.checkMetadataObject(metalake, object);
            ownerDispatcher.setOwner(metalake, object, request.getName(), request.getType());
            return Utils.ok(new SetResponse(true));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleOwnerException(
          OperationType.SET, String.format("metadata object %s", fullName), metalake, e);
    }
  }
}
