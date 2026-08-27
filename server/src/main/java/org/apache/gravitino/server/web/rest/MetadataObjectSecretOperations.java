/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.NotSupportedException;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.responses.SecretsResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.secret.SecretPropertyOperationDispatcher;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationFullName;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.AuthorizationObjectType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/objects/{type}/{fullName}/secrets")
public class MetadataObjectSecretOperations {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataObjectSecretOperations.class);

  private static final Set<MetadataObject.Type> supportsSecretMetadataTypes =
      ImmutableSet.of(
          MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.FILESET);

  private final SecretPropertyOperationDispatcher secretPropertyOperationDispatcher;

  @SuppressWarnings("unused")
  @Context
  private HttpServletRequest httpRequest;

  @Inject
  public MetadataObjectSecretOperations(SecretPropertyOperationDispatcher dispatcher) {
    this.secretPropertyOperationDispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-secrets." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-secrets", absolute = true)
  @AuthorizationExpression(expression = AuthorizationExpressionConstants.CAN_ACCESS_METADATA)
  public Response getSecrets(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("type") @AuthorizationObjectType String type,
      @PathParam("fullName") @AuthorizationFullName String fullName) {
    LOG.info(
        "Received get secrets request for object type: {}, full name: {} under metalake: {}",
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            if (!supportsSecretOperations(object)) {
              throw new NotSupportedException(
                  "Doesn't support secret property operations for metadata object type");
            }

            NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, object);
            Entity.EntityType entityType = MetadataObjectUtil.toEntityType(object);
            Map<String, String> secrets =
                secretPropertyOperationDispatcher.getSecrets(identifier, entityType);
            return Utils.ok(new SecretsResponse(secrets));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSecretException(OperationType.GET, fullName, e);
    }
  }

  private static boolean supportsSecretOperations(MetadataObject metadataObject) {
    return supportsSecretMetadataTypes.contains(metadataObject.type());
  }
}
