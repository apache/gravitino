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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.CAN_ACCESS_METADATA;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.policy.PolicyDTO;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationFullName;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.AuthorizationObjectType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/objects/{type}/{fullName}/policies")
public class MetadataObjectPolicyOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataObjectPolicyOperations.class);

  private final PolicyDispatcher policyDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetadataObjectPolicyOperations(PolicyDispatcher policyDispatcher) {
    this.policyDispatcher = policyDispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-object-policies." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-object-policies", absolute = true)
  @AuthorizationExpression(expression = CAN_ACCESS_METADATA)
  public Response listPoliciesForMetadataObject(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("type") @AuthorizationObjectType String type,
      @PathParam("fullName") @AuthorizationFullName String fullName,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    LOG.info(
        "Received list policy {} request for object type: {}, full name: {} under metalake: {}",
        verbose ? "infos" : "names",
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

            PolicyEntity[] policies =
                policyDispatcher.listPolicyInfosForMetadataObject(metalake, object);
            policies =
                MetadataAuthzHelper.filterByExpression(
                    metalake,
                    AuthorizationExpressionConstants.LOAD_POLICY_AUTHORIZATION_EXPRESSION,
                    Entity.EntityType.POLICY,
                    policies,
                    (policyEntity -> NameIdentifierUtil.ofPolicy(metalake, policyEntity.name())));

            if (verbose) {
              PolicyDTO[] policyDTOs =
                  Arrays.stream(policies)
                      .map(policy -> PolicyOperations.toDTO(policy, Optional.empty()))
                      .toArray(PolicyDTO[]::new);
              LOG.info(
                  "List {} policies info for object type: {}, full name: {} under metalake: {}",
                  policyDTOs.length,
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new PolicyListResponse(policyDTOs));

            } else {
              String[] policyNames =
                  Arrays.stream(policies).map(PolicyEntity::name).toArray(String[]::new);

              LOG.info(
                  "List {} policies for object type: {}, full name: {} under metalake: {}",
                  policyNames.length,
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new NameListResponse(policyNames));
            }
          });

    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.LIST, "", fullName, e);
    }
  }
}
