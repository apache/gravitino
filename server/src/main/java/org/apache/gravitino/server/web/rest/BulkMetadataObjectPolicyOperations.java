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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_POLICY_AUTHORIZATION_EXPRESSION;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import org.apache.gravitino.bulk.BulkManager;
import org.apache.gravitino.dto.policy.PolicyDTO;
import org.apache.gravitino.dto.requests.BulkMetadataObjectsRequest;
import org.apache.gravitino.dto.responses.BulkMetadataObjectPolicyListResponse;
import org.apache.gravitino.dto.responses.BulkMetadataObjectPolicyListResponse.Result;
import org.apache.gravitino.dto.responses.MetadataObjectPolicyListResponse;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Provides bounded, read-only bulk lookups of effective policies. */
@Path("/bulk/metalakes/{metalake}/objects/policies")
public class BulkMetadataObjectPolicyOperations {
  private final PolicyDispatcher policyDispatcher;

  @Context private HttpServletRequest httpRequest;

  /**
   * Creates the bulk policy resource.
   *
   * @param policyDispatcher the policy dispatcher
   */
  @Inject
  public BulkMetadataObjectPolicyOperations(PolicyDispatcher policyDispatcher) {
    this.policyDispatcher = policyDispatcher;
  }

  /**
   * Lists effective policies in request order, preserving the single-object API's visibility rules.
   * Registered tables and schemas use their stored identities without reloading external metadata.
   *
   * @param metalake the metalake name
   * @param request the metadata objects to load
   * @return effective policies, or an error if any requested object cannot be loaded
   */
  @POST
  @Consumes("application/json")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-object-policies-bulk." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-object-policies-bulk", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listPolicies(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      BulkMetadataObjectsRequest request) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Preconditions.checkArgument(request != null, "Request body cannot be null");
            request.validate();
            new BulkManager(GravitinoEnv.getInstance().config())
                .checkBulkSize("metadataObjects", request.getMetadataObjects().length);
            List<MetadataObject> objects =
                Arrays.stream(request.getMetadataObjects())
                    .map(object -> MetadataObjects.parse(object.fullName(), object.type()))
                    .collect(Collectors.toList());
            if (MetadataAuthzHelper.filterMetadataObject(metalake, request.getMetadataObjects())
                    .length
                != objects.size()) {
              throw new ForbiddenException(
                  "Not authorized to access one or more requested metadata objects");
            }
            Map<MetadataObject, PolicyEntity[]> policies =
                policyDispatcher.listPolicyInfosForDisplay(metalake, objects);
            List<Result> results = new ArrayList<>();
            for (int i = 0; i < objects.size(); i++) {
              PolicyEntity[] allPolicies = policies.get(objects.get(i));
              PolicyEntity[] visiblePolicies =
                  MetadataAuthzHelper.filterByExpression(
                      metalake,
                      LOAD_POLICY_AUTHORIZATION_EXPRESSION,
                      Entity.EntityType.POLICY,
                      allPolicies,
                      policy -> NameIdentifierUtil.ofPolicy(metalake, policy.name()));
              PolicyDTO[] policyDTOs =
                  Arrays.stream(visiblePolicies)
                      .map(
                          policy ->
                              PolicyOperations.toDTO(
                                  policy, Optional.of(policy.inherited().orElse(false))))
                      .toArray(PolicyDTO[]::new);
              results.add(
                  new Result(
                      request.getMetadataObjects()[i],
                      new MetadataObjectPolicyListResponse(
                          policyDTOs, visiblePolicies.length < allPolicies.length)));
            }
            return Utils.ok(
                new BulkMetadataObjectPolicyListResponse(results.toArray(new Result[0])));
          });
    } catch (ForbiddenException e) {
      return Utils.forbidden(e.getMessage(), e);
    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.LIST, "", metalake, e);
    }
  }
}
