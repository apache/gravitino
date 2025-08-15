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
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.policy.PolicyDTO;
import org.apache.gravitino.dto.requests.PoliciesAssociateRequest;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
import org.apache.gravitino.dto.responses.PolicyResponse;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.server.web.Utils;
import org.glassfish.jersey.internal.guava.Sets;
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
  @Path("{policy}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-object-policy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-object-policy", absolute = true)
  public Response getPolicyForObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @PathParam("policy") String policyName) {
    LOG.info(
        "Received get policy {} request for object type: {}, full name: {} under metalake: {}",
        policyName,
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
            Optional<PolicyEntity> policyEntity = getPolicyForObject(metalake, object, policyName);
            Optional<PolicyDTO> policyDTO =
                policyEntity.map(t -> PolicyOperations.toDTO(t, Optional.of(false)));

            MetadataObject parentObject = MetadataObjects.parent(object);
            while (!policyEntity.isPresent() && parentObject != null) {
              policyEntity = getPolicyForObject(metalake, parentObject, policyName);
              policyDTO = policyEntity.map(t -> PolicyOperations.toDTO(t, Optional.of(true)));
              parentObject = MetadataObjects.parent(parentObject);
            }

            if (!policyDTO.isPresent()) {
              LOG.warn(
                  "Policy {} not found for object type: {}, full name: {} under metalake: {}",
                  policyName,
                  type,
                  fullName,
                  metalake);
              return Utils.notFound(
                  NoSuchPolicyException.class.getSimpleName(),
                  "Policy not found: "
                      + policyName
                      + " for object type: "
                      + type
                      + ", full name: "
                      + fullName
                      + " under metalake: "
                      + metalake);
            } else {
              LOG.info(
                  "Get policy: {} for object type: {}, full name: {} under metalake: {}",
                  policyName,
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new PolicyResponse(policyDTO.get()));
            }
          });

    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.GET, policyName, fullName, e);
    }
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-object-policies." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-object-policies", absolute = true)
  public Response listPoliciesForMetadataObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
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

            Set<PolicyDTO> policies = Sets.newHashSet();
            PolicyEntity[] nonInheritedPolicies =
                policyDispatcher.listPolicyInfosForMetadataObject(metalake, object);
            if (ArrayUtils.isNotEmpty(nonInheritedPolicies)) {
              Collections.addAll(
                  policies,
                  Arrays.stream(nonInheritedPolicies)
                      .map(t -> PolicyOperations.toDTO(t, Optional.of(false)))
                      .toArray(PolicyDTO[]::new));
            }

            MetadataObject parentObject = MetadataObjects.parent(object);
            while (parentObject != null) {
              PolicyEntity[] inheritedPolicies =
                  policyDispatcher.listPolicyInfosForMetadataObject(metalake, parentObject);
              if (ArrayUtils.isNotEmpty(inheritedPolicies)) {
                Collections.addAll(
                    policies,
                    Arrays.stream(inheritedPolicies)
                        .map(t -> PolicyOperations.toDTO(t, Optional.of(true)))
                        .toArray(PolicyDTO[]::new));
              }
              parentObject = MetadataObjects.parent(parentObject);
            }

            if (verbose) {
              LOG.info(
                  "List {} policies info for object type: {}, full name: {} under metalake: {}",
                  policies.size(),
                  type,
                  fullName,
                  metalake);
              return Utils.ok(new PolicyListResponse(policies.toArray(new PolicyDTO[0])));

            } else {
              String[] policyNames = policies.stream().map(PolicyDTO::name).toArray(String[]::new);

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

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "associate-object-policies." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "associate-object-policies", absolute = true)
  public Response associatePoliciesForObject(
      @PathParam("metalake") String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      PoliciesAssociateRequest request) {
    LOG.info(
        "Received associate policies request for object type: {}, full name: {} under metalake: {}",
        type,
        fullName,
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            MetadataObject object =
                MetadataObjects.parse(
                    fullName, MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT)));
            String[] policyNames =
                policyDispatcher.associatePoliciesForMetadataObject(
                    metalake, object, request.getPoliciesToAdd(), request.getPoliciesToRemove());
            policyNames = policyNames == null ? new String[0] : policyNames;

            LOG.info(
                "Associated policies: {} for object type: {}, full name: {} under metalake: {}",
                Arrays.toString(policyNames),
                type,
                fullName,
                metalake);
            return Utils.ok(new NameListResponse(policyNames));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.ASSOCIATE, "", fullName, e);
    }
  }

  /**
   * Get the policy for the given metadata object and policy name. If the policy is not found,
   * return an empty Optional.
   *
   * @param metalake the name of the metalake
   * @param object the metadata object for which the policy is associated
   * @param policyName the name of the policy to retrieve
   * @return an Optional containing the policy if found, otherwise an empty Optional
   */
  private Optional<PolicyEntity> getPolicyForObject(
      String metalake, MetadataObject object, String policyName) {
    try {
      return Optional.ofNullable(
          policyDispatcher.getPolicyForMetadataObject(metalake, object, policyName));
    } catch (NoSuchPolicyException e) {
      LOG.info("Policy {} not found for object: {}", policyName, object);
      return Optional.empty();
    }
  }
}
