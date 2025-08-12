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
import java.util.HashSet;
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
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.server.web.Utils;
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

            // 1. Check if the policy is directly attached to the object.
            Optional<Policy> policy = getPolicyForObject(metalake, object, policyName);
            if (policy.isPresent()) {
              LOG.info(
                  "Get policy: {} for object type: {}, full name: {} under metalake: {}",
                  policyName,
                  type,
                  fullName,
                  metalake);
              return Utils.ok(
                  new PolicyResponse(DTOConverters.toDTO(policy.get(), Optional.of(false))));
            }

            // 2. If not found, first ensure the target policy exists and is inheritable for this
            // object type.
            Policy targetPolicy = policyDispatcher.getPolicy(metalake, policyName);
            if (!targetPolicy.inheritable()
                || !targetPolicy.supportedObjectTypes().contains(object.type())) {
              return getPolicyNotFoundResponse(metalake, policyName, type, fullName);
            }

            // 3. Check for exclusive policy conflicts on the current object.
            // If an exclusive policy of the same type already exists on this object, the target
            // policy cannot be inherited.
            Policy[] currentObjectPolicies =
                policyDispatcher.listPolicyInfosForMetadataObject(metalake, object);
            if (hasConflictExclusivePolicy(currentObjectPolicies, targetPolicy)) {
              return getPolicyNotFoundResponse(metalake, policyName, type, fullName);
            }

            // 4. Traverse up the hierarchy to find the policy from a parent.
            MetadataObject parentObject = MetadataObjects.parent(object);
            while (parentObject != null) {
              if (!targetPolicy.supportedObjectTypes().contains(parentObject.type())) {
                // If the parent object type is not supported by the target policy, skip it.
                parentObject = MetadataObjects.parent(parentObject);
                continue;
              }

              Policy[] parentPolicies =
                  policyDispatcher.listPolicyInfosForMetadataObject(metalake, parentObject);

              // If another exclusive policy of the same type is found on a parent, it blocks
              // inheritance from ancestors further up.
              if (hasConflictExclusivePolicy(parentPolicies, targetPolicy)) {
                return getPolicyNotFoundResponse(metalake, policyName, type, fullName);
              }

              // Check if the target policy is attached to this parent.
              Optional<Policy> inheritedPolicy =
                  ArrayUtils.isEmpty(parentPolicies)
                      ? Optional.empty()
                      : Arrays.stream(parentPolicies)
                          .filter(p -> p.name().equals(policyName))
                          .findFirst();
              if (inheritedPolicy.isPresent()) {
                // Policy found. Return it as an inherited policy.
                LOG.info(
                    "Found policy: {} for object type: {}, full name: {} under metalake: {}",
                    policyName,
                    type,
                    fullName,
                    metalake);
                return Utils.ok(
                    new PolicyResponse(
                        DTOConverters.toDTO(inheritedPolicy.get(), Optional.of(true))));
              }

              parentObject = MetadataObjects.parent(parentObject);
            }

            // 5. If no policy is found after checking all parents, return not found.
            return getPolicyNotFoundResponse(metalake, policyName, type, fullName);
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

            Set<PolicyDTO> policies = new HashSet<>();
            Set<String> exclusivePolicyTypes = new HashSet<>();

            // 1. Get policies directly attached to the object.
            Policy[] nonInheritedPolicies =
                policyDispatcher.listPolicyInfosForMetadataObject(metalake, object);
            for (Policy policy : nonInheritedPolicies) {
              if (policy.exclusive()) {
                // Record the types of exclusive policies to handle inheritance blocking.
                exclusivePolicyTypes.add(policy.policyType());
              }
              policies.add(DTOConverters.toDTO(policy, Optional.of(false)));
            }

            // 2. Traverse up the hierarchy to collect all inheritable policies.
            MetadataObject parentObject = MetadataObjects.parent(object);
            while (parentObject != null) {
              Policy[] inheritedPolicies =
                  policyDispatcher.listPolicyInfosForMetadataObject(metalake, parentObject);
              for (Policy policy : inheritedPolicies) {
                // An inherited policy is collected only if:
                // a. It is marked as inheritable.
                // b. It supports the current object's type.
                if (!policy.supportedObjectTypes().contains(object.type())
                    || !policy.inheritable()) {
                  continue;
                }

                // c. If it's an exclusive policy, its type must not have been added by a
                //    closer-level policy (from the child or a lower-level parent). This
                //    ensures that child's exclusive policies override parent's.
                if (policy.exclusive()) {
                  if (exclusivePolicyTypes.contains(policy.policyType())) {
                    continue;
                  }
                  // Record the exclusive policy type to block any further policies of the same
                  // type from higher-level ancestors.
                  exclusivePolicyTypes.add(policy.policyType());
                }
                policies.add(DTOConverters.toDTO(policy, Optional.of(true)));
              }
              parentObject = MetadataObjects.parent(parentObject);
            }

            // 3. Format and return the response
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
   * Checks if the target policy has a conflict with an existing list of policies.
   *
   * <p>A conflict is defined as: the target policy is exclusive, and there is another exclusive
   * policy of the same type but with a different name already in the provided list.
   *
   * @param policies The list of policies to check against.
   * @param targetPolicy The policy to be checked for conflicts.
   * @return {@code true} if a conflict exists, {@code false} otherwise.
   */
  private boolean hasConflictExclusivePolicy(Policy[] policies, Policy targetPolicy) {
    if (!targetPolicy.exclusive() || ArrayUtils.isEmpty(policies)) {
      return false;
    }
    return Arrays.stream(policies)
        .anyMatch(
            p ->
                // Another exclusive policy with the same type but a different name causes a
                // conflict.
                p.exclusive()
                    && p.policyType().equalsIgnoreCase(targetPolicy.policyType())
                    && !p.name().equals(targetPolicy.name()));
  }

  private Response getPolicyNotFoundResponse(
      String metalakeName, String policyName, String objectType, String fullName) {
    LOG.warn(
        "Policy {} not found for object type: {}, full name: {} under metalake: {}",
        policyName,
        objectType,
        fullName,
        metalakeName);
    return Utils.notFound(
        NoSuchPolicyException.class.getSimpleName(),
        "Policy not found: "
            + policyName
            + " for object type: "
            + objectType
            + ", full name: "
            + fullName
            + " under metalake: "
            + metalakeName);
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
  private Optional<Policy> getPolicyForObject(
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
