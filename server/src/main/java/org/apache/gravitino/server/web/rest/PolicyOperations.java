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

import static org.apache.gravitino.dto.util.DTOConverters.fromDTO;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.dto.policy.PolicyDTO;
import org.apache.gravitino.dto.requests.PolicyCreateRequest;
import org.apache.gravitino.dto.requests.PolicySetRequest;
import org.apache.gravitino.dto.requests.PolicyUpdateRequest;
import org.apache.gravitino.dto.requests.PolicyUpdatesRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.MetadataObjectListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.PolicyListResponse;
import org.apache.gravitino.dto.responses.PolicyResponse;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyChange;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/policies")
public class PolicyOperations {

  private static final Logger LOG = LoggerFactory.getLogger(PolicyOperations.class);

  private final PolicyDispatcher policyDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public PolicyOperations(PolicyDispatcher policyDispatcher) {
    this.policyDispatcher = policyDispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-policies." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-policies", absolute = true)
  public Response listPolicies(
      @PathParam("metalake") String metalake,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    LOG.info(
        "Received list policy {} request for metalake: {}", verbose ? "infos" : "names", metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (verbose) {
              PolicyEntity[] policies = policyDispatcher.listPolicyInfos(metalake);
              PolicyDTO[] policyDTOs =
                  Arrays.stream(policies)
                      .map(p -> toDTO(p, Optional.empty()))
                      .toArray(PolicyDTO[]::new);

              LOG.info("List {} policies info under metalake: {}", policyDTOs.length, metalake);
              return Utils.ok(new PolicyListResponse(policyDTOs));

            } else {
              String[] policyNames = policyDispatcher.listPolicies(metalake);
              policyNames = policyNames == null ? new String[0] : policyNames;

              LOG.info("List {} policies under metalake: {}", policyNames.length, metalake);
              return Utils.ok(new NameListResponse(policyNames));
            }
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.LIST, "", metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-policy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-policy", absolute = true)
  public Response createPolicy(
      @PathParam("metalake") String metalake, PolicyCreateRequest request) {
    LOG.info("Received create policy request under metalake: {}", metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            PolicyEntity policy =
                policyDispatcher.createPolicy(
                    metalake,
                    request.getName(),
                    Policy.BuiltInType.fromPolicyType(request.getPolicyType()),
                    request.getComment(),
                    request.getEnabled(),
                    fromDTO(request.getPolicyContent()));

            LOG.info("Created policy: {} under metalake: {}", policy.name(), metalake);
            return Utils.ok(new PolicyResponse(toDTO(policy, Optional.empty())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @GET
  @Path("{policy}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-policy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-policy", absolute = true)
  public Response getPolicy(
      @PathParam("metalake") String metalake, @PathParam("policy") String name) {
    LOG.info("Received get policy request for policy: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            PolicyEntity policy = policyDispatcher.getPolicy(metalake, name);
            LOG.info("Get policy: {} under metalake: {}", name, metalake);
            return Utils.ok(new PolicyResponse(toDTO(policy, Optional.empty())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.GET, name, metalake, e);
    }
  }

  @PUT
  @Path("{policy}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-policy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-policy", absolute = true)
  public Response alterPolicy(
      @PathParam("metalake") String metalake,
      @PathParam("policy") String name,
      PolicyUpdatesRequest request) {
    LOG.info("Received alter policy request for policy: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();

            PolicyChange[] changes =
                request.getUpdates().stream()
                    .map(PolicyUpdateRequest::policyChange)
                    .toArray(PolicyChange[]::new);
            PolicyEntity policy = policyDispatcher.alterPolicy(metalake, name, changes);

            LOG.info("Altered policy: {} under metalake: {}", name, metalake);
            return Utils.ok(new PolicyResponse(toDTO(policy, Optional.empty())));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.ALTER, name, metalake, e);
    }
  }

  @PATCH
  @Path("{policy}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "set-policy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "set-policy", absolute = true)
  public Response setPolicy(
      @PathParam("metalake") String metalake,
      @PathParam("policy") String name,
      PolicySetRequest request) {
    LOG.info("Received set policy request for policy: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (request.isEnable()) {
              policyDispatcher.enablePolicy(metalake, name);
            } else {
              policyDispatcher.disablePolicy(metalake, name);
            }

            Response response = Utils.ok(new BaseResponse());
            LOG.info(
                "Successfully {} policy: {} under metalake: {}",
                request.isEnable() ? "enabled" : "disabled",
                name,
                metalake);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.ENABLE, name, metalake, e);
    }
  }

  @DELETE
  @Path("{policy}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "delete-policy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-policy", absolute = true)
  public Response deletePolicy(
      @PathParam("metalake") String metalake, @PathParam("policy") String name) {
    LOG.info("Received delete policy request for policy: {} under metalake: {}", name, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = policyDispatcher.deletePolicy(metalake, name);
            if (!deleted) {
              LOG.warn("Cannot find to be deleted policy {} under metalake {}", name, metalake);
            } else {
              LOG.info("Deleted policy: {} under metalake: {}", name, metalake);
            }

            return Utils.ok(new DropResponse(deleted));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.DELETE, name, metalake, e);
    }
  }

  @GET
  @Path("{policy}/objects")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-objects-for-policy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-objects-for-policy", absolute = true)
  public Response listMetadataObjectsForPolicy(
      @PathParam("metalake") String metalake, @PathParam("policy") String policyName) {
    LOG.info("Received list objects for policy: {} under metalake: {}", policyName, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject[] objects =
                policyDispatcher.listMetadataObjectsForPolicy(metalake, policyName);
            objects = objects == null ? new MetadataObject[0] : objects;

            LOG.info(
                "List {} objects for policy: {} under metalake: {}",
                objects.length,
                policyName,
                metalake);

            MetadataObjectDTO[] objectDTOs =
                Arrays.stream(objects).map(DTOConverters::toDTO).toArray(MetadataObjectDTO[]::new);
            return Utils.ok(new MetadataObjectListResponse(objectDTOs));
          });

    } catch (Exception e) {
      return ExceptionHandlers.handlePolicyException(OperationType.LIST, "", metalake, e);
    }
  }

  static PolicyDTO toDTO(PolicyEntity policy, Optional<Boolean> inherited) {
    PolicyDTO.Builder builder =
        PolicyDTO.builder()
            .withName(policy.name())
            .withComment(policy.comment())
            .withPolicyType(policy.policyType().name().toLowerCase(Locale.ROOT))
            .withEnabled(policy.enabled())
            .withContent(DTOConverters.toDTO(policy.content()))
            .withInherited(inherited)
            .withAudit(DTOConverters.toDTO(policy.auditInfo()))
            .withInherited(inherited);

    return builder.build();
  }
}
