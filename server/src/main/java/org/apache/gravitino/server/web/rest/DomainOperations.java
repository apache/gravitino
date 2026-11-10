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
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import org.apache.gravitino.Entity;
import org.apache.gravitino.domain.Domain;
import org.apache.gravitino.domain.DomainChange;
import org.apache.gravitino.domain.DomainDispatcher;
import org.apache.gravitino.dto.domain.DomainDTO;
import org.apache.gravitino.dto.requests.DomainBackingTagSetRequest;
import org.apache.gravitino.dto.requests.DomainCreateRequest;
import org.apache.gravitino.dto.requests.DomainParentSetRequest;
import org.apache.gravitino.dto.requests.DomainSetRequest;
import org.apache.gravitino.dto.requests.DomainUpdateRequest;
import org.apache.gravitino.dto.requests.DomainUpdatesRequest;
import org.apache.gravitino.dto.responses.DomainListResponse;
import org.apache.gravitino.dto.responses.DomainResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.NoSuchDomainException;
import org.apache.gravitino.meta.DomainEntity;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.server.authorization.DomainAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST operations and authorization checks for domains. */
@Path("/metalakes/{metalake}/domains")
@Produces("application/vnd.gravitino.v1+json")
public class DomainOperations {

  private static final Logger LOG = LoggerFactory.getLogger(DomainOperations.class);
  private final DomainDispatcher domainDispatcher;
  @Context private HttpServletRequest httpRequest;

  /**
   * Creates the domain resource.
   *
   * @param domainDispatcher The domain dispatcher.
   */
  @Inject
  public DomainOperations(DomainDispatcher domainDispatcher) {
    this.domainDispatcher = domainDispatcher;
  }

  /**
   * Lists the domain names or snapshots in a metalake.
   *
   * @param metalake The metalake name.
   * @param details Whether to include domain snapshots.
   * @return The REST response.
   */
  @GET
  @Timed(name = "list-domains." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-domains", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listDomains(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @QueryParam("details") @DefaultValue("false") boolean details) {
    return execute(
        OperationType.LIST,
        metalake,
        "",
        () -> {
          return details
              ? listResponse(
                  DomainAuthzHelper.filterDomains(
                      metalake, domainDispatcher.listDomainInfos(metalake)))
              : Utils.ok(
                  new NameListResponse(
                      DomainAuthzHelper.enabled()
                          ? Arrays.stream(
                                  DomainAuthzHelper.filterDomains(
                                      metalake, domainDispatcher.listDomainInfos(metalake)))
                              .map(DomainEntity::name)
                              .toArray(String[]::new)
                          : domainDispatcher.listDomains(metalake)));
        });
  }

  /**
   * Creates a draft domain and its optional parent edge atomically.
   *
   * @param metalake The metalake name.
   * @param request The requested domain change.
   * @return The REST response.
   */
  @POST
  @Consumes("application/json")
  @Timed(name = "create-domain." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-domain", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response createDomain(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      DomainCreateRequest request) {
    return execute(
        OperationType.CREATE,
        metalake,
        "",
        () -> {
          validateRequest(request);
          DomainCreateRequest.BackingTag tag = request.getBackingTag();
          DomainAuthzHelper.requireManage(metalake, null);
          if (request.getParent() != null) {
            DomainAuthzHelper.requireManage(domainDispatcher, metalake, request.getParent());
          }
          if (tag.isCreate()) {
            DomainAuthzHelper.requireBackingTag(metalake, tag.getName(), true);
          }
          DomainDispatcher checked =
              DomainAuthzHelper.checked(
                  domainDispatcher,
                  state -> {
                    DomainAuthzHelper.requireManage(metalake, null);
                    state
                        .parent()
                        .ifPresent(parent -> DomainAuthzHelper.requireManage(metalake, parent));
                    DomainAuthzHelper.requireBackingTag(metalake, tag.getName(), tag.isCreate());
                  });
          DomainEntity created =
              request.getParent() == null
                  ? checked.createDomain(
                      metalake,
                      request.getName(),
                      request.getComment(),
                      request.getProperties(),
                      tag.tagValue(),
                      tag.isCreate())
                  : checked.createSubdomain(
                      metalake,
                      request.getParent(),
                      request.getName(),
                      request.getComment(),
                      request.getProperties(),
                      tag.tagValue(),
                      tag.isCreate());
          return domainResponse(created);
        });
  }

  /**
   * Gets a domain snapshot.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @return The REST response.
   */
  @GET
  @Path("{domain}")
  @Timed(name = "get-domain." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-domain", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response getDomain(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain) {
    return execute(
        OperationType.GET,
        metalake,
        domain,
        () -> {
          DomainEntity loaded = domainDispatcher.getDomain(metalake, domain);
          DomainAuthzHelper.requireVisible(metalake, loaded);
          return domainResponse(loaded);
        });
  }

  /**
   * Applies name, comment and property updates to a domain.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @param request The requested domain change.
   * @return The REST response.
   */
  @PUT
  @Path("{domain}")
  @Consumes("application/json")
  @Timed(name = "alter-domain." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-domain", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response alterDomain(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain,
      DomainUpdatesRequest request) {
    return execute(
        OperationType.ALTER,
        metalake,
        domain,
        () -> {
          validateRequest(request);
          DomainChange[] changes =
              request.getUpdates().stream()
                  .map(DomainUpdateRequest::domainChange)
                  .toArray(DomainChange[]::new);
          return domainResponse(
              DomainAuthzHelper.managed(domainDispatcher, metalake)
                  .alterDomain(metalake, domain, changes));
        });
  }

  /**
   * Publishes or unpublishes a domain and returns the updated snapshot.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @param request The requested domain change.
   * @return The REST response.
   */
  @PATCH
  @Path("{domain}")
  @Consumes("application/json")
  @Timed(name = "set-domain-status." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "set-domain-status", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response setDomain(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain,
      DomainSetRequest request) {
    return execute(
        OperationType.ALTER,
        metalake,
        domain,
        () -> {
          validateRequest(request);
          DomainDispatcher checked = DomainAuthzHelper.managed(domainDispatcher, metalake);
          return domainResponse(
              request.getStatus() == Domain.Status.PUBLISHED
                  ? checked.publishDomain(metalake, domain)
                  : checked.unpublishDomain(metalake, domain));
        });
  }

  /**
   * Deletes a domain, returning false when it is absent.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @return The REST response.
   */
  @DELETE
  @Path("{domain}")
  @Timed(name = "delete-domain." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-domain", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response deleteDomain(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain) {
    return execute(
        OperationType.DELETE,
        metalake,
        domain,
        () -> {
          boolean deleted;
          try {
            deleted =
                DomainAuthzHelper.managed(domainDispatcher, metalake)
                    .deleteDomain(metalake, domain);
          } catch (NoSuchDomainException e) {
            // Hidden drafts have the same delete response as absent domains. The failed check
            // aborts the store transaction before any domain is deleted.
            deleted = false;
          }
          return Utils.ok(new DropResponse(deleted));
        });
  }

  /**
   * Attaches or moves a domain under a top-level parent.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @param request The requested domain change.
   * @return The REST response.
   */
  @PUT
  @Path("{domain}/parent")
  @Consumes("application/json")
  @Timed(name = "set-domain-parent." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "set-domain-parent", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response setParent(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain,
      DomainParentSetRequest request) {
    return execute(
        OperationType.ALTER,
        metalake,
        domain,
        () -> {
          validateRequest(request);
          DomainAuthzHelper.requireManage(domainDispatcher, metalake, domain);
          DomainAuthzHelper.requireManage(domainDispatcher, metalake, request.getParent());
          return domainResponse(
              DomainAuthzHelper.checked(
                      domainDispatcher,
                      state -> {
                        DomainAuthzHelper.requireManage(metalake, state.domain());
                        state
                            .parent()
                            .ifPresent(parent -> DomainAuthzHelper.requireManage(metalake, parent));
                      })
                  .attachDomain(metalake, domain, request.getParent()));
        });
  }

  /**
   * Detaches a domain; a top-level domain is unchanged.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @return The REST response.
   */
  @DELETE
  @Path("{domain}/parent")
  @Timed(name = "remove-domain-parent." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "remove-domain-parent", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response removeParent(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain) {
    return execute(
        OperationType.ALTER,
        metalake,
        domain,
        () -> {
          return domainResponse(
              DomainAuthzHelper.managed(domainDispatcher, metalake).detachDomain(metalake, domain));
        });
  }

  /**
   * Replaces the backing tag or removes it when backingTag is null.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @param request The requested domain change.
   * @return The REST response.
   */
  @PUT
  @Path("{domain}/backingTag")
  @Consumes("application/json")
  @Timed(name = "set-domain-backing-tag." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "set-domain-backing-tag", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response setBackingTag(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain,
      DomainBackingTagSetRequest request) {
    return execute(
        OperationType.ALTER,
        metalake,
        domain,
        () -> {
          validateRequest(request);
          DomainAuthzHelper.requireManage(domainDispatcher, metalake, domain);
          return domainResponse(
              DomainAuthzHelper.checked(
                      domainDispatcher,
                      state -> {
                        DomainAuthzHelper.requireManage(metalake, state.domain());
                        if (request.getBackingTag() != null) {
                          DomainAuthzHelper.requireBackingTag(
                              metalake, request.getBackingTag().getName(), false);
                        }
                      })
                  .updateBackingTag(
                      metalake,
                      domain,
                      request.getBackingTag() == null ? null : request.getBackingTag().tagValue()));
        });
  }

  /**
   * Lists the subdomain names or snapshots.
   *
   * @param metalake The metalake name.
   * @param domain The domain name.
   * @param details Whether to include domain snapshots.
   * @return The REST response.
   */
  @GET
  @Path("{domain}/subdomains")
  @Timed(name = "list-subdomains." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-subdomains", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listSubdomains(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("domain") String domain,
      @QueryParam("details") @DefaultValue("false") boolean details) {
    return execute(
        OperationType.LIST,
        metalake,
        domain,
        () -> {
          DomainAuthzHelper.requireVisible(domainDispatcher, metalake, domain);
          return details
              ? listResponse(
                  DomainAuthzHelper.filterDomains(
                      metalake, domainDispatcher.listSubdomainInfos(metalake, domain)))
              : Utils.ok(
                  new NameListResponse(
                      DomainAuthzHelper.enabled()
                          ? Arrays.stream(
                                  DomainAuthzHelper.filterDomains(
                                      metalake,
                                      domainDispatcher.listSubdomainInfos(metalake, domain)))
                              .map(DomainEntity::name)
                              .toArray(String[]::new)
                          : domainDispatcher.listSubdomains(metalake, domain)));
        });
  }

  // Shared with the reverse membership query so every endpoint has the same snapshot shape.
  static DomainDTO toDTO(DomainEntity domain) {
    return new DomainDTO(
        domain.name(),
        domain.comment(),
        domain.properties(),
        domain.status(),
        domain.parent().orElse(null),
        domain.backingTag().orElse(null),
        DTOConverters.toDTO(domain.auditInfo()),
        domain.relations());
  }

  private Response execute(
      OperationType operation, String metalake, String domain, Supplier<Response> action) {
    LOG.info("Received domain {} request for {} under metalake {}", operation, domain, metalake);
    try {
      return Utils.doAs(httpRequest, action::get);
    } catch (Exception e) {
      return ExceptionHandlers.handleDomainException(operation, domain, metalake, e);
    }
  }

  private static void validateRequest(RESTRequest request) {
    if (request == null) {
      throw new IllegalArgumentException("Request body cannot be null");
    }
    request.validate();
  }

  private static Response domainResponse(DomainEntity domain) {
    return Utils.ok(new DomainResponse(toDTO(domain)));
  }

  private static Response listResponse(DomainEntity[] domains) {
    return Utils.ok(
        new DomainListResponse(
            Arrays.stream(domains).map(DomainOperations::toDTO).toArray(DomainDTO[]::new)));
  }
}
