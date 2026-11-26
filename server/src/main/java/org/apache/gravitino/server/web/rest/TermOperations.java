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
import com.google.common.base.Preconditions;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.BeanParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.dto.requests.TermCreateRequest;
import org.apache.gravitino.dto.requests.TermUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.TermListResponse;
import org.apache.gravitino.dto.responses.TermResponse;
import org.apache.gravitino.dto.responses.TermTreeResponse;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.NotInUseException;
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.GlossaryTermViews;
import org.apache.gravitino.metalake.MetalakeManager;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Metalake-scoped glossary term definitions and hierarchy.
 *
 * <p>The empty authorization expressions retain the interceptor's metalake membership and active
 * role checks. The glossary manager checks term and domain access inside the read snapshot or write
 * transaction, including domains and relation targets supplied in a request body.
 */
@Path("/metalakes/{metalake}/terms")
@Produces("application/vnd.gravitino.v1+json")
public class TermOperations {
  private static final Logger LOG = LoggerFactory.getLogger(TermOperations.class);
  private static final ExceptionHandler COMMON_ERRORS =
      new ExceptionHandlers.BaseExceptionHandler();
  private final GlossaryTermViews views;
  @Context private HttpServletRequest httpRequest;

  /** Creates a term resource using the server's glossary manager. */
  @Inject
  public TermOperations(GlossaryTermViews views) {
    this.views = views;
  }

  /**
   * Lists visible terms. A bare {@code GET /metalakes/{metalake}/terms} returns names across all
   * domains and statuses. For approved definitions in a domain, use {@code
   * ?domain=Sales&status=APPROVED&details=true}.
   *
   * @param metalake Required metalake name from the path.
   * @param query Optional list filters and response format. See {@link ListTermsQuery}.
   * @return A name list by default, or term snapshots when details are requested.
   */
  @GET
  @Timed(name = "list-terms." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-terms", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listTerms(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @BeanParam @Nullable ListTermsQuery query) {
    return execute(
        OperationType.LIST,
        metalake,
        null,
        () -> {
          ListTermsQuery effective = query == null ? new ListTermsQuery() : query;
          effective.validate();
          GlossaryTerm.Status filter = parseStatus(effective.status);
          return Boolean.parseBoolean(effective.details)
              ? Utils.ok(
                  new TermListResponse(
                      views.listTerms(metalake, effective.domain, effective.nameContains, filter)))
              : Utils.ok(
                  new NameListResponse(
                      views.listTermNames(
                          metalake, effective.domain, effective.nameContains, filter)));
        });
  }

  /**
   * Reads a term by name, including its visible direct relations.
   *
   * @param metalake Required metalake name.
   * @param term Required term name. Encode an embedded slash as {@code %2F}.
   * @param status Optional case-insensitive DRAFT, APPROVED or DEPRECATED filter. Omit to read any
   *     visible status. A root outside the filter returns 404.
   * @return The matching term snapshot.
   */
  @GET
  @Path("{term}")
  @Timed(name = "get-term." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-term", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response getTerm(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("term") String term,
      @QueryParam("status") @Nullable String status) {
    return execute(
        OperationType.GET,
        metalake,
        term,
        () -> Utils.ok(new TermResponse(views.getTerm(metalake, term, parseStatus(status)))));
  }

  /**
   * Creates a term and its initial relationships atomically.
   *
   * @param metalake Required metalake name.
   * @param request Required JSON body. Only {@code name} is required, for example {@code
   *     {"name":"Revenue"}}. Status defaults to APPROVED. DRAFT and DEPRECATED are also accepted.
   *     Omitted synonyms, domains, relations, links and properties start empty.
   * @return The created term and its revision token.
   */
  @POST
  @Consumes("application/json")
  @Timed(name = "create-term." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-term", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response createTerm(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      TermCreateRequest request) {
    return execute(
        OperationType.CREATE,
        metalake,
        null,
        () -> {
          Preconditions.checkArgument(request != null, "Request body is required");
          return Utils.ok(new TermResponse(views.createTerm(metalake, request.toSpec())));
        });
  }

  /**
   * Applies the requested edits in order as one transaction.
   *
   * @param metalake Required metalake name.
   * @param term Required current term name.
   * @param request Required body with a nonempty {@code updates} array. To guard against concurrent
   *     changes, copy the last read's {@code revisionToken} into {@code expectedRevision}. Omit
   *     that field for an unconditional update. A stale token returns 409.
   * @return The resulting term snapshot.
   */
  @PUT
  @Path("{term}")
  @Consumes("application/json")
  @Timed(name = "alter-term." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-term", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response alterTerm(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("term") String term,
      TermUpdatesRequest request) {
    return execute(
        OperationType.ALTER,
        metalake,
        term,
        () -> {
          Preconditions.checkArgument(request != null, "Request body is required");
          return Utils.ok(
              new TermResponse(
                  views.alterTerm(
                      metalake, term, request.getExpectedRevision(), request.changes())));
        });
  }

  /**
   * Drops a term by name.
   *
   * @param metalake Required metalake name.
   * @param term Required term name.
   * @return A drop response with {@code dropped=false} for a missing or hidden term.
   */
  @DELETE
  @Path("{term}")
  @Timed(name = "delete-term." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-term", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response dropTerm(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("term") String term) {
    return execute(
        OperationType.DELETE,
        metalake,
        term,
        () -> Utils.ok(new DropResponse(views.dropTerm(metalake, term))));
  }

  /**
   * Walks the hierarchy from a named root.
   *
   * @param metalake Required metalake name.
   * @param term Required root term name.
   * @param direction Optional case-insensitive traversal direction. NARROWER (default) returns
   *     descendants; BROADER returns ancestors.
   * @param status Optional case-insensitive DRAFT, APPROVED or DEPRECATED filter. Omit to traverse
   *     all visible statuses. A filtered root returns 404. Traversal stops at each filtered node.
   * @return Each reachable visible term once, excluding the root.
   */
  @GET
  @Path("{term}/hierarchy")
  @Timed(name = "get-term-hierarchy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-term-hierarchy", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response getTermHierarchy(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("term") String term,
      @QueryParam("direction") @DefaultValue("NARROWER") String direction,
      @QueryParam("status") @Nullable String status) {
    return execute(
        OperationType.LIST,
        metalake,
        term,
        () -> {
          GlossaryManager.Direction walk = parseDirection(direction);
          return Utils.ok(
              new TermTreeResponse(
                  views.getTermHierarchy(metalake, term, walk, parseStatus(status))));
        });
  }

  /** Optional query parameters for browsing glossary terms. Combine filters to narrow the list. */
  public static class ListTermsQuery {
    /** Return full definitions with true, or names only with false (default). */
    @QueryParam("details")
    @DefaultValue("false")
    private String details = "false";

    /** Restrict results to this domain and its visible subdomains. Omit to include every domain. */
    @QueryParam("domain")
    @Nullable
    private String domain;

    /**
     * Filter by DRAFT, APPROVED or DEPRECATED, ignoring case. Omit to include every visible status.
     */
    @QueryParam("status")
    @Nullable
    private String status;

    /** Case-insensitive substring of the term name. Omit to include every name. */
    @QueryParam("nameContains")
    @Nullable
    private String nameContains;

    // Reserved lookup parameters. Reject them until term matching and binding expansion are
    // available.
    @QueryParam("match")
    @Nullable
    private String match;

    @QueryParam("bindings")
    @Nullable
    private String bindings;

    /** Creates a query with default values (names only, no filters). */
    public ListTermsQuery() {}

    /**
     * Returns whether full term definitions were requested.
     *
     * @return The raw details flag string, defaulting to {@code "false"}.
     */
    public String getDetails() {
      return details;
    }

    /**
     * Sets whether to return full term definitions.
     *
     * @param details {@code "true"} or {@code "false"}, ignoring case.
     * @return This query instance.
     */
    public ListTermsQuery withDetails(String details) {
      this.details = details;
      return this;
    }

    /**
     * Returns the optional domain filter.
     *
     * @return The domain name, or null if omitted.
     */
    @Nullable
    public String getDomain() {
      return domain;
    }

    /**
     * Restricts results to the named domain and its visible subdomains.
     *
     * @param domain The nonblank domain name, or null to include all domains.
     * @return This query instance.
     */
    public ListTermsQuery withDomain(@Nullable String domain) {
      this.domain = domain;
      return this;
    }

    /**
     * Returns the optional status filter.
     *
     * @return The raw status filter string, or null if omitted.
     */
    @Nullable
    public String getStatus() {
      return status;
    }

    /**
     * Restricts results to terms matching the given case-insensitive status.
     *
     * @param status DRAFT, APPROVED, or DEPRECATED (ignoring case), or null for all statuses.
     * @return This query instance.
     */
    public ListTermsQuery withStatus(@Nullable String status) {
      this.status = status;
      return this;
    }

    /**
     * Returns the optional case-insensitive name substring filter.
     *
     * @return The name fragment, or null if omitted.
     */
    @Nullable
    public String getNameContains() {
      return nameContains;
    }

    /**
     * Restricts results to terms whose name contains the given case-insensitive substring.
     *
     * @param nameContains The nonblank name fragment, or null to include all names.
     * @return This query instance.
     */
    public ListTermsQuery withNameContains(@Nullable String nameContains) {
      this.nameContains = nameContains;
      return this;
    }

    private void validate() {
      Preconditions.checkArgument(
          "true".equalsIgnoreCase(details) || "false".equalsIgnoreCase(details),
          "Invalid details value '%s', expected true or false",
          details);
      Preconditions.checkArgument(
          domain == null || StringUtils.isNotBlank(domain), "domain must not be blank");
      Preconditions.checkArgument(
          nameContains == null || StringUtils.isNotBlank(nameContains),
          "nameContains must not be blank");
      Preconditions.checkArgument(
          match == null && bindings == null, "Term lookup and binding expansion are not supported");
    }
  }

  @Nullable
  private static GlossaryTerm.Status parseStatus(@Nullable String status) {
    return status == null ? null : GlossaryTerm.Status.fromString(status);
  }

  private static GlossaryManager.Direction parseDirection(@Nullable String direction) {
    if (direction == null
        || GlossaryManager.Direction.NARROWER.name().equalsIgnoreCase(direction)) {
      return GlossaryManager.Direction.NARROWER;
    }
    if (GlossaryManager.Direction.BROADER.name().equalsIgnoreCase(direction)) {
      return GlossaryManager.Direction.BROADER;
    }
    throw new IllegalArgumentException(
        String.format("Invalid hierarchy direction '%s', expected BROADER or NARROWER", direction));
  }

  private Response execute(
      OperationType operation, String metalake, @Nullable String term, Supplier<Response> action) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            // Like other metalake-scoped resources, glossary terms are unavailable while the
            // metalake is disabled. This also maps a missing metalake to NoSuchMetalakeException.
            MetalakeManager.checkMetalakeInUse(metalake);
            return action.get();
          });
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "Invalid glossary term {} request for [{}] under metalake [{}]: {}",
          operation,
          term,
          metalake,
          e.getMessage(),
          e);
      return Utils.illegalArguments(e.getMessage(), e);
    } catch (NotFoundException e) {
      LOG.warn(
          "Glossary term {} target [{}] not found under metalake [{}]: {}",
          operation,
          term,
          metalake,
          e.getMessage(),
          e);
      return Utils.notFound(e.getMessage(), e);
    } catch (ForbiddenException e) {
      LOG.warn(
          "Forbidden glossary term {} request for [{}] under metalake [{}]: {}",
          operation,
          term,
          metalake,
          e.getMessage(),
          e);
      return Utils.forbidden(e.getMessage(), e);
    } catch (AlreadyExistsException e) {
      LOG.warn(
          "Conflict on glossary term {} request for [{}] under metalake [{}]: {}",
          operation,
          term,
          metalake,
          e.getMessage(),
          e);
      return Utils.alreadyExists(e.getMessage(), e);
    } catch (NotInUseException e) {
      LOG.warn(
          "Metalake [{}] is not in use for glossary term {} request [{}]: {}",
          metalake,
          operation,
          term,
          e.getMessage(),
          e);
      return Utils.notInUse(e.getMessage(), e);
    } catch (Exception e) {
      return COMMON_ERRORS.handle(operation, term, metalake, e);
    }
  }
}
