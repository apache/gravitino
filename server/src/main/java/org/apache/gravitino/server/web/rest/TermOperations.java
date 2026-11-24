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
      @BeanParam ListTermsQuery query) {
    return execute(
        OperationType.LIST,
        metalake,
        null,
        () -> {
          query.validate();
          GlossaryTerm.Status filter = parseStatus(query.status);
          return Boolean.parseBoolean(query.details)
              ? Utils.ok(
                  new TermListResponse(
                      views.listTerms(metalake, query.domain, query.nameContains, filter)))
              : Utils.ok(
                  new NameListResponse(
                      views.listTermNames(metalake, query.domain, query.nameContains, filter)));
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
   * @param direction Optional traversal direction. NARROWER (default) returns descendants; BROADER
   *     returns ancestors.
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
    private String details;

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

    private void validate() {
      Preconditions.checkArgument(
          "true".equalsIgnoreCase(details) || "false".equalsIgnoreCase(details),
          "Invalid details value '%s', expected true or false",
          details);
      Preconditions.checkArgument(
          match == null && bindings == null, "Term lookup and binding expansion are not supported");
    }
  }

  @Nullable
  private static GlossaryTerm.Status parseStatus(@Nullable String status) {
    return status == null ? null : GlossaryTerm.Status.fromString(status);
  }

  private static GlossaryManager.Direction parseDirection(String direction) {
    Preconditions.checkArgument(
        GlossaryManager.Direction.BROADER.name().equals(direction)
            || GlossaryManager.Direction.NARROWER.name().equals(direction),
        "Invalid hierarchy direction '%s', expected BROADER or NARROWER",
        direction);
    return GlossaryManager.Direction.valueOf(direction);
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
      return Utils.illegalArguments(e.getMessage(), e);
    } catch (NotFoundException e) {
      return Utils.notFound(e.getMessage(), e);
    } catch (ForbiddenException e) {
      return Utils.forbidden(e.getMessage(), e);
    } catch (AlreadyExistsException e) {
      return Utils.alreadyExists(e.getMessage(), e);
    } catch (NotInUseException e) {
      return Utils.notInUse(e.getMessage(), e);
    } catch (Exception e) {
      return COMMON_ERRORS.handle(operation, term, metalake, e);
    }
  }
}
