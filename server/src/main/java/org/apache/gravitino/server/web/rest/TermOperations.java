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
import org.apache.gravitino.glossary.GlossaryManager;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.GlossaryTermViews;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;

/** Metalake-scoped glossary term definitions and hierarchy. */
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

  /** Lists visible names or full snapshots matching the supplied filters. */
  @GET
  @Timed(name = "list-terms." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-terms", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response list(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @QueryParam("details") @DefaultValue("false") boolean details,
      @QueryParam("domain") String domain,
      @QueryParam("status") String status,
      @QueryParam("nameContains") String nameContains,
      @QueryParam("match") String match,
      @QueryParam("bindings") String bindings) {
    return execute(
        OperationType.LIST,
        metalake,
        null,
        () -> {
          Preconditions.checkArgument(
              match == null && bindings == null,
              "Term lookup and binding expansion are not supported");
          GlossaryTerm.Status filter = parseStatus(status);
          return details
              ? Utils.ok(new TermListResponse(views.list(metalake, domain, nameContains, filter)))
              : Utils.ok(new NameListResponse(views.names(metalake, domain, nameContains, filter)));
        });
  }

  /** Reads a visible glossary definition. */
  @GET
  @Path("{term}")
  @Timed(name = "get-term." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-term", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response get(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("term") String term,
      @QueryParam("status") String status) {
    return execute(
        OperationType.GET,
        metalake,
        term,
        () -> Utils.ok(new TermResponse(views.get(metalake, term, parseStatus(status)))));
  }

  /** Creates the definition and its initial relationships atomically. */
  @POST
  @Consumes("application/json")
  @Timed(name = "create-term." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-term", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response create(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      TermCreateRequest request) {
    return execute(
        OperationType.CREATE,
        metalake,
        null,
        () -> {
          Preconditions.checkArgument(request != null, "Request body is required");
          return Utils.ok(new TermResponse(views.create(metalake, request.toSpec())));
        });
  }

  /** Applies a batch of term edits, optionally checking the caller's revision token. */
  @PUT
  @Path("{term}")
  @Consumes("application/json")
  @Timed(name = "alter-term." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-term", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response alter(
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
                  views.alter(metalake, term, request.getExpectedRevision(), request.changes())));
        });
  }

  /** Drops a visible term and returns whether it existed. */
  @DELETE
  @Path("{term}")
  @Timed(name = "delete-term." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-term", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response drop(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("term") String term) {
    return execute(
        OperationType.DELETE,
        metalake,
        term,
        () -> Utils.ok(new DropResponse(views.drop(metalake, term))));
  }

  /** Walks the visible hierarchy transitively in the requested direction. */
  @GET
  @Path("{term}/hierarchy")
  @Timed(name = "get-term-hierarchy." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-term-hierarchy", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response hierarchy(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("term") String term,
      @QueryParam("direction") @DefaultValue("NARROWER") String direction,
      @QueryParam("status") String status) {
    return execute(
        OperationType.LIST,
        metalake,
        term,
        () -> {
          GlossaryManager.Direction walk = GlossaryManager.Direction.valueOf(direction);
          Preconditions.checkArgument(
              walk != GlossaryManager.Direction.RELATED, "direction must be BROADER or NARROWER");
          return Utils.ok(
              new TermTreeResponse(views.hierarchy(metalake, term, walk, parseStatus(status))));
        });
  }

  @Nullable
  private static GlossaryTerm.Status parseStatus(@Nullable String status) {
    return status == null ? null : GlossaryTerm.Status.valueOf(status);
  }

  private Response execute(
      OperationType operation, String metalake, @Nullable String term, Supplier<Response> action) {
    try {
      return Utils.doAs(httpRequest, action::get);
    } catch (IllegalArgumentException e) {
      return Utils.illegalArguments(e.getMessage(), e);
    } catch (NotFoundException e) {
      return Utils.notFound(e.getMessage(), e);
    } catch (ForbiddenException e) {
      return Utils.forbidden(e.getMessage(), e);
    } catch (AlreadyExistsException e) {
      return Utils.alreadyExists(e.getMessage(), e);
    } catch (Exception e) {
      return COMMON_ERRORS.handle(operation, term, metalake, e);
    }
  }
}
