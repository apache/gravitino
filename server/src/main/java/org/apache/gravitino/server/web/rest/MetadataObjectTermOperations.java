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
import java.util.Locale;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.dto.requests.TermsAssociateRequest;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.ObjectTermsResponse;
import org.apache.gravitino.dto.responses.TermListResponse;
import org.apache.gravitino.glossary.GlossaryTerm;
import org.apache.gravitino.glossary.GlossaryTermViews;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;

/**
 * Reverse glossary binding reads and writes. Target authorization runs before child enumeration.
 */
@Path("/metalakes/{metalake}/objects/{type}/{fullName}")
@Produces("application/vnd.gravitino.v1+json")
public class MetadataObjectTermOperations {
  private final GlossaryTermViews views;
  @Context private HttpServletRequest httpRequest;

  /** Creates the resource backed by the server's glossary manager. */
  @Inject
  public MetadataObjectTermOperations(GlossaryTermViews views) {
    this.views = views;
  }

  /**
   * Lists direct terms and optionally the visible terms of table columns or semantic-model metrics.
   */
  @GET
  @Path("terms")
  @Timed(name = "list-object-terms." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-object-terms", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listObjectTerms(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @QueryParam("details") @DefaultValue("false") String details,
      @QueryParam("children") @DefaultValue("false") String children,
      @QueryParam("status") String status) {
    return list(metalake, type, fullName, null, details, children, status);
  }

  /** Lists the caller-visible glossary terms of an exact metric name. */
  @GET
  @Path("metrics/{metric}/terms")
  @Timed(name = "list-metric-terms." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-metric-terms", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listMetricTerms(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @PathParam("metric") String metric,
      @QueryParam("details") @DefaultValue("false") String details,
      @QueryParam("children") @DefaultValue("false") String children,
      @QueryParam("status") String status) {
    return list(metalake, type, fullName, metric, details, children, status);
  }

  /** Atomically adds and removes terms on an implemented-by metadata object. */
  @POST
  @Path("terms")
  @Consumes("application/json")
  @Timed(name = "associate-object-terms." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "associate-object-terms", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response associateObjectTerms(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      TermsAssociateRequest request) {
    return associate(metalake, type, fullName, null, request);
  }

  /** Atomically adds and removes terms on a semantic-model metric. */
  @POST
  @Path("metrics/{metric}/terms")
  @Consumes("application/json")
  @Timed(name = "associate-metric-terms." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "associate-metric-terms", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response associateMetricTerms(
      @PathParam("metalake") @AuthorizationMetadata(type = EntityType.METALAKE) String metalake,
      @PathParam("type") String type,
      @PathParam("fullName") String fullName,
      @PathParam("metric") String metric,
      TermsAssociateRequest request) {
    return associate(metalake, type, fullName, metric, request);
  }

  private Response list(
      String metalake,
      String type,
      String fullName,
      @Nullable String metric,
      String details,
      String children,
      @Nullable String status) {
    return TermOperations.execute(
        httpRequest,
        OperationType.LIST,
        metalake,
        fullName,
        () -> {
          MetadataObject object = target(type, fullName, metric);
          boolean expand = flag("children", children);
          boolean detailed = flag("details", details);
          Preconditions.checkArgument(
              !expand
                  || metric == null
                      && (object.type() == MetadataObject.Type.TABLE
                          || object.type() == MetadataObject.Type.SEMANTIC_MODEL),
              "children requires a table or semantic model");
          GlossaryTerm.Status filter = TermOperations.parseStatus(status);
          if (!detailed && !expand) {
            return Utils.ok(
                new NameListResponse(views.listTargetTermNames(metalake, object, metric, filter)));
          }
          ObjectTermsResponse terms =
              views.listTargetTerms(metalake, object, metric, expand, filter);
          return metric == null
              ? Utils.ok(terms)
              : Utils.ok(new TermListResponse(terms.getTerms()));
        });
  }

  private Response associate(
      String metalake,
      String type,
      String fullName,
      @Nullable String metric,
      TermsAssociateRequest request) {
    return TermOperations.execute(
        httpRequest,
        OperationType.ASSOCIATE,
        metalake,
        fullName,
        () -> {
          Preconditions.checkArgument(request != null, "A terms association request is required");
          request.validate();
          return Utils.ok(
              new NameListResponse(
                  views.associateTerms(
                      metalake,
                      target(type, fullName, metric),
                      metric,
                      request.getTermsToAdd(),
                      request.getTermsToRemove())));
        });
  }

  private static MetadataObject target(String type, String fullName, @Nullable String metric) {
    MetadataObject.Type objectType = MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT));
    Preconditions.checkArgument(
        metric == null || objectType == MetadataObject.Type.SEMANTIC_MODEL,
        "Only semantic models have metrics");
    return MetadataObjects.parse(fullName, objectType);
  }

  private static boolean flag(String name, String value) {
    Preconditions.checkArgument(
        "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value),
        "%s must be true or false",
        name);
    return Boolean.parseBoolean(value);
  }
}
