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
import java.util.Set;
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
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.MetricInfoListResponse;
import org.apache.gravitino.dto.semantic.SemanticModelMetricDTO;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.semantic.SemanticModelMetric;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.server.web.pagination.ListPager;
import org.apache.gravitino.server.web.pagination.PageToken;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST discovery of metrics contained in the Semantic Models of a schema. */
@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-model-metrics")
public class SemanticModelMetricOperations {
  private static final Logger LOG = LoggerFactory.getLogger(SemanticModelMetricOperations.class);

  private final SemanticModelDispatcher dispatcher;
  @Context private HttpServletRequest httpRequest;

  /**
   * Creates schema-level metric discovery operations.
   *
   * @param dispatcher The Semantic Model dispatcher.
   */
  @Inject
  public SemanticModelMetricOperations(SemanticModelDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  /**
   * Lists metric identifiers or complete definitions visible to the caller.
   *
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param details Whether to return complete metric definitions.
   * @return An identifier list or metric info list response.
   */
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-semantic-model-metrics." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-semantic-model-metrics", absolute = true)
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response listMetrics(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @QueryParam("details") @DefaultValue("false") boolean details,
      @QueryParam("pageSize") Integer pageSize,
      @QueryParam("pageToken") String pageToken) {
    LOG.info(
        "Received list Semantic Model metrics request for {}.{}.{}, details: {}",
        metalake,
        catalog,
        schema,
        details);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace namespace = NamespaceUtil.ofSemanticModel(metalake, catalog, schema);
            if (!details) {
              ListPager.ArrayPage<NameIdentifier> page =
                  ListPager.pageArray(
                      dispatcher.listMetrics(namespace),
                      NameIdentifier::toString,
                      all -> {
                        Set<NameIdentifier> visibleModels =
                            visibleModels(
                                metalake,
                                Arrays.stream(all)
                                    .map(ident -> NameIdentifier.of(ident.namespace().levels()))
                                    .toArray(NameIdentifier[]::new));
                        return Arrays.stream(all)
                            .filter(
                                ident ->
                                    visibleModels.contains(
                                        NameIdentifier.of(ident.namespace().levels())))
                            .toArray(NameIdentifier[]::new);
                      },
                      NameIdentifier[]::new,
                      pageSize,
                      pageToken,
                      PageToken.scopeOf("listMetrics", metalake, catalog, schema, "names"));
              return Utils.ok(
                  new EntityListResponse(page.items()).withNextPageToken(page.nextPageToken()));
            }

            ListPager.ArrayPage<SemanticModelMetric> page =
                ListPager.pageArray(
                    dispatcher.listMetricInfos(namespace),
                    info -> NameIdentifier.of(info.namespace(), info.metric().name()).toString(),
                    all -> {
                      Set<NameIdentifier> visibleModels =
                          visibleModels(
                              metalake,
                              Arrays.stream(all)
                                  .map(info -> NameIdentifier.of(info.namespace().levels()))
                                  .toArray(NameIdentifier[]::new));
                      return Arrays.stream(all)
                          .filter(
                              info ->
                                  visibleModels.contains(
                                      NameIdentifier.of(info.namespace().levels())))
                          .toArray(SemanticModelMetric[]::new);
                    },
                    SemanticModelMetric[]::new,
                    pageSize,
                    pageToken,
                    PageToken.scopeOf("listMetrics", metalake, catalog, schema, "details"));
            SemanticModelMetricDTO[] visible =
                Arrays.stream(page.items())
                    .map(SemanticModelMetricDTO::fromSemanticModelMetric)
                    .toArray(SemanticModelMetricDTO[]::new);
            return Utils.ok(
                new MetricInfoListResponse(visible).withNextPageToken(page.nextPageToken()));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(
          OperationType.LIST_SEMANTIC_MODEL_METRICS, "", schema, e);
    }
  }

  private Set<NameIdentifier> visibleModels(String metalake, NameIdentifier[] models) {
    NameIdentifier[] distinct = Arrays.stream(models).distinct().toArray(NameIdentifier[]::new);
    return new HashSet<>(
        Arrays.asList(
            MetadataAuthzHelper.filterByExpression(
                metalake,
                AuthorizationExpressionConstants.FILTER_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
                Entity.EntityType.SEMANTIC_MODEL,
                distinct)));
  }
}
