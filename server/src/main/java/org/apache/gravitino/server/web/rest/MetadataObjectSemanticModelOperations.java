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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationFullName;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.AuthorizationObjectType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.MetadataObjectUtil;

/** Lists visible Semantic Model datasets directly associated with a source metadata object. */
@Path("/metalakes/{metalake}/objects/{type}/{fullName}/semantic-model-datasets")
public class MetadataObjectSemanticModelOperations {
  private final SemanticModelDispatcher dispatcher;
  @Context private HttpServletRequest httpRequest;

  /**
   * Creates source-scoped dataset discovery operations.
   *
   * @param dispatcher The Semantic Model dispatcher.
   */
  @Inject
  public MetadataObjectSemanticModelOperations(SemanticModelDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  /**
   * Lists matching datasets from models the caller can load, including their parent access checks.
   *
   * @param metalake The source metalake.
   * @param type The table or View source type.
   * @param fullName The source's catalog.schema.name.
   * @return An ordered list of fully qualified dataset identifiers.
   */
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(
      name = "list-semantic-model-datasets." + MetricNames.HTTP_PROCESS_DURATION,
      absolute = true)
  @ResponseMetered(name = "list-semantic-model-datasets", absolute = true)
  @AuthorizationExpression(expression = AuthorizationExpressionConstants.CAN_ACCESS_METADATA)
  public Response listSemanticModelDatasets(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("type") @AuthorizationObjectType String type,
      @PathParam("fullName") @AuthorizationFullName String fullName) {
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            MetadataObject.Type sourceType =
                MetadataObject.Type.valueOf(type.toUpperCase(Locale.ROOT));
            Preconditions.checkArgument(
                sourceType == MetadataObject.Type.TABLE || sourceType == MetadataObject.Type.VIEW,
                "Source must be a table or View");
            MetadataObject source = MetadataObjects.parse(fullName, sourceType);
            NameIdentifier[] datasets =
                dispatcher.listSemanticModelDatasets(
                    MetadataObjectUtil.toEntityIdent(metalake, source),
                    MetadataObjectUtil.toEntityType(source));
            NameIdentifier[] models =
                Arrays.stream(datasets)
                    .map(dataset -> NameIdentifier.of(dataset.namespace().levels()))
                    .distinct()
                    .toArray(NameIdentifier[]::new);
            Set<NameIdentifier> visible =
                new HashSet<>(
                    Arrays.asList(
                        MetadataAuthzHelper.filterByExpression(
                            metalake,
                            AuthorizationExpressionConstants
                                .LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
                            Entity.EntityType.SEMANTIC_MODEL,
                            models)));
            return Utils.ok(
                new EntityListResponse(
                    Arrays.stream(datasets)
                        .filter(
                            dataset ->
                                visible.contains(NameIdentifier.of(dataset.namespace().levels())))
                        .toArray(NameIdentifier[]::new)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(
          OperationType.LIST_SEMANTIC_MODEL_DATASETS_BY_SOURCE, "", fullName, e);
    }
  }
}
