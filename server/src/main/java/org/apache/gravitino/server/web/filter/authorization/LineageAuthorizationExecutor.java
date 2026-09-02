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

package org.apache.gravitino.server.web.filter.authorization;

import static org.apache.gravitino.server.web.filter.ParameterUtil.extractFromParameters;

import com.google.common.base.Preconditions;
import io.openlineage.server.OpenLineage.Dataset;
import io.openlineage.server.OpenLineage.DatasetFacet;
import io.openlineage.server.OpenLineage.DatasetFacets;
import io.openlineage.server.OpenLineage.RunEvent;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.lineage.source.rest.LineageEventValidator;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Authorization executor for every input and output dataset in an OpenLineage event. */
public class LineageAuthorizationExecutor implements AuthorizationExecutor {

  private static final String DATASET_TYPE_FACET = "datasetType";

  private final Parameter[] parameters;
  private final Object[] args;
  private final String expression;
  private List<AuthorizationTarget> authorizationTargets = List.of();
  private boolean authorizationTargetsResolved;

  /**
   * Creates an authorization executor for an OpenLineage event.
   *
   * @param parameters parameters of the intercepted REST method
   * @param args arguments passed to the intercepted REST method
   * @param expression authorization expression to evaluate for every dataset
   */
  public LineageAuthorizationExecutor(Parameter[] parameters, Object[] args, String expression) {
    this.parameters = parameters;
    this.args = args;
    this.expression = expression;
  }

  @Override
  public Optional<String> getAuthorizationMetalake() {
    RunEvent event = extractRunEvent();
    resolveAuthorizationTargets(event);
    return Optional.of(event.getJob().getNamespace());
  }

  @Override
  public boolean execute(AuthorizationRequestContext context) {
    if (!authorizationTargetsResolved) {
      resolveAuthorizationTargets(extractRunEvent());
    }

    AuthorizationExpressionEvaluator evaluator = new AuthorizationExpressionEvaluator(expression);
    context.setOriginalAuthorizationExpression(expression);

    for (AuthorizationTarget target : authorizationTargets) {
      if (!evaluator.evaluate(
          target.metadataContext, Map.of(), context, Optional.of(target.entityType.name()))) {
        return false;
      }
    }
    return true;
  }

  static MetadataObject.Type getMetadataType(Dataset dataset) {
    DatasetFacets facets = dataset.getFacets();
    if (facets == null) {
      return MetadataObject.Type.TABLE;
    }

    DatasetFacet datasetTypeFacet = facets.getAdditionalProperties().get(DATASET_TYPE_FACET);
    if (datasetTypeFacet == null) {
      return MetadataObject.Type.TABLE;
    }

    Object datasetType = datasetTypeFacet.getAdditionalProperties().get(DATASET_TYPE_FACET);
    Preconditions.checkArgument(
        datasetType instanceof String && StringUtils.isNotBlank((String) datasetType),
        "The datasetType facet must contain a non-blank datasetType");

    return switch (((String) datasetType).toUpperCase(Locale.ROOT)) {
      case "TABLE" -> MetadataObject.Type.TABLE;
      case "VIEW" -> MetadataObject.Type.VIEW;
      case "FILE", "FILESET" -> MetadataObject.Type.FILESET;
      case "MODEL", "MODEL_VERSION" -> MetadataObject.Type.MODEL;
      case "TOPIC" -> MetadataObject.Type.TOPIC;
      default -> throw new IllegalArgumentException("Unsupported dataset type: " + datasetType);
    };
  }

  private RunEvent extractRunEvent() {
    Object request = extractFromParameters(parameters, args);
    Preconditions.checkArgument(request instanceof RunEvent, "Lineage request must be a RunEvent");
    return (RunEvent) request;
  }

  private void resolveAuthorizationTargets(RunEvent event) {
    if (authorizationTargetsResolved) {
      return;
    }

    LineageEventValidator.validate(event);
    String metalake = event.getJob().getNamespace();
    List<AuthorizationTarget> targets = new ArrayList<>();
    resolveAuthorizationTargets(event.getInputs(), "inputs", metalake, targets);
    resolveAuthorizationTargets(event.getOutputs(), "outputs", metalake, targets);
    authorizationTargets = List.copyOf(targets);
    authorizationTargetsResolved = true;
  }

  private static void resolveAuthorizationTargets(
      List<? extends Dataset> datasets,
      String fieldName,
      String metalake,
      List<AuthorizationTarget> targets) {
    if (datasets == null) {
      return;
    }

    for (int index = 0; index < datasets.size(); index++) {
      Dataset dataset = datasets.get(index);
      Preconditions.checkArgument(
          metalake.equals(dataset.getNamespace()),
          "%s[%s].namespace must match job.namespace '%s'",
          fieldName,
          index,
          metalake);

      MetadataObject.Type metadataType = getMetadataType(dataset);
      MetadataObject metadataObject = MetadataObjects.parse(dataset.getName(), metadataType);
      Entity.EntityType entityType = MetadataObjectUtil.toEntityType(metadataType);
      NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
      Map<Entity.EntityType, NameIdentifier> metadataContext =
          NameIdentifierUtil.splitNameIdentifier(metalake, entityType, identifier);
      targets.add(new AuthorizationTarget(metadataContext, entityType));
    }
  }

  private static class AuthorizationTarget {
    private final Map<Entity.EntityType, NameIdentifier> metadataContext;
    private final Entity.EntityType entityType;

    private AuthorizationTarget(
        Map<Entity.EntityType, NameIdentifier> metadataContext, Entity.EntityType entityType) {
      this.metadataContext = metadataContext;
      this.entityType = entityType;
    }
  }
}
