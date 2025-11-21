/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
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
import java.lang.reflect.Parameter;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.dto.requests.JobRunRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.utils.NameIdentifierUtil;

public class RunJobAuthorizationExecutor implements AuthorizationExecutor {
  private final Parameter[] parameters;
  private final Object[] args;
  private final Map<Entity.EntityType, NameIdentifier> metadataContext;
  private final AuthorizationExpressionEvaluator authorizationExpressionEvaluator;
  private final Map<String, Object> pathParams;
  private final String entityType;

  public RunJobAuthorizationExecutor(
      Parameter[] parameters,
      Object[] args,
      Map<Entity.EntityType, NameIdentifier> metadataContext,
      AuthorizationExpressionEvaluator authorizationExpressionEvaluator,
      Map<String, Object> pathParams,
      String entityType) {
    this.parameters = parameters;
    this.args = args;
    this.metadataContext = metadataContext;
    this.authorizationExpressionEvaluator = authorizationExpressionEvaluator;
    this.pathParams = pathParams;
    this.entityType = entityType;
  }

  @Override
  public boolean execute() throws Exception {
    Object request = extractFromParameters(parameters, args);
    if (request == null) {
      return false;
    }

    AuthorizationRequestContext context = new AuthorizationRequestContext();
    Preconditions.checkArgument(
        request instanceof JobRunRequest,
        "Expected JobRunRequest but found %s",
        request.getClass().getSimpleName());
    JobRunRequest jobRunRequest = (JobRunRequest) request;
    jobRunRequest.validate();
    String jobTemplateName = jobRunRequest.getJobTemplateName();
    NameIdentifier metalake = metadataContext.get(Entity.EntityType.METALAKE);
    NameIdentifier jobTemplateNameIdentifier =
        NameIdentifierUtil.ofJobTemplate(metalake.name(), jobTemplateName);
    metadataContext.put(Entity.EntityType.JOB_TEMPLATE, jobTemplateNameIdentifier);

    return authorizationExpressionEvaluator.evaluate(
        metadataContext, pathParams, context, Optional.ofNullable(entityType));
  }
}
