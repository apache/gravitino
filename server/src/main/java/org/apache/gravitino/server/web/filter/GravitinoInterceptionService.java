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

package org.apache.gravitino.server.web.filter;

import static org.apache.gravitino.server.web.filter.ParameterUtil.extractAuthorizationRequestTypeFromParameters;
import static org.apache.gravitino.server.web.filter.ParameterUtil.extractMetadataObjectTypeFromParameters;
import static org.apache.gravitino.server.web.filter.ParameterUtil.extractNameIdentifierFromParameters;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.ConstructorInterceptor;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationRequest;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.server.web.filter.authorization.AuthorizationExecutor;
import org.apache.gravitino.server.web.filter.authorization.AuthorizeExecutorFactory;
import org.apache.gravitino.server.web.rest.CatalogOperations;
import org.apache.gravitino.server.web.rest.FilesetOperations;
import org.apache.gravitino.server.web.rest.GroupOperations;
import org.apache.gravitino.server.web.rest.MetadataObjectTagOperations;
import org.apache.gravitino.server.web.rest.MetalakeOperations;
import org.apache.gravitino.server.web.rest.ModelOperations;
import org.apache.gravitino.server.web.rest.OwnerOperations;
import org.apache.gravitino.server.web.rest.PartitionOperations;
import org.apache.gravitino.server.web.rest.PermissionOperations;
import org.apache.gravitino.server.web.rest.RoleOperations;
import org.apache.gravitino.server.web.rest.SchemaOperations;
import org.apache.gravitino.server.web.rest.StatisticOperations;
import org.apache.gravitino.server.web.rest.TableOperations;
import org.apache.gravitino.server.web.rest.TagOperations;
import org.apache.gravitino.server.web.rest.TopicOperations;
import org.apache.gravitino.server.web.rest.UserOperations;
import org.apache.gravitino.utils.PrincipalUtils;
import org.glassfish.hk2.api.Descriptor;
import org.glassfish.hk2.api.Filter;
import org.glassfish.hk2.api.InterceptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GravitinoInterceptionService defines a method interceptor for REST interfaces to create dynamic
 * proxies. It implements metadata authorization when invoking REST API methods. It needs to be
 * registered in the hk2 bean container when the gravitino server starts.
 */
public class GravitinoInterceptionService implements InterceptionService {

  @Override
  public Filter getDescriptorFilter() {
    return new ClassListFilter(
        ImmutableSet.of(
            MetalakeOperations.class.getName(),
            CatalogOperations.class.getName(),
            SchemaOperations.class.getName(),
            TableOperations.class.getName(),
            ModelOperations.class.getName(),
            TopicOperations.class.getName(),
            FilesetOperations.class.getName(),
            UserOperations.class.getName(),
            GroupOperations.class.getName(),
            PermissionOperations.class.getName(),
            RoleOperations.class.getName(),
            OwnerOperations.class.getName(),
            StatisticOperations.class.getName(),
            PartitionOperations.class.getName(),
            MetadataObjectTagOperations.class.getName(),
            TagOperations.class.getName()));
  }

  @Override
  public List<MethodInterceptor> getMethodInterceptors(Method method) {
    return ImmutableList.of(new MetadataAuthorizationMethodInterceptor());
  }

  @Override
  public List<ConstructorInterceptor> getConstructorInterceptors(Constructor<?> constructor) {
    return Collections.emptyList();
  }

  /**
   * Through dynamic proxy, obtain the annotations on the method and parameter list to perform
   * metadata authorization.
   */
  private static class MetadataAuthorizationMethodInterceptor implements MethodInterceptor {
    private static final Logger LOG =
        LoggerFactory.getLogger(MetadataAuthorizationMethodInterceptor.class);

    /**
     * Determine whether authorization is required and the rules via the authorization annotation ,
     * and obtain the metadata ID that requires authorization via the authorization annotation.
     *
     * @param methodInvocation methodInvocation with the Method object
     * @return the return result of the original method.
     * @throws Throwable throw an exception when authorization fails.
     */
    @Override
    public Object invoke(MethodInvocation methodInvocation) throws Throwable {
      try {
        Method method = methodInvocation.getMethod();
        Parameter[] parameters = method.getParameters();
        AuthorizationExpression expressionAnnotation =
            method.getAnnotation(AuthorizationExpression.class);
        AuthorizationExecutor executor;
        if (expressionAnnotation != null) {
          String expression = expressionAnnotation.expression();
          Object[] args = methodInvocation.getArguments();
          String entityType = extractMetadataObjectTypeFromParameters(parameters, args);
          Map<Entity.EntityType, NameIdentifier> metadataContext =
              extractNameIdentifierFromParameters(parameters, args);
          Map<String, Object> pathParams = Utils.extractPathParamsFromParameters(parameters, args);
          AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
              new AuthorizationExpressionEvaluator(expression);
          AuthorizationRequest.RequestType requestType =
              extractAuthorizationRequestTypeFromParameters(parameters);
          executor =
              AuthorizeExecutorFactory.create(
                  requestType,
                  metadataContext,
                  authorizationExpressionEvaluator,
                  pathParams,
                  entityType,
                  parameters,
                  args);
          boolean authorizeResult = executor.execute();
          if (!authorizeResult) {
            return buildNoAuthResponse(expressionAnnotation, metadataContext, method, expression);
          }
        }
        return methodInvocation.proceed();
      } catch (Exception ex) {
        String currentUser = PrincipalUtils.getCurrentUserName();
        String methodName = methodInvocation.getMethod().getName();

        LOG.error(
            "System internal error during authorization - User: {}, Operation: {}",
            currentUser,
            methodName,
            ex);
        return Utils.internalError(
            "Authorization failed due to system internal error. Please contact administrator.", ex);
      }
    }

    private Response buildNoAuthResponse(
        AuthorizationExpression expressionAnnotation,
        Map<Entity.EntityType, NameIdentifier> metadataContext,
        Method method,
        String expression) {
      MetadataObject.Type type = expressionAnnotation.accessMetadataType();
      NameIdentifier accessMetadataName =
          metadataContext.get(Entity.EntityType.valueOf(type.name()));
      String errorMessage = expressionAnnotation.errorMessage();
      String currentUser = PrincipalUtils.getCurrentUserName();
      String methodName = method.getName();

      LOG.warn(
          "Authorization failed - User: {}, Operation: {}, Metadata: {}, Expression: {}",
          currentUser,
          methodName,
          accessMetadataName,
          expression);

      return buildNoAuthResponse(errorMessage, accessMetadataName, currentUser, methodName);
    }

    private Response buildNoAuthResponse(
        String errorMessage,
        NameIdentifier accessMetadataName,
        String currentUser,
        String methodName) {
      String contextualMessage;
      String accessMetadataMessage =
          accessMetadataName != null
              ? String.format("on metadata '%s'", accessMetadataName.name())
              : "";
      if (StringUtils.isNotBlank(errorMessage)) {
        contextualMessage =
            String.format(
                "User '%s' is not authorized to perform operation '%s' %s: %s",
                currentUser, methodName, accessMetadataMessage, errorMessage);
      } else {
        contextualMessage =
            String.format(
                "User '%s' is not authorized to perform operation '%s' %s",
                currentUser, methodName, accessMetadataMessage);
      }
      return Utils.forbidden(contextualMessage, null);
    }
  }

  private record ClassListFilter(Set<String> targetClasses) implements Filter {
    private ClassListFilter(Set<String> targetClasses) {
      this.targetClasses = new HashSet<>(targetClasses);
    }

    @Override
    public boolean matches(Descriptor descriptor) {
      String implementation = descriptor.getImplementation();
      return targetClasses.contains(implementation);
    }
  }
}
