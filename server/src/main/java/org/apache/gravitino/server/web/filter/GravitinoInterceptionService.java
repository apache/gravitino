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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import org.aopalliance.intercept.ConstructorInterceptor;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.server.web.rest.CatalogOperations;
import org.apache.gravitino.server.web.rest.FilesetOperations;
import org.apache.gravitino.server.web.rest.GroupOperations;
import org.apache.gravitino.server.web.rest.MetalakeOperations;
import org.apache.gravitino.server.web.rest.ModelOperations;
import org.apache.gravitino.server.web.rest.OwnerOperations;
import org.apache.gravitino.server.web.rest.PermissionOperations;
import org.apache.gravitino.server.web.rest.RoleOperations;
import org.apache.gravitino.server.web.rest.SchemaOperations;
import org.apache.gravitino.server.web.rest.TableOperations;
import org.apache.gravitino.server.web.rest.TopicOperations;
import org.apache.gravitino.server.web.rest.UserOperations;
import org.apache.gravitino.utils.NameIdentifierUtil;
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
            OwnerOperations.class.getName()));
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
        if (expressionAnnotation != null) {
          String expression = expressionAnnotation.expression();
          Object[] args = methodInvocation.getArguments();
          Map<Entity.EntityType, NameIdentifier> metadataContext =
              extractNameIdentifierFromParameters(parameters, args);
          Map<String, Object> pathParams = extractPathParamsFromParameters(parameters, args);
          AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
              new AuthorizationExpressionEvaluator(expression);
          boolean authorizeResult =
              authorizationExpressionEvaluator.evaluate(
                  metadataContext, pathParams, new AuthorizationRequestContext());
          if (!authorizeResult) {
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

    private Map<Entity.EntityType, NameIdentifier> extractNameIdentifierFromParameters(
        Parameter[] parameters, Object[] args) {
      Map<Entity.EntityType, String> entities = new HashMap<>();
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
      for (int i = 0; i < parameters.length; i++) {
        Parameter parameter = parameters[i];
        AuthorizationMetadata authorizeResource =
            parameter.getAnnotation(AuthorizationMetadata.class);
        if (authorizeResource == null) {
          continue;
        }
        Entity.EntityType type = authorizeResource.type();
        entities.put(type, String.valueOf(args[i]));
      }
      String metalake = entities.get(Entity.EntityType.METALAKE);
      String catalog = entities.get(Entity.EntityType.CATALOG);
      String schema = entities.get(Entity.EntityType.SCHEMA);
      String table = entities.get(Entity.EntityType.TABLE);
      String topic = entities.get(Entity.EntityType.TOPIC);
      String fileset = entities.get(Entity.EntityType.FILESET);
      entities.forEach(
          (type, metadata) -> {
            switch (type) {
              case CATALOG:
                nameIdentifierMap.put(
                    Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalake, catalog));
                break;
              case SCHEMA:
                nameIdentifierMap.put(
                    Entity.EntityType.SCHEMA,
                    NameIdentifierUtil.ofSchema(metalake, catalog, schema));
                break;
              case TABLE:
                nameIdentifierMap.put(
                    Entity.EntityType.TABLE,
                    NameIdentifierUtil.ofTable(metalake, catalog, schema, table));
                break;
              case TOPIC:
                nameIdentifierMap.put(
                    Entity.EntityType.TOPIC,
                    NameIdentifierUtil.ofTopic(metalake, catalog, schema, topic));
                break;
              case FILESET:
                nameIdentifierMap.put(
                    Entity.EntityType.FILESET,
                    NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset));
                break;
              case MODEL:
                String model = entities.get(Entity.EntityType.MODEL);
                nameIdentifierMap.put(
                    Entity.EntityType.MODEL,
                    NameIdentifierUtil.ofModel(metalake, catalog, schema, model));
                break;
              case METALAKE:
                nameIdentifierMap.put(
                    Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
                break;
              case USER:
                nameIdentifierMap.put(
                    Entity.EntityType.USER,
                    NameIdentifierUtil.ofUser(metadata, entities.get(Entity.EntityType.USER)));
                break;
              case GROUP:
                nameIdentifierMap.put(
                    Entity.EntityType.GROUP,
                    NameIdentifierUtil.ofGroup(metalake, entities.get(Entity.EntityType.GROUP)));
                break;
              case ROLE:
                nameIdentifierMap.put(
                    Entity.EntityType.ROLE,
                    NameIdentifierUtil.ofRole(metalake, entities.get(Entity.EntityType.ROLE)));
                break;
              default:
                break;
            }
          });
      return nameIdentifierMap;
    }

    private Map<String, Object> extractPathParamsFromParameters(
        Parameter[] parameters, Object[] args) {
      Map<String, Object> pathParams = new HashMap<>();
      for (int i = 0; i < parameters.length; i++) {
        Parameter parameter = parameters[i];
        PathParam pathParam = parameter.getAnnotation(PathParam.class);
        if (pathParam == null) {
          continue;
        }
        pathParams.put("p_" + pathParam.value(), args[i]);
      }
      return pathParams;
    }
  }

  private static class ClassListFilter implements Filter {
    private final Set<String> targetClasses;

    public ClassListFilter(Set<String> targetClasses) {
      this.targetClasses = new HashSet<>(targetClasses);
    }

    @Override
    public boolean matches(Descriptor descriptor) {
      String implementation = descriptor.getImplementation();
      return targetClasses.contains(implementation);
    }
  }
}
