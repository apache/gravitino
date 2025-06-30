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
import org.aopalliance.intercept.ConstructorInterceptor;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.server.web.rest.CatalogOperations;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.glassfish.hk2.api.Descriptor;
import org.glassfish.hk2.api.Filter;
import org.glassfish.hk2.api.InterceptionService;

/**
 * GravitinoInterceptionService defines a method interceptor for REST interfaces to create dynamic
 * proxies. It implements metadata authorization when invoking REST API methods. It needs to be
 * registered in the hk2 bean container when the gravitino server starts.
 */
public class GravitinoInterceptionService implements InterceptionService {

  @Override
  public Filter getDescriptorFilter() {
    return new ClassListFilter(ImmutableSet.of(CatalogOperations.class.getName()));
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
          AuthorizationExpressionEvaluator authorizationExpressionEvaluator =
              new AuthorizationExpressionEvaluator(expression);
          boolean authorizeResult = authorizationExpressionEvaluator.evaluate(metadataContext);
          if (!authorizeResult) {
            MetadataObject.Type type = expressionAnnotation.accessMetadataType();
            NameIdentifier accessMetadataName =
                metadataContext.get(Entity.EntityType.valueOf(type.name()));
            return Utils.forbidden(
                String.format("Can not access metadata %s", accessMetadataName.name()),
                new ForbiddenException("Can not access metadata %s", accessMetadataName));
          }
        }
        return methodInvocation.proceed();
      } catch (Exception ex) {
        return Utils.forbidden("Can not access metadata. Cause by: " + ex.getMessage(), ex);
      }
    }

    private Map<Entity.EntityType, NameIdentifier> extractNameIdentifierFromParameters(
        Parameter[] parameters, Object[] args) {
      Map<Entity.EntityType, String> metadatas = new HashMap<>();
      Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
      for (int i = 0; i < parameters.length; i++) {
        Parameter parameter = parameters[i];
        AuthorizationMetadata authorizeResource =
            parameter.getAnnotation(AuthorizationMetadata.class);
        if (authorizeResource == null) {
          continue;
        }
        MetadataObject.Type type = authorizeResource.type();
        metadatas.put(Entity.EntityType.valueOf(type.name()), String.valueOf(args[i]));
      }
      String metalake = metadatas.get(Entity.EntityType.METALAKE);
      String catalog = metadatas.get(Entity.EntityType.CATALOG);
      String schema = metadatas.get(Entity.EntityType.SCHEMA);
      String table = metadatas.get(Entity.EntityType.TABLE);
      String topic = metadatas.get(Entity.EntityType.TOPIC);
      metadatas.forEach(
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
                    Entity.EntityType.SCHEMA,
                    NameIdentifierUtil.ofTable(metalake, catalog, schema, table));
                break;
              case TOPIC:
                nameIdentifierMap.put(
                    Entity.EntityType.SCHEMA,
                    NameIdentifierUtil.ofTopic(metalake, catalog, schema, topic));
                break;
              case METALAKE:
                nameIdentifierMap.put(
                    Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
                break;
              default:
                break;
            }
          });
      return nameIdentifierMap;
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
