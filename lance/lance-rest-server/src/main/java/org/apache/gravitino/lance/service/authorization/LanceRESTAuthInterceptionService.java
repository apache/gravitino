/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.lance.service.authorization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Named;
import org.aopalliance.intercept.ConstructorInterceptor;
import org.aopalliance.intercept.MethodInterceptor;
import org.apache.gravitino.lance.service.rest.LanceNamespaceOperations;
import org.apache.gravitino.lance.service.rest.LanceTableOperations;
import org.glassfish.hk2.api.Descriptor;
import org.glassfish.hk2.api.Filter;
import org.glassfish.hk2.api.InterceptionService;

/** Installs metadata authorization proxies for Lance REST resources. */
public class LanceRESTAuthInterceptionService implements InterceptionService {

  /** HK2 binding name for the metalake passed to the authorization interceptor. */
  public static final String METALAKE_BINDING = "lanceAuthorizationMetalake";

  // Membership here only routes a class through the interceptor; each method still opts in with
  // @AuthorizationExpression, and a method without one runs unauthorized. The table writes
  // (create, register, drop, alter) are still to be annotated.
  private static final Set<String> INTERCEPTED_CLASSES =
      ImmutableSet.of(
          LanceNamespaceOperations.class.getName(), LanceTableOperations.class.getName());

  private final MethodInterceptor authorizationInterceptor;

  /**
   * Creates the interception service for one metalake.
   *
   * @param metalakeName metalake exposed by Lance REST
   */
  @Inject
  public LanceRESTAuthInterceptionService(@Named(METALAKE_BINDING) String metalakeName) {
    this.authorizationInterceptor = new LanceMetadataAuthorizationMethodInterceptor(metalakeName);
  }

  @Override
  public Filter getDescriptorFilter() {
    return (Descriptor descriptor) -> INTERCEPTED_CLASSES.contains(descriptor.getImplementation());
  }

  @Override
  public List<MethodInterceptor> getMethodInterceptors(Method method) {
    return ImmutableList.of(authorizationInterceptor);
  }

  @Override
  public List<ConstructorInterceptor> getConstructorInterceptors(Constructor<?> constructor) {
    return Collections.emptyList();
  }
}
