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
package org.apache.gravitino.lance.service.authorization;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.Path;
import org.apache.gravitino.lance.service.rest.LanceHealthOperations;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.glassfish.hk2.api.Descriptor;
import org.junit.jupiter.api.Test;

/**
 * Guards the completeness of Lance REST authorization.
 *
 * <p>The shared interception pipeline authorizes a method only when it carries {@link
 * AuthorizationExpression}; a method without one is invoked unchecked. Adding an endpoint therefore
 * opens a hole that no behavioral test would notice, because the new endpoint simply has no
 * authorization test of its own. These tests scan the REST resource package instead of listing the
 * endpoints, so a new endpoint or a new resource class fails here until it is authorized.
 */
class TestLanceRESTEndpointAuthorizationCoverage {

  private static final String RESOURCE_PACKAGE = "org.apache.gravitino.lance.service.rest";

  /**
   * Health checks answer before any metalake is known and are served to unauthenticated liveness
   * probes, so they deliberately carry no authorization annotation.
   */
  private static final Set<Class<?>> UNAUTHORIZED_RESOURCES = Set.of(LanceHealthOperations.class);

  @Test
  void testEveryRESTEndpointDeclaresAnAuthorizationExpression() throws Exception {
    List<Class<?>> resources = authorizedResourceClasses();

    List<String> unauthorized =
        resources.stream()
            .flatMap(resource -> Arrays.stream(resource.getDeclaredMethods()))
            .filter(TestLanceRESTEndpointAuthorizationCoverage::isRestEndpoint)
            .filter(method -> !method.isAnnotationPresent(AuthorizationExpression.class))
            .map(method -> method.getDeclaringClass().getSimpleName() + "#" + method.getName())
            .sorted()
            .collect(Collectors.toList());

    assertTrue(
        unauthorized.isEmpty(),
        "Lance REST endpoints without @AuthorizationExpression are served unchecked: "
            + unauthorized);
  }

  @Test
  void testEveryAuthorizedResourceIsIntercepted() throws Exception {
    LanceRESTAuthInterceptionService interceptionService =
        new LanceRESTAuthInterceptionService("test_metalake");

    for (Class<?> resource : authorizedResourceClasses()) {
      assertTrue(
          interceptionService.getDescriptorFilter().matches(descriptorOf(resource)),
          resource.getName()
              + " declares authorization expressions but is not intercepted, so they are never "
              + "evaluated");
    }

    // The exception is explicit rather than incidental: a resource is unauthorized only because it
    // is listed here, and listing it also keeps it out of the interception service.
    for (Class<?> resource : UNAUTHORIZED_RESOURCES) {
      assertFalse(
          interceptionService.getDescriptorFilter().matches(descriptorOf(resource)),
          resource.getName() + " is intercepted but declares no authorization expression");
    }
  }

  private static boolean isRestEndpoint(Method method) {
    // A JAX-RS HTTP method is any annotation that is itself meta-annotated with @HttpMethod, which
    // covers @GET, @POST, @PUT, @DELETE, @HEAD and @PATCH without listing them.
    return Arrays.stream(method.getAnnotations())
        .anyMatch(annotation -> annotation.annotationType().isAnnotationPresent(HttpMethod.class));
  }

  private static Descriptor descriptorOf(Class<?> resource) {
    Descriptor descriptor = mock(Descriptor.class);
    when(descriptor.getImplementation()).thenReturn(resource.getName());
    return descriptor;
  }

  /** Returns the JAX-RS resource classes that must be authorized. */
  private static List<Class<?>> authorizedResourceClasses() throws Exception {
    List<Class<?>> resources =
        scanPackage().stream()
            .filter(clazz -> clazz.isAnnotationPresent(Path.class))
            .filter(clazz -> !UNAUTHORIZED_RESOURCES.contains(clazz))
            .collect(Collectors.toList());

    // Without this the scan silently passing would make both tests vacuous.
    assertFalse(resources.isEmpty(), "No JAX-RS resource found in " + RESOURCE_PACKAGE);
    return resources;
  }

  private static List<Class<?>> scanPackage() throws Exception {
    String resourcePath = RESOURCE_PACKAGE.replace('.', '/');
    List<Class<?>> classes = new ArrayList<>();
    Enumeration<URL> roots =
        Thread.currentThread().getContextClassLoader().getResources(resourcePath);
    while (roots.hasMoreElements()) {
      URL root = roots.nextElement();
      if (!"file".equals(root.getProtocol())) {
        continue;
      }
      File[] files =
          new File(root.toURI())
              .listFiles((directory, name) -> name.endsWith(".class") && !name.contains("$"));
      if (files == null) {
        continue;
      }
      for (File file : files) {
        String simpleName =
            file.getName().substring(0, file.getName().length() - ".class".length());
        classes.add(Class.forName(RESOURCE_PACKAGE + "." + simpleName));
      }
    }
    return classes;
  }
}
