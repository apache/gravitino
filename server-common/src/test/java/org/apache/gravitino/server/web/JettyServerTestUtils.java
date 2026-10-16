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
package org.apache.gravitino.server.web;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.Filter;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;

/**
 * Shared reflection/introspection helpers for tests that need to inspect a live {@link
 * JettyServer}'s real servlet and filter mappings, rather than parsing source text or mocking
 * Jetty. Published via server-common's {@code testArtifacts} configuration so tests in other
 * modules built on {@link JettyServer} (the main server, Iceberg REST, Lance REST) don't each
 * re-implement the same reflection and stream logic.
 */
public final class JettyServerTestUtils {

  private JettyServerTestUtils() {}

  /**
   * Reflects into {@code server}'s private {@code servletContextHandler} field, which is otherwise
   * only populated after {@link JettyServer#initialize} runs and has no public getter.
   *
   * @param server the Jetty server to inspect
   * @return the server's live servlet context handler
   * @throws ReflectiveOperationException if the field cannot be accessed
   */
  public static ServletContextHandler getServletContextHandler(JettyServer server)
      throws ReflectiveOperationException {
    Field handlerField = JettyServer.class.getDeclaredField("servletContextHandler");
    handlerField.setAccessible(true);
    return (ServletContextHandler) handlerField.get(server);
  }

  /**
   * Returns every pathSpec that {@code filterClass} is bound to on {@code servletHandler}.
   *
   * @param servletHandler the live servlet handler to inspect
   * @param filterClass the filter class to look for
   * @return the set of pathSpecs the filter is registered on
   */
  public static Set<String> filterPathSpecsFor(
      ServletHandler servletHandler, Class<? extends Filter> filterClass) {
    return Arrays.stream(servletHandler.getFilterMappings())
        .filter(
            filterMapping ->
                filterClass
                    .getName()
                    .equals(servletHandler.getFilter(filterMapping.getFilterName()).getClassName()))
        .flatMap(filterMapping -> Arrays.stream(filterMapping.getPathSpecs()))
        .collect(Collectors.toSet());
  }
}
