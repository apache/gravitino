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
package org.apache.gravitino.integration.test.util;

import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an extension for JUnit 5, which aims to perform certain operations (such as resource
 * recycling, etc.) after all test executions are completed (regardless of success or failure). You
 * can Refer to {@link BaseIT} for more information.
 */
public class CloseContainerExtension implements BeforeAllCallback {
  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    // Ensure that the container suite is initialized before closing it
    if (ContainerSuite.initialized()) {
      synchronized (CloseContainerExtension.class) {
        extensionContext
            .getRoot()
            .getStore(ExtensionContext.Namespace.GLOBAL)
            .getOrComputeIfAbsent(CloseableContainer.class);
      }
    }
  }

  private static class CloseableContainer implements ExtensionContext.Store.CloseableResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloseableContainer.class);
    private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();

    @Override
    public void close() {
      try {
        CONTAINER_SUITE.close();
        LOGGER.info("Containers were closed successfully");
      } catch (Exception e) {
        LOGGER.warn("Containers were not closed as expected", e);
      }
    }
  }
}
