/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloseContainerExtension implements BeforeAllCallback {
  @Override
  public void beforeAll(ExtensionContext extensionContext) {
    synchronized (CloseContainerExtension.class) {
      extensionContext
          .getRoot()
          .getStore(ExtensionContext.Namespace.GLOBAL)
          .getOrComputeIfAbsent(CloseableContainer.class);
    }
  }

  private static class CloseableContainer implements ExtensionContext.Store.CloseableResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(CloseableContainer.class);
    private static final ContainerSuite CONTAINER_SUITE = ContainerSuite.getInstance();

    @Override
    public void close() {
      try {
        CONTAINER_SUITE.close();
      } catch (Exception e) {
        LOGGER.warn("Containers are not closed as expected", e);
      }
    }
  }
}
