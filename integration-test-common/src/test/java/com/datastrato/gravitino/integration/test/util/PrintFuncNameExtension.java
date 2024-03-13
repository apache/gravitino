/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.util;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrintFuncNameExtension implements BeforeEachCallback, AfterEachCallback {
  public static final Logger LOG = LoggerFactory.getLogger(PrintFuncNameExtension.class);

  @Override
  public void beforeEach(ExtensionContext context) {
    LOG.info("===== Entry test: {} =====", context.getDisplayName());
  }

  @Override
  public void afterEach(ExtensionContext context) {
    LOG.info("====== Exit Test: {} ======", context.getDisplayName());
  }
}
