/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test;

import java.util.Map;

public class MiniGravitinoContext {
  Map<String, String> customConfig;
  final boolean ignoreIcebergRestService;

  public MiniGravitinoContext(Map<String, String> customConfig, boolean ignoreIcebergRestService) {
    this.customConfig = customConfig;
    this.ignoreIcebergRestService = ignoreIcebergRestService;
  }
}
