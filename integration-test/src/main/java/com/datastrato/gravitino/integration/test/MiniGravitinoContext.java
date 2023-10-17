/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.integration.test;

import java.util.Map;

public class MiniGravitinoContext {
  Map<String, String> customConfig;

  public MiniGravitinoContext(Map<String, String> customConfig) {
    this.customConfig = customConfig;
  }
}
