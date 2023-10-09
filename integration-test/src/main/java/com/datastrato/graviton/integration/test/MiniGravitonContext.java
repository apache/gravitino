/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.integration.test;

import java.util.Map;

public class MiniGravitonContext {
  Map<String, String> customConfig;

  public MiniGravitonContext(Map<String, String> customConfig) {
    this.customConfig = customConfig;
  }
}
