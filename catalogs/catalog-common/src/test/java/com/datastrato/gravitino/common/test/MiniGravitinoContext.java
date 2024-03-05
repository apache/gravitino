/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.common.test;

import java.util.Map;

public class MiniGravitinoContext {
  Map<String, String> customConfig;

  public MiniGravitinoContext(Map<String, String> customConfig) {
    this.customConfig = customConfig;
  }
}
