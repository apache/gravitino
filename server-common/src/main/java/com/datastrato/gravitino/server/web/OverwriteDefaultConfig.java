/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.server.web;

import java.util.Map;

/** Provide methods to overwrite the default config. */
public interface OverwriteDefaultConfig {

  /**
   * Returns key value pairs to overwrite the config which are not set explicitly.
   *
   * @return an Map of Config key and value pairs
   */
  Map<String, String> getOverwriteDefaultConfig();
}
