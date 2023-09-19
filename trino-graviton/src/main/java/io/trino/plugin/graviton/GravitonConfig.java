/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package io.trino.plugin.graviton;

import java.net.URI;
import java.util.Map;

public class GravitonConfig {
  private URI gravitonUri;

  public GravitonConfig(Map<String, String> requiredConfig) throws Exception {
    gravitonUri = new URI(requiredConfig.getOrDefault("graviton.uri", "http://localhost:8090"));
  }

  public String getURI() {
    return gravitonUri.toString();
  }
}
