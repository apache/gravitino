/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import io.trino.spi.TrinoException;
import java.time.Instant;
import java.util.Map;
import org.apache.logging.log4j.util.Strings;

/** Help Gravitino connector access CatalogMetadata from gravitino client. */
public class GravitinoCatalog {

  private final String metalake;
  private final Catalog catalog;

  public GravitinoCatalog(String metalake, Catalog catalog) {
    this.metalake = metalake;
    this.catalog = catalog;
  }

  public String getProvider() {
    return catalog.provider();
  }

  public String getName() {
    return catalog.name();
  }

  public String getMetalake() {
    return metalake;
  }

  public NameIdentifier geNameIdentifier() {
    return NameIdentifier.ofCatalog(metalake, catalog.name());
  }

  public String getProperty(String name, String defaultValue) {
    return catalog.properties().getOrDefault(name, defaultValue);
  }

  public String getRequiredProperty(String name) throws Exception {
    String value = catalog.properties().getOrDefault(name, "");
    if (Strings.isBlank(value)) {
      throw new TrinoException(GRAVITINO_MISSING_CONFIG, "Missing required config: " + name);
    }
    return value;
  }

  public Map<String, String> getProperties() {
    return catalog.properties();
  }

  public Instant getLastModifiedTime() {
    Instant time = catalog.auditInfo().lastModifiedTime();
    return time == null ? catalog.auditInfo().createTime() : time;
  }
}
