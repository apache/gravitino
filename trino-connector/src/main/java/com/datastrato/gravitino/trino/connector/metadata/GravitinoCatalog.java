/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.trino.connector.metadata;

import static com.datastrato.gravitino.trino.connector.GravitinoErrorCode.GRAVITINO_MISSING_CONFIG;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.NameIdentifier;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.TrinoException;
import java.time.Instant;
import java.util.Map;
import org.apache.logging.log4j.util.Strings;

/** Help Gravitino connector access CatalogMetadata from gravitino client. */
public class GravitinoCatalog {

  private static ObjectMapper objectMapper = new ObjectMapper();

  private final String metalake;
  private final String provider;
  private final String name;
  private final Map<String, String> properties;
  private final long lastModifiedTime;

  public GravitinoCatalog(String metalake, Catalog catalog) {
    this.metalake = metalake;
    this.provider = catalog.provider();
    this.name = catalog.name();
    this.properties = catalog.properties();
    Instant time =
        catalog.auditInfo().lastModifiedTime() == null
            ? catalog.auditInfo().createTime()
            : catalog.auditInfo().lastModifiedTime();
    lastModifiedTime = time.toEpochMilli();
  }

  @JsonCreator
  public GravitinoCatalog(
      @JsonProperty("metalake") String metalake,
      @JsonProperty("provider") String provider,
      @JsonProperty("name") String name,
      @JsonProperty("properties") Map<String, String> properties,
      @JsonProperty("lastModifiedTime") long lastModifiedTime) {
    this.metalake = metalake;
    this.provider = provider;
    this.name = name;
    this.properties = properties;
    this.lastModifiedTime = lastModifiedTime;
  }

  @JsonProperty
  public String getProvider() {
    return provider;
  }

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public String getMetalake() {
    return metalake;
  }

  public NameIdentifier geNameIdentifier() {
    return NameIdentifier.ofCatalog(metalake, name);
  }

  public String getProperty(String name, String defaultValue) {
    return properties.getOrDefault(name, defaultValue);
  }

  public String getRequiredProperty(String name) throws Exception {
    String value = properties.getOrDefault(name, "");
    if (Strings.isBlank(value)) {
      throw new TrinoException(GRAVITINO_MISSING_CONFIG, "Missing required config: " + name);
    }
    return value;
  }

  @JsonProperty
  public Map<String, String> getProperties() {
    return properties;
  }

  @JsonProperty
  public long getLastModifiedTime() {
    return lastModifiedTime;
  }

  public static String toJson(GravitinoCatalog catalog) throws JsonProcessingException {
    return objectMapper.writeValueAsString(catalog);
  }

  public static GravitinoCatalog fromJson(String jsonString) throws JsonProcessingException {
    return objectMapper.readValue(jsonString, GravitinoCatalog.class);
  }
}
